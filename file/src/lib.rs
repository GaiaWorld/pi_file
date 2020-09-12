extern crate async_file;
extern crate hash;
extern crate fnv;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;

use std::{env, path::{Path, PathBuf}, sync::Arc, sync::Weak};
use std::ops::Deref;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::{ Result};
use std::collections::hash_map::{Entry};
use async_file::{AsyncFileOptions, WriteOptions, AsyncFile};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::lock::{rw_lock::RwLock, mutex_lock::Mutex, spin_lock::SpinLock};
use hash::{XHashMap, DefaultHasher};


lazy_static! {
    /// 异步 文件IO 运行时，多线程，不需要主动推
    pub static ref FILE_RUNTIME: MultiTaskRuntime<()> = {
        // 获得环境变量声明的异步文件线程数，如果没有声明，则取cpu物理核数
        let count = match env::var("_ver") {
            Ok(r) => usize::from_str_radix(r.as_str(), 10).unwrap(),
            _ => num_cpus::get()
        };
        // 线程池：每个线程1M的栈空间，10ms 休眠，10毫秒的定时器间隔
        let pool = MultiTaskPool::new("File-Runtime".to_string(), count, 1024 * 1024, 10, Some(10));
        pool.startup(true)
    };
    /// 打开文件的全局表
    static ref OPEN_FILE_MAP: Table = Table(Mutex::new(XHashMap::default()));
}

struct Table(Mutex<XHashMap<PathBuf, Weak<InnerSafeFile>>>);

/*
* 安全文件
*/

pub struct SafeFile(Arc<InnerSafeFile>);

impl Deref for SafeFile {
	type Target = AsyncFile<()>;
    #[inline(always)]
	fn deref(&self) -> &AsyncFile<()> {
		&(*self.0).file
	}
}
enum LockType {
    Rw(RwLock<()>),
    Lock(Mutex<()>),
}
struct InnerSafeFile {
    file: AsyncFile<()>,
    lock: LockType,
    buff: SpinLock<(Arc<[u8]>, usize)>,
}
impl Debug for InnerSafeFile {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self.file)
    }
}
impl InnerSafeFile {
    fn new(file: AsyncFile<()>, lock: LockType) -> Self {
        let vec = Vec::new();
        InnerSafeFile{
            file,
            lock,
            buff: SpinLock::new((Arc::from(&vec[..]), 0)),
        }
    }
}
// impl<O: Default + 'static> Clone for SafeFile<O> {
//     fn clone(&self) -> Self {
//         SaveFile(self.0.clone())
//     }
// }

/*
* 异步文件的异步方法
*/
impl SafeFile {
    //以指定方式异步打开指定的文件
    pub async fn open<P>(path: P,
                         options: AsyncFileOptions) -> Result<Self>
        where P: AsRef<Path> + Send + 'static {
        let path = path.as_ref().to_path_buf();
        {
            let tab = OPEN_FILE_MAP.0.lock().await;
            match tab.get(&path) {
                Some(r) => match r.upgrade() {
                    Some(rr) => {
                        return Ok(SafeFile(rr))
                    },
                    _ => ()
                },
                _ => ()
            }
        }
        let lock = match options {
            AsyncFileOptions::TruncateWrite => LockType::Lock(Mutex::new(())),
            _ => LockType::Rw(RwLock::new(()))
        };
        let file = match AsyncFile::open(
            FILE_RUNTIME.clone(), path.clone(), options).await {
            Ok(file) => Arc::new(InnerSafeFile::new(file, lock)),
            Err(r) => return Err(r)
        };
        let mut tab = OPEN_FILE_MAP.0.lock().await;
        match tab.entry(path) {
            Entry::Occupied(mut e) => {
                match e.get().upgrade() {
                    Some(rr) => {
                        return Ok(SafeFile(rr))
                    },
                    _ => {
                        e.insert(Arc::downgrade(&file));
                        Ok(SafeFile(file,))
                    }
                }
            }
            Entry::Vacant(e) => {
                e.insert(Arc::downgrade(&file));
                Ok(SafeFile(file))
            }
        }
    }
    //从指定位置开始异步读指定字节
    pub async fn read(&self, pos: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            //无效的字节数，则立即返回
            return Ok(Vec::with_capacity(0));
        }
        match self.0.lock { // 如果是截断写，则读取缓冲区的数据
            LockType::Lock(ref lock) => {
                let data = {
                    let lock = self.0.buff.lock();
                    lock.0.clone()
                };
                let read = lock.lock().await;
                if data.len() > 0 {
                    Ok(Vec::from([])) // TODO .slice(pos, pos + usize)
                }else{
                    match self.0.file.read(pos, len).await {
                        Ok(r) => {

                            Ok(r.clone())
                        },
                        Err(r) => Err(r)
                    }
                }
            },
            LockType::Rw(ref lock) => {
                let read = lock.read().await;
                self.0.file.read(pos, len).await
            }
        }
    }

    //从指定位置开始异步写指定字节
    pub async fn write(&self, pos: u64, buf: Arc<[u8]>, options: WriteOptions) -> Result<usize> {
        if buf.len() == 0 {
            //无效的字节数，则立即返回
            return Ok(0);
        }
        match self.0.lock { // 如果是截断写，则先设置缓冲区的数据和版本
            LockType::Lock(ref lock) => {        
                {
                    let mut lock = self.0.buff.lock();
                    lock.0 = buf;
                    lock.1 += 1;
                    lock.1
                };
                let write = lock.lock().await;
                let data_ver = { // 获得异步锁后先获取数据及版本
                    let lock = self.0.buff.lock();
                    (lock.0.clone(), lock.1)
                };
                if data_ver.1 == 0 { // 最新数据已经落地，则直接返回成功
                    Ok(data_ver.0.len())
                }else{
                    match self.0.file.write(pos, data_ver.0, options).await {
                        Ok(r) => {
                            // 写成功后再次获取锁
                            let mut lock = self.0.buff.lock();
                            // 比较版本号， 如果相同，则将版本号设为0，表示数据已经落地
                            if lock.1 == data_ver.1 {
                                lock.1 = 0;
                            }
                            Ok(r)
                        },
                        Err(r) => Err(r)
                    }
                }
            },
            LockType::Rw(ref lock) => {
                let write = lock.write().await;
                self.0.file.write(pos, buf, options).await
            }
        }
    }
}

/*
* 打开异步文件
*/
pub async fn open<P>(path: P, options: AsyncFileOptions) -> Result<AsyncFile<()>>
    where P: AsRef<Path> + Send + 'static {
    AsyncFile::open(FILE_RUNTIME.clone(), path, options).await
}
/*
* 异步创建目录
*/
pub async fn create_dir<P>(path: P) -> Result<()>
    where P: AsRef<Path> + Send + 'static {
    async_file::create_dir(FILE_RUNTIME.clone(), path).await
}

/*
* 异步移除文件
*/
pub async fn remove_file<P>(path: P) -> Result<()>
    where P: AsRef<Path> + Send + 'static{
    async_file::remove_file(FILE_RUNTIME.clone(), path).await
}

/*
* 异步移除目录
*/
pub async fn remove_dir<P>(path: P) -> Result<()>
    where P: AsRef<Path> + Send + 'static {
    async_file::remove_dir(FILE_RUNTIME.clone(), path).await
}
/*
* 异步重命名文件或目录
*/
pub async fn rename<P>(from: P, to: P) -> Result<()>
    where P: AsRef<Path> + Send + 'static {
    async_file::rename(FILE_RUNTIME.clone(), from, to).await
}
/*
* 异步复制文件
*/
pub async fn copy_file<P>(from: P, to: P) -> Result<u64>
    where P: AsRef<Path> + Send + 'static {
    async_file::copy_file(FILE_RUNTIME.clone(), from, to).await
}

/*
* 异步递归移除目录 TODO
*/
pub async fn remove_dir_all<P>(path: P) -> Result<()>
    where P: AsRef<Path> + Send + 'static {
    async_file::remove_dir(FILE_RUNTIME.clone(), path).await
}