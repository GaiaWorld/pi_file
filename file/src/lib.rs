extern crate async_file;
extern crate hash;
extern crate fnv;
extern crate num_cpus;
#[macro_use]
extern crate lazy_static;

use std::{env, path::{Path, PathBuf}, sync::Arc, sync::Weak};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::io::{ Result};
use std::collections::hash_map::{Entry};
use async_file::{AsyncFileOptions, WriteOptions, AsyncFile};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::lock::{rw_lock::RwLock, mutex_lock::Mutex};
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

struct InnerSafeFile {
    file: AsyncFile<()>,
    lock: RwLock<()>,
    buff: Arc<[u8]>,
}
impl Debug for InnerSafeFile {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self.file)
    }
}
impl InnerSafeFile {
    fn new(file: AsyncFile<()>) -> Self {
        let vec = Vec::new();
        InnerSafeFile{
            file,
            lock: RwLock::new(()),
            buff: Arc::from(&vec[..]),
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
        let file = match AsyncFile::open(
            FILE_RUNTIME.clone(), path.clone(), options).await {
            Ok(file) => Arc::new(InnerSafeFile::new(file)),
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
                        Ok(SafeFile(file))
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
        if self.0.file.
        let mut buf = Vec::with_capacity(len);
        unsafe { buf.set_len(len); }
        AsyncReadFile::new(self.0.runtime.clone(), buf, 0, self.clone(), pos, len, 0).await
    }

    //从指定位置开始异步写指定字节
    pub async fn write(&self, pos: u64, buf: Arc<[u8]>, options: WriteOptions) -> Result<usize> {
        if buf.len() == 0 {
            //无效的字节数，则立即返回
            return Ok(0);
        }

        AsyncWriteFile::new(self.0.runtime.clone(), buf, 0, self.clone(), pos, options, 0).await
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