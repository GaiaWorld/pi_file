//! # 异步存储模块 Store< K=Arc<[u8]>, V=Arc<[u8]> >
//!
//! * 此模块的异步函数，需要用FILE_RUNTIME环境；
//! * 此模块每个open得到table的大小，不要太大，因为内容全部进入内存；一般：5M以内
//! 
//！ 流程如下
//!
//! * open时，按日志从新到旧的顺序，全部 依次读到内存；
//!    + removed表，仅仅是这时候用到，用于记录那些条目是已经移除的；
//! * read时，永远从 内存map 读；
//! * write时，先 往Log中写入，成功后再插入到 内存map；
//! * remove时，往Log中写入一条仅有key的数据，成功后，再移除掉 内存map对应的项
//!
//! TODO K应该是可序列化可排序的约束， keys提供范围获取， entrys提供范围获取

use std::{collections::BTreeMap, fmt::Debug, path::{Path, PathBuf}};
use std::io::Result;
use std::sync::Arc;

use rt_file::{FILE_RUNTIME};
use hash::XHashMap;
use pi_store::log_store::log_file::{LogFile, LogMethod, PairLoader};
use r#async::lock::spin_lock::SpinLock;


/// 线程安全的异步存储
#[derive(Clone)]
pub struct AsyncStore(Arc<InnerStore>);

unsafe impl Send for AsyncStore {}
unsafe impl Sync for AsyncStore {}

impl AsyncStore {
    ///
    /// 打开 path目录 下的异步存储
    ///
    /// * buf_len: 写缓冲区的字节数，一般4K的倍数
    /// * file_len: 单个日志文件的字节数
    ///
    pub async fn open<P: AsRef<Path> + Debug>(path: P, buf_len: usize, file_len: usize) -> Result<Self> {
        match LogFile::open(FILE_RUNTIME.clone(), path, buf_len, file_len, None).await {
            Err(e) => Err(e),
            Ok(file) => {
                //打开指定路径的日志存储成功
                let mut store = StoreOpen{
                        removed: XHashMap::default(),
                        store: AsyncStore(Arc::new(InnerStore {
                        map: SpinLock::new(BTreeMap::new()),
                        file: file.clone(),
                    }))
                };

                // 异步加载所有条目到内存
                if let Err(e) = file.load(&mut store, None, true).await {
                    Err(e)
                } else {
                    //初始化内存数据成功
                    Ok(store.store)
                }
            }
        }
    }

    /// 获取 存储的数据数量
    pub fn len(&self) -> usize {
        (&*self.0.map.lock()).len()
    }

    /// 同步读指定key的值
    pub fn read(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        if let Some(value) = self.0.map.lock().get(key) {
            return Some(value.clone());
        }
        None
    }

    /// 同步获取关键字集合
    pub fn keys(&self) -> Vec<Arc<[u8]>> {
        self.0.map.lock().keys().cloned().collect::<Vec<_>>()
    }

    /// 同步获取值集合
    pub fn values(&self) -> Vec<Arc<[u8]>> {
        self.0.map.lock().values().cloned().collect::<Vec<_>>()
    }

    /// 异步写指定key的存储数据
    pub async fn write(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<Option<Vec<u8>>> {
        let id = self
            .0
            .file
            .append(LogMethod::PlainAppend, key.as_ref(), value.as_ref());
        if let Err(e) = self.0.file.delay_commit(id, false,10).await {
            Err(e)
        } else {
            if let Some(value) = self.0.map.lock().insert(key, value) {
                //更新指定key的存储数据，则返回更新前的存储数据
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    /// 异步移除指定key的存储数据
    pub async fn remove(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let id = self.0.file.append(LogMethod::Remove, key, &[]);
        if let Err(e) = self.0.file.delay_commit(id, false, 10).await {
            Err(e)
        } else {
            if let Some(value) = self.0.map.lock().remove(key) {
                Ok(Some(value.to_vec()))
            } else {
                Ok(None)
            }
        }
    }
}

// 内部存储对象
struct InnerStore {
    // 所有内容的内存数据
    map: SpinLock<BTreeMap<Arc<[u8]>, Arc<[u8]>>>,
    // 日志文件
    file: LogFile,
}
struct StoreOpen {
    // 记住已删除的键，LogFile内部只管二进制； 仅仅是open阶段 用到
    removed: XHashMap<Vec<u8>, ()>,
    store: AsyncStore,
}

/// 定义 加载策略，用在open时候
/// 注：在open时，会将所有条目，从最新到最旧的顺序，全部加载到内存
impl PairLoader for StoreOpen {
    // 给个键，决定是否要加载；
    //    如果没标志为删除，而且没有含键，则加载该条目（新的先读，旧的后读）
    fn is_require(&self, _log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool {
        !self.removed.contains_key(key)
            && !self
                .store.0
                .map
                .lock()
                .contains_key(key.as_slice())
    }
    // 如果is_require返回true，底层会加载；
    // 加载完成时，会回调此函数；
    //      注：如果value为None，则说明此条目是删除条目
    fn load(&mut self, _log_file: Option<&PathBuf>, _method: LogMethod, key: Vec<u8>, value: Option<Vec<u8>>) {
        if let Some(value) = value {
            self.store.0
                .map
                .lock()
                .insert(key.into(), value.into());
        } else {
            // value为null，代表 移除的条目
            self.removed.insert(key, ());
        }
    }
}
