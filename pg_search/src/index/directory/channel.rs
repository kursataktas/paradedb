use anyhow::Result;
use crossbeam::channel::{Receiver, Sender};
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{
    io,
    io::{Cursor, Write},
    ops::Range,
    result,
};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr};
use tantivy::Directory;

use crate::index::directory::blocking::{BlockingDirectory, BlockingLock};
use crate::index::reader::channel::ChannelReader;
use crate::index::reader::file_handle::FileHandleReader;
use crate::index::segment_handle::SegmentHandle;
use crate::index::writer::channel::ChannelWriter;
use crate::index::writer::io::IoWriter;

#[derive(Debug)]
pub enum ChannelRequest {
    AcquireLock(Lock),
    AtomicRead(PathBuf),
    AtomicWrite(PathBuf, Vec<u8>),
    ReleaseBlockingLock(BlockingLock),
    SegmentRead(PathBuf, Range<usize>, SegmentHandle),
    SegmentWrite(PathBuf, Cursor<Vec<u8>>),
    SegmentDelete(PathBuf),
    GetSegmentHandle(PathBuf),
    ShouldDeleteCtids(Vec<u64>),
    Terminate,
}

pub enum ChannelResponse {
    AtomicWriteAck,
    SegmentWriteAck,
    SegmentDeleteAck,
    AcquiredLock(BlockingLock),
    Bytes(Vec<u8>),
    SegmentHandle(Option<SegmentHandle>),
    ShouldDeleteCtids(Vec<u64>),
}

impl Debug for ChannelResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelResponse::AcquiredLock(_) => write!(f, "AcquiredLock"),
            ChannelResponse::AtomicWriteAck => write!(f, "AtomicWriteAck"),
            ChannelResponse::SegmentWriteAck => write!(f, "SegmentWriteAck"),
            ChannelResponse::SegmentDeleteAck => write!(f, "AcquiredSegmentDeleteAckLock"),
            ChannelResponse::Bytes(_) => write!(f, "Bytes"),
            ChannelResponse::SegmentHandle(_) => write!(f, "SegmentHandle"),
            ChannelResponse::ShouldDeleteCtids(_) => write!(f, "ShouldDeleteCtids"),
        }
    }
}

pub struct ChannelLock {
    // This is an Option because we need to take ownership of the lock in the Drop implementation
    lock: Option<BlockingLock>,
    sender: Sender<ChannelRequest>,
}

impl Drop for ChannelLock {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            self.sender.send(ChannelRequest::ReleaseBlockingLock(lock)).unwrap();
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChannelDirectory {
    request_sender: Sender<ChannelRequest>,
    request_receiver: Receiver<ChannelRequest>,
    response_sender: Sender<ChannelResponse>,
    response_receiver: Receiver<ChannelResponse>,
    relation_oid: u32,
}

// A directory that actually forwards all read/write requests to a channel
// This channel is used to communicate with the actual storage implementation
impl ChannelDirectory {
    pub fn new(
        request_channel: (Sender<ChannelRequest>, Receiver<ChannelRequest>),
        response_channel: (Sender<ChannelResponse>, Receiver<ChannelResponse>),
        relation_oid: u32,
    ) -> Self {
        let (request_sender, request_receiver) = request_channel;
        let (response_sender, response_receiver) = response_channel;

        Self {
            request_sender,
            response_receiver,
            response_sender,
            request_receiver,
            relation_oid,
        }
    }
}

impl Directory for ChannelDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(unsafe {
            ChannelReader::new(
                self.relation_oid,
                path,
                self.request_sender.clone(),
                self.response_receiver.clone(),
            )
            .map_err(|e| {
                OpenReadError::wrap_io_error(
                    io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
                    path.to_path_buf(),
                )
            })?
        }))
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        Ok(io::BufWriter::new(Box::new(unsafe {
            ChannelWriter::new(
                self.relation_oid,
                path,
                self.request_sender.clone(),
                self.response_receiver.clone(),
            )
        })))
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        self.request_sender
            .send(ChannelRequest::AtomicRead(path.to_path_buf()))
            .unwrap();

        match self.response_receiver.recv().unwrap() {
            ChannelResponse::Bytes(bytes) => Ok(bytes),
            unexpected => Err(OpenReadError::wrap_io_error(
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("atomic_read unexpected response {:?}", unexpected),
                ),
                path.to_path_buf(),
            )),
        }
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.request_sender
            .send(ChannelRequest::AtomicWrite(
                path.to_path_buf(),
                data.to_vec(),
            ))
            .unwrap();

        match self.response_receiver.recv().unwrap() {
            ChannelResponse::AtomicWriteAck => Ok(()),
            unexpected => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("atomic_write unexpected response {:?}", unexpected),
            )),
        }
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        self.request_sender
            .send(ChannelRequest::SegmentDelete(path.to_path_buf()))
            .unwrap();

        match self.response_receiver.recv().unwrap() {
            ChannelResponse::SegmentDeleteAck => Ok(()),
            unexpected => Err(DeleteError::IoError {
                io_error: io::Error::new(
                    io::ErrorKind::Other,
                    format!("delete unexpected response {:?}", unexpected),
                )
                .into(),
                filepath: path.to_path_buf(),
            }),
        }
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        todo!("directory exists");
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        self.request_sender
            .send(ChannelRequest::AcquireLock(Lock {
                filepath: lock.filepath.clone(),
                is_blocking: lock.is_blocking,
            }))
            .unwrap();

        match self.response_receiver.recv().unwrap() {
            ChannelResponse::AcquiredLock(blocking_lock) => {
                Ok(DirectoryLock::from(Box::new(ChannelLock {
                    lock: Some(blocking_lock),
                    sender: self.request_sender.clone(),
                })))
            }
            unexpected => Err(LockError::IoError(
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("acquire_lock unexpected response {:?}", unexpected),
                )
                .into(),
            )),
        }
        // Ok(DirectoryLock::from(Box::new(Lock {
        //     filepath: lock.filepath.clone(),
        //     is_blocking: true,
        // })))
    }

    // Internally, tantivy only uses this API to detect new commits to implement the
    // `OnCommitWithDelay` `ReloadPolicy`. Not implementing watch in a `Directory` only prevents
    // the `OnCommitWithDelay` `ReloadPolicy` to work properly.
    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        unimplemented!("OnCommitWithDelay ReloadPolicy not supported");
    }

    // Block storage handles disk writes for us, we don't need to fsync
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

pub struct ChannelRequestHandler {
    directory: BlockingDirectory,
    relation_oid: u32,
    sender: Sender<ChannelResponse>,
    receiver: Receiver<ChannelRequest>,
}

pub struct ChannelRequestStats {
    pub pages_deleted: u32,
}

impl ChannelRequestHandler {
    pub fn open(
        directory: BlockingDirectory,
        relation_oid: u32,
        sender: Sender<ChannelResponse>,
        receiver: Receiver<ChannelRequest>,
    ) -> Self {
        Self {
            directory: directory,
            relation_oid,
            receiver,
            sender,
        }
    }

    pub fn receive_blocking(
        &self,
        should_delete: Option<impl Fn(u64) -> bool>,
    ) -> Result<ChannelRequestStats> {
        let mut pages_deleted = 0;
        for message in self.receiver.iter() {
            match message {
                ChannelRequest::AcquireLock(lock) => {
                    let blocking_lock = unsafe { self.directory.acquire_blocking_lock(&lock)? };
                    self.sender
                        .send(ChannelResponse::AcquiredLock(blocking_lock))?;
                }
                ChannelRequest::AtomicRead(path) => {
                    let data = self.directory.atomic_read(&path)?;
                    self.sender.send(ChannelResponse::Bytes(data))?;
                }
                ChannelRequest::AtomicWrite(path, data) => {
                    self.directory.atomic_write(&path, &data)?;
                    self.sender.send(ChannelResponse::AtomicWriteAck)?;
                }
                ChannelRequest::GetSegmentHandle(path) => {
                    let handle = unsafe { SegmentHandle::open(self.relation_oid, &path)? };
                    self.sender.send(ChannelResponse::SegmentHandle(handle))?;
                }
                ChannelRequest::ReleaseBlockingLock(blocking_lock) => {
                    drop(blocking_lock);
                }
                ChannelRequest::SegmentRead(path, range, handle) => {
                    let reader = FileHandleReader::new(self.relation_oid, &path, handle);
                    let data = reader.read_bytes(range)?;
                    self.sender
                        .send(ChannelResponse::Bytes(data.as_slice().to_owned()))?;
                }
                ChannelRequest::SegmentWrite(path, data) => {
                    let mut writer = unsafe { IoWriter::new(self.relation_oid, &path) };
                    writer.write_all(data.get_ref())?;
                    self.sender.send(ChannelResponse::SegmentWriteAck)?;
                }
                ChannelRequest::SegmentDelete(path) => {
                    pages_deleted += self.directory.delete_with_stats(&path)?;
                    self.sender.send(ChannelResponse::SegmentDeleteAck)?;
                }
                ChannelRequest::ShouldDeleteCtids(ctids) => {
                    if let Some(ref should_delete) = should_delete {
                        let filtered_ctids: Vec<u64> = ctids
                            .into_iter()
                            .filter(|&ctid_val| should_delete(ctid_val))
                            .collect();
                        self.sender
                            .send(ChannelResponse::ShouldDeleteCtids(filtered_ctids))?;
                    } else {
                        self.sender
                            .send(ChannelResponse::ShouldDeleteCtids(vec![]))?;
                    }
                }
                ChannelRequest::Terminate => break,
            }
        }

        Ok(ChannelRequestStats { pages_deleted })
    }
}
