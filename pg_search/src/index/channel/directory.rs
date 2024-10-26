use crossbeam::channel::{Receiver, Sender};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{io, io::Cursor, ops::Range, result};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr};
use tantivy::Directory;

use super::reader::ChannelReader;
use super::writer::ChannelWriter;
use crate::postgres::storage::segment_handle::SegmentHandle;

#[derive(Debug)]
pub enum ChannelRequest {
    AtomicRead(PathBuf),
    AtomicWrite(PathBuf, Vec<u8>),
    SegmentRead(PathBuf, Range<usize>, SegmentHandle),
    SegmentWrite(PathBuf, Cursor<Vec<u8>>),
    GetSegmentHandle(PathBuf),
    ShouldDeleteCtids(Vec<u64>),
    Terminate,
}

#[derive(Debug)]
pub enum ChannelResponse {
    Bytes(Vec<u8>),
    SegmentHandle(Option<SegmentHandle>),
    SegmentWriteAck,
    AtomicWriteAck,
    ShouldDeleteCtids(Vec<u64>),
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
        // TODO: What to do with a deleted segment?
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        todo!("directory exists");
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        // The lock itself doesn't seem to actually be used anywhere by Tantivy
        // acquire_lock seems to be a place for us to implement our own locking behavior
        // which we don't need since pg_sys::ReadBuffer is already handling this
        Ok(DirectoryLock::from(Box::new(Lock {
            filepath: lock.filepath.clone(),
            is_blocking: true,
        })))
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        todo!("directory watch");
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}
