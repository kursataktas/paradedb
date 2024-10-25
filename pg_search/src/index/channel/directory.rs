use crossbeam::channel::{Receiver, Sender};
use std::path::{Path, PathBuf};
use std::{io, ops::Range, result};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};

use crate::postgres::storage::segment_handle::SegmentHandle;

pub enum ChannelRequest {
    AtomicRead(PathBuf),
    AtomicWrite(PathBuf, Vec<u8>),
    SegmentRead(PathBuf, Range<usize>, SegmentHandle),
    SegmentWrite(PathBuf, Vec<u8>),
    GetSegmentHandle(PathBuf),
}

#[derive(Debug)]
pub enum ChannelResponse {
    Bytes(Vec<u8>),
    SegmentHandle(Option<SegmentHandle>),
    SegmentWriteAck,
    AtomicWriteAck,
}

struct ChannelDirectory {
    request_sender: Sender<ChannelRequest>,
    request_receiver: Receiver<ChannelResponse>,
    response_sender: Sender<ChannelRequest>,
    response_receiver: Receiver<ChannelResponse>,
}

// A directory that actually forwards all read/write requests to a channel
// This channel is used to communicate with the actual storage implementation
impl ChannelDirectory {
    pub fn new(
        request_channel: (Sender<ChannelRequest>, Receiver<ChannelResponse>),
        response_channel: (Sender<ChannelRequest>, Receiver<ChannelResponse>),
    ) -> Self {
        let (request_sender, request_receiver) = request_channel;
        let (response_sender, response_receiver) = response_channel;

        Self {
            request_sender,
            response_receiver,
            response_sender,
            request_receiver,
        }
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

    fn atomic_write(&self, path: &Path, data: &[u8]) -> result::Result<(), OpenWriteError> {
        self.request_sender
            .send(ChannelRequest::AtomicWrite(
                path.to_path_buf(),
                data.to_vec(),
            ))
            .unwrap();

        match self.response_receiver.recv().unwrap() {
            ChannelResponse::AtomicWriteAck => Ok(()),
            unexpected => Err(OpenWriteError::wrap_io_error(
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("atomic_write unexpected response {:?}", unexpected),
                ),
                path.to_path_buf(),
            )),
        }
    }
}
