use anyhow::Result;
use crossbeam::channel::{Receiver, Sender};
use std::ops::Range;
use std::path::{Path, PathBuf};
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::HasLen;

use crate::postgres::storage::segment_handle::SegmentHandle;

use super::directory::{ChannelRequest, ChannelResponse};

#[derive(Clone, Debug)]
pub struct ChannelReader {
    path: PathBuf,
    handle: SegmentHandle,
    relation_oid: u32,
    sender: Sender<ChannelRequest>,
    receiver: Receiver<ChannelResponse>,
}

impl ChannelReader {
    pub unsafe fn new(
        relation_oid: u32,
        path: &Path,
        sender: Sender<ChannelRequest>,
        receiver: Receiver<ChannelResponse>,
    ) -> Result<Self> {
        sender
            .send(ChannelRequest::GetSegmentHandle(path.to_path_buf()))
            .unwrap();
        let handle = match receiver.recv().unwrap() {
            ChannelResponse::SegmentHandle(handle) => handle.expect("SegmentHandle should exist"),
            unexpected => panic!("SegmentHandle expected, got {:?}", unexpected),
        };

        Ok(Self {
            path: path.to_path_buf(),
            handle,
            relation_oid,
            sender,
            receiver,
        })
    }
}

impl FileHandle for ChannelReader {
    fn read_bytes(&self, range: Range<usize>) -> Result<OwnedBytes, std::io::Error> {
        self.sender
            .send(ChannelRequest::SegmentRead(
                self.path.clone(),
                range.clone(),
                self.handle.clone(),
            ))
            .unwrap();
        let data = match self.receiver.recv().unwrap() {
            ChannelResponse::Bytes(data) => data,
            unexpected => panic!("Bytes expected, got {:?}", unexpected),
        };

        Ok(OwnedBytes::new(data))
    }
}

impl HasLen for ChannelReader {
    fn len(&self) -> usize {
        self.handle.internal().total_bytes()
    }
}
