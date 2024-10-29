use anyhow::Result;
use crossbeam::channel::{Receiver, Sender};
use std::ops::Range;
use std::path::Path;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::HasLen;

use crate::index::directory::channel::{ChannelRequest, ChannelResponse};
use crate::index::segment_handle::SegmentHandle;

#[derive(Clone, Debug)]
pub struct ChannelReader {
    handle: SegmentHandle,
    sender: Sender<ChannelRequest>,
    receiver: Receiver<ChannelResponse>,
}

impl ChannelReader {
    pub unsafe fn new(
        path: &Path,
        sender: Sender<ChannelRequest>,
        receiver: Receiver<ChannelResponse>,
    ) -> Result<Self> {
        sender
            .send(ChannelRequest::GetSegmentHandle(path.to_path_buf()))
            .unwrap();
        let handle = match receiver.recv().unwrap() {
            ChannelResponse::SegmentHandle(handle) => {
                handle.expect(format!("SegmentHandle for {} should exist", path.display()).as_str())
            }
            unexpected => panic!("SegmentHandle expected, got {:?}", unexpected),
        };

        Ok(Self {
            handle,
            sender,
            receiver,
        })
    }
}

impl FileHandle for ChannelReader {
    fn read_bytes(&self, range: Range<usize>) -> Result<OwnedBytes, std::io::Error> {
        self.sender
            .send(ChannelRequest::SegmentRead(
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
        self.handle.total_bytes
    }
}
