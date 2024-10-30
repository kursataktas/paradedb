use crossbeam::channel::{Receiver, Sender};
use std::io::{Cursor, Result, Write};
use std::path::{Path, PathBuf};
use tantivy::directory::{AntiCallToken, TerminatingWrite};

use crate::index::directory::channel::{ChannelRequest, ChannelResponse};

#[derive(Clone, Debug)]
pub struct ChannelWriter {
    path: PathBuf,
    data: Cursor<Vec<u8>>,
    sender: Sender<ChannelRequest>,
    receiver: Receiver<ChannelResponse>,
}

impl ChannelWriter {
    pub unsafe fn new(
        path: &Path,
        sender: Sender<ChannelRequest>,
        receiver: Receiver<ChannelResponse>,
    ) -> Self {
        Self {
            path: path.to_path_buf(),
            data: Cursor::new(Vec::new()),
            sender,
            receiver,
        }
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.data.write_all(data)?;
        Ok(data.len())
    }

    // TODO: Implement flush so we don't hold the entire buffer in memory
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for ChannelWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> Result<()> {
        self.sender
            .send(ChannelRequest::SegmentWrite(
                self.path.clone(),
                self.data.clone(),
            ))
            .unwrap();
        match self.receiver.recv().unwrap() {
            ChannelResponse::SegmentWriteAck => Ok(()),
            unexpected => panic!("SegmentWrite expected, got {:?}", unexpected),
        }
    }
}
