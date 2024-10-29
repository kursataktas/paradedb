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
        assert!(
            !path.to_str().unwrap().ends_with(".lock"),
            ".lock files should not be written"
        );

        Self {
            path: path.to_path_buf(),
            data: Cursor::new(Vec::new()),
            sender,
            receiver,
        }
    }
}

impl Write for ChannelWriter {
    // This function will attempt to write the entire contents of `buf`, but
    // the entire write might not succeed, or the write may also generate an
    // error. Typically, a call to `write` represents one attempt to write to
    // any wrapped object.
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.data.write_all(data)?;
        Ok(data.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl TerminatingWrite for ChannelWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> Result<()> {
        eprintln!("ChannelWriter::terminate_ref");
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
