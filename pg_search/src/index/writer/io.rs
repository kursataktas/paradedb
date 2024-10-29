use pgrx::*;
use std::io::{Cursor, Read, Result, Seek, Write};
use std::path::{Path, PathBuf};
use tantivy::directory::{AntiCallToken, TerminatingWrite};

use crate::index::segment_handle::SegmentHandle;
use crate::postgres::buffer::BufferCache;
use crate::postgres::utils::max_heap_tuple_size;

#[derive(Clone, Debug)]
pub struct IoWriter {
    relation_oid: u32,
    path: PathBuf,
    data: Cursor<Vec<u8>>,
}

impl IoWriter {
    pub unsafe fn new(relation_oid: u32, path: &Path) -> Self {
        assert!(
            !path.to_str().unwrap().ends_with(".lock"),
            ".lock files should not be written"
        );

        Self {
            relation_oid,
            path: path.to_path_buf(),
            data: Cursor::new(Vec::new()),
        }
    }
}

impl Write for IoWriter {
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

impl TerminatingWrite for IoWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> Result<()> {
        unsafe {
            const MAX_HEAP_TUPLE_SIZE: usize = unsafe { max_heap_tuple_size() };
            let mut sink = [0; MAX_HEAP_TUPLE_SIZE];

            let cache = BufferCache::open(self.relation_oid);
            let total_bytes = self.data.get_ref().len();
            self.data.seek(std::io::SeekFrom::Start(0))?;
            let mut blocks: Vec<pg_sys::BlockNumber> = vec![];

            while let Ok(bytes_read) = self.data.read(&mut sink) {
                if bytes_read == 0 {
                    break;
                }

                let buffer = cache.new_buffer(0);
                let page = pg_sys::BufferGetPage(buffer);
                let data_slice = &sink[0..bytes_read];

                pg_sys::PageAddItemExtended(
                    page,
                    data_slice.as_ptr() as pg_sys::Item,
                    data_slice.len(),
                    pg_sys::InvalidOffsetNumber,
                    0,
                );

                blocks.push(pg_sys::BufferGetBlockNumber(buffer));
                pg_sys::MarkBufferDirty(buffer);
                pg_sys::UnlockReleaseBuffer(buffer);
            }

            SegmentHandle::create(self.relation_oid, &self.path, blocks, total_bytes);
            Ok(())
        }
    }
}
