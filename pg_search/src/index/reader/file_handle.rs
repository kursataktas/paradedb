use anyhow::Result;
use pgrx::*;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::HasLen;

use crate::index::segment_handle::SegmentHandle;
use crate::postgres::buffer::BufferCache;
use crate::postgres::utils::max_heap_tuple_size;

#[derive(Clone, Debug)]
pub struct FileHandleReader {
    path: PathBuf,
    handle: SegmentHandle,
    relation_oid: u32,
}

impl FileHandleReader {
    pub fn new(relation_oid: u32, path: &Path, handle: SegmentHandle) -> Self {
        Self {
            path: path.to_path_buf(),
            handle,
            relation_oid,
        }
    }
}

impl FileHandle for FileHandleReader {
    fn read_bytes(&self, range: Range<usize>) -> Result<OwnedBytes, std::io::Error> {
        unsafe {
            const MAX_HEAP_TUPLE_SIZE: usize = unsafe { max_heap_tuple_size() };
            let cache = BufferCache::open(self.relation_oid);
            let start = range.start;
            let end = range.end;
            let start_block = start / MAX_HEAP_TUPLE_SIZE;
            let end_block = end / MAX_HEAP_TUPLE_SIZE;
            let blocks = self.handle.blocks.clone();
            let mut data: Vec<u8> = vec![];

            for i in start_block..=end_block {
                let buffer = cache.get_buffer(blocks[i], None);
                let page = pg_sys::BufferGetPage(buffer);
                let item_id = pg_sys::PageGetItemId(page, pg_sys::FirstOffsetNumber);
                let item = pg_sys::PageGetItem(page, item_id);

                let slice_start = if i == start_block {
                    start % MAX_HEAP_TUPLE_SIZE
                } else {
                    0
                };
                let slice_end = if i == end_block {
                    end % MAX_HEAP_TUPLE_SIZE
                } else {
                    MAX_HEAP_TUPLE_SIZE
                };
                let slice_len = slice_end - slice_start;
                let slice = from_raw_parts(item.add(slice_start) as *const u8, slice_len);
                data.extend_from_slice(slice);

                pg_sys::ReleaseBuffer(buffer);
            }

            Ok(OwnedBytes::new(data))
        }
    }
}

impl HasLen for FileHandleReader {
    fn len(&self) -> usize {
        self.handle.total_bytes
    }
}
