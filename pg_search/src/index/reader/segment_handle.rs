use anyhow::Result;
use pgrx::*;
use std::io::{Error, ErrorKind};
use std::ops::Range;
use std::slice::from_raw_parts;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::HasLen;

use crate::index::segment_handle::SegmentHandle;
use crate::postgres::buffer::BufferCache;
use crate::postgres::utils::max_heap_tuple_size;

#[derive(Clone, Debug)]
pub struct SegmentHandleReader {
    handle: SegmentHandle,
    relation_oid: u32,
}

impl SegmentHandleReader {
    pub fn new(relation_oid: u32, handle: SegmentHandle) -> Self {
        Self {
            handle,
            relation_oid,
        }
    }
}

impl FileHandle for SegmentHandleReader {
    fn read_bytes(&self, range: Range<usize>) -> Result<OwnedBytes, Error> {
        unsafe {
            const MAX_HEAP_TUPLE_SIZE: usize = unsafe { max_heap_tuple_size() };
            let cache = BufferCache::open(self.relation_oid);
            let start = range.start;
            let end = range.end.min(self.len());
            if start >= end {
                return Err(Error::new(ErrorKind::InvalidInput, "Invalid range"));
            }
            let start_block = start / MAX_HEAP_TUPLE_SIZE;
            let end_block = end / MAX_HEAP_TUPLE_SIZE;
            let blocks = self.handle.blocks.clone();
            let mut data: Vec<u8> = vec![];

            for (i, blockno) in blocks
                .iter()
                .enumerate()
                .take(end_block + 1)
                .skip(start_block)
            {
                let buffer = cache.get_buffer(*blockno, Some(pg_sys::BUFFER_LOCK_SHARE));
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

                pg_sys::UnlockReleaseBuffer(buffer);
            }

            Ok(OwnedBytes::new(data))
        }
    }
}

impl HasLen for SegmentHandleReader {
    fn len(&self) -> usize {
        self.handle.total_bytes
    }
}
