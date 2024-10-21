use anyhow::Result;
use pgrx::*;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;
use tantivy::HasLen;

use crate::postgres::storage::buffer::BufferCache;
use crate::postgres::storage::segment_handle::SegmentHandle;
use crate::postgres::utils::max_heap_tuple_size;

#[derive(Clone, Debug)]
pub struct SegmentReader {
    path: PathBuf,
    handle: SegmentHandle,
    relation_oid: u32,
}

impl SegmentReader {
    pub unsafe fn new(relation_oid: u32, path: &Path) -> Result<Self> {
        let handle = SegmentHandle::open(relation_oid, path)?
            .expect(&format!("SegmentHandle should exist for {:?}", path));
        Ok(Self {
            path: path.to_path_buf(),
            handle,
            relation_oid,
        })
    }
}

impl FileHandle for SegmentReader {
    fn read_bytes(&self, range: Range<usize>) -> Result<OwnedBytes, std::io::Error> {
        unsafe {
            const MAX_HEAP_TUPLE_SIZE: usize = unsafe { max_heap_tuple_size() };
            let cache = BufferCache::open(self.relation_oid);
            let start = range.start as usize;
            let end = range.end as usize;
            let start_block = start / MAX_HEAP_TUPLE_SIZE;
            let end_block = end / MAX_HEAP_TUPLE_SIZE;
            let blocks = self.handle.internal().blocks();
            let mut data: Vec<u8> = vec![];

            pgrx::info!(
                "read_bytes: {:?} start_block: {} end_block: {}",
                self.path,
                start_block,
                end_block
            );

            pgrx::info!("blocks: {:?}", blocks);

            for blockno in blocks
                .iter()
                .skip(start_block)
                .take(end_block - start_block + 1)
            {
                pgrx::info!("here");
                let buffer = cache.get_buffer(*blockno, pg_sys::BUFFER_LOCK_SHARE);
                let page = pg_sys::BufferGetPage(buffer);
                let item_id = pg_sys::PageGetItemId(page, pg_sys::FirstOffsetNumber);
                let item = pg_sys::PageGetItem(page, item_id);
                let len = (*item_id).lp_len() as usize;

                let slice_start = start % MAX_HEAP_TUPLE_SIZE as usize;
                let slice_end = end % MAX_HEAP_TUPLE_SIZE as usize;
                pgrx::info!(
                    "read_bytes: {:?} slice_start: {} slice_end: {}",
                    self.path,
                    slice_start,
                    slice_end
                );
                let slice_len = slice_end - slice_start;
                let mut vec: Vec<u8> = Vec::with_capacity(slice_len);
                let slice = from_raw_parts(item.add(slice_start as usize) as *const u8, slice_len);
                data.extend_from_slice(slice);
            }

            pgrx::info!("got data {:?}", data);
            Ok(OwnedBytes::new(data))
        }
    }
}

impl HasLen for SegmentReader {
    fn len(&self) -> usize {
        self.handle.internal().total_bytes()
    }
}
