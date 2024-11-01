use crate::postgres::buffer::{BufferCache, SEGMENT_HANDLE_BLOCKNO};
use anyhow::Result;
use pgrx::*;
use serde::{Deserialize, Serialize};
use serde_json::from_slice;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SegmentHandle {
    pub path: PathBuf,
    pub blocks: Vec<pg_sys::BlockNumber>,
    pub total_bytes: usize,
}

impl SegmentHandle {
    pub unsafe fn open(relation_oid: u32, path: &Path) -> Result<Option<Self>> {
        let cache = BufferCache::open(relation_oid);
        let buffer = cache.get_buffer(SEGMENT_HANDLE_BLOCKNO, Some(pg_sys::BUFFER_LOCK_SHARE));
        let page = pg_sys::BufferGetPage(buffer);

        let mut offsetno = pg_sys::FirstOffsetNumber;
        // TODO: Implement a way to read the next block if the current block is full
        while offsetno <= pg_sys::PageGetMaxOffsetNumber(page) {
            let item_id = pg_sys::PageGetItemId(page, offsetno);
            let item = pg_sys::PageGetItem(page, item_id);
            let segment: SegmentHandle = from_slice(from_raw_parts(
                item as *const u8,
                (*item_id).lp_len() as usize,
            ))?;
            if segment.path == path {
                pg_sys::UnlockReleaseBuffer(buffer);
                return Ok(Some(segment));
            }
            offsetno += 1;
        }

        pg_sys::UnlockReleaseBuffer(buffer);
        Ok(None)
    }

    pub unsafe fn create(
        relation_oid: u32,
        path: &Path,
        blocks: Vec<pg_sys::BlockNumber>,
        total_bytes: usize,
    ) {
        let cache = BufferCache::open(relation_oid);
        let buffer = cache.get_buffer(SEGMENT_HANDLE_BLOCKNO, Some(pg_sys::BUFFER_LOCK_SHARE));
        let page = pg_sys::BufferGetPage(buffer);

        if pg_sys::PageGetFreeSpace(page) < size_of::<SegmentHandle>() {
            // let new_buffer = cache.new_buffer(size_of::<SegmentHandle>());
            // (*special).insert_blockno = pg_sys::BufferGetBlockNumber(new_buffer);
            // pg_sys::MarkBufferDirty(buffer);
            // buffer = new_buffer;
            // page = pg_sys::BufferGetPage(buffer);
            todo!("go to next page");
        }

        let segment = SegmentHandle {
            path: path.to_path_buf(),
            blocks,
            total_bytes,
        };
        let serialized: Vec<u8> = serde_json::to_vec(&segment).unwrap();
        pg_sys::PageAddItemExtended(
            page,
            serialized.as_ptr() as pg_sys::Item,
            serialized.len(),
            pg_sys::InvalidOffsetNumber,
            0,
        );
        pg_sys::MarkBufferDirty(buffer);
        pg_sys::UnlockReleaseBuffer(buffer);
    }
}
