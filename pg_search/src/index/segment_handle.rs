use crate::postgres::buffer::{
    BufferCache, LinkedBlockSpecialData, MetaPageData, METADATA_BLOCKNO, SEGMENT_HANDLE_BLOCKNO,
};
use anyhow::{bail, Result};
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
        let mut blockno = SEGMENT_HANDLE_BLOCKNO;
        let mut buffer = cache.get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_SHARE));

        while blockno != pg_sys::InvalidBlockNumber {
            let page = pg_sys::BufferGetPage(buffer);
            let mut offsetno = pg_sys::FirstOffsetNumber;

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

            blockno = {
                let special = pg_sys::PageGetSpecialPointer(page) as *mut LinkedBlockSpecialData;
                (*special).next_blockno
            };
            pg_sys::UnlockReleaseBuffer(buffer);
            buffer = cache.get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_SHARE));
        }

        Ok(None)
    }

    pub unsafe fn create(
        relation_oid: u32,
        path: &Path,
        blocks: Vec<pg_sys::BlockNumber>,
        total_bytes: usize,
    ) -> Result<()> {
        let cache = BufferCache::open(relation_oid);
        let metadata_buffer =
            cache.get_buffer(METADATA_BLOCKNO, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE));
        let metadata_page = pg_sys::BufferGetPage(metadata_buffer);
        let metadata_data = pg_sys::PageGetContents(metadata_page) as *mut MetaPageData;
        let segment_handle_insert_blockno = (*metadata_data).segment_handle_insert_blockno;

        let insert_buffer = cache.get_buffer(
            segment_handle_insert_blockno,
            Some(pg_sys::BUFFER_LOCK_EXCLUSIVE),
        );
        let insert_page = pg_sys::BufferGetPage(insert_buffer);

        let segment = SegmentHandle {
            path: path.to_path_buf(),
            blocks,
            total_bytes,
        };
        let serialized: Vec<u8> = serde_json::to_vec(&segment).unwrap();

        if pg_sys::PageAddItemExtended(
            insert_page,
            serialized.as_ptr() as pg_sys::Item,
            serialized.len(),
            pg_sys::InvalidOffsetNumber,
            0,
        ) == pg_sys::InvalidOffsetNumber
        {
            let special = pg_sys::PageGetSpecialPointer(insert_page) as *mut LinkedBlockSpecialData;
            let new_buffer = cache.new_buffer(size_of::<LinkedBlockSpecialData>());
            let new_blockno = pg_sys::BufferGetBlockNumber(new_buffer);
            (*metadata_data).segment_handle_insert_blockno = new_blockno;
            (*special).next_blockno = new_blockno;

            pg_sys::MarkBufferDirty(metadata_buffer);
            pg_sys::MarkBufferDirty(insert_buffer);
            pg_sys::UnlockReleaseBuffer(metadata_buffer);
            pg_sys::UnlockReleaseBuffer(insert_buffer);

            let new_page = pg_sys::BufferGetPage(new_buffer);

            if pg_sys::PageAddItemExtended(
                new_page,
                serialized.as_ptr() as pg_sys::Item,
                serialized.len(),
                pg_sys::InvalidOffsetNumber,
                0,
            ) == pg_sys::InvalidOffsetNumber
            {
                bail!("Failed to write SegmentHandle for {:?}", path);
            }

            pg_sys::MarkBufferDirty(new_buffer);
            pg_sys::UnlockReleaseBuffer(new_buffer);
        } else {
            pg_sys::MarkBufferDirty(insert_buffer);
            pg_sys::UnlockReleaseBuffer(insert_buffer);
            pg_sys::UnlockReleaseBuffer(metadata_buffer);
        }

        Ok(())
    }
}
