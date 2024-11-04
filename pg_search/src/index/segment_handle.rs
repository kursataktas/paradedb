use crate::postgres::buffer::{BufferCache, SEARCH_META_BLOCKNO};
use anyhow::Result;
use pgrx::*;
use serde::{Deserialize, Serialize};
use serde_json::from_slice;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;

pub(crate) struct SegmentHandleSpecialData {
    // If the metadata block overflows, the next block to write to
    pub next_blockno: pg_sys::BlockNumber,
    // The block number that stores .meta.json
    pub meta_blockno: pg_sys::BlockNumber,
    // The block number that stores .managed.json
    pub managed_blockno: pg_sys::BlockNumber,
}

#[derive(Clone, Debug)]
pub(crate) struct SegmentHandle {
    // Tracks the handle is physically stored
    blockno: pg_sys::BlockNumber,
    offsetno: pg_sys::OffsetNumber,
    relation_oid: u32,
    internal: SegmentHandleInternal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SegmentHandleInternal {
    path: PathBuf,
    blocks: Vec<pg_sys::BlockNumber>,
    total_bytes: usize,
}

impl SegmentHandleInternal {
    pub fn new(path: PathBuf, blocks: Vec<pg_sys::BlockNumber>, total_bytes: usize) -> Self {
        Self {
            path,
            blocks,
            total_bytes,
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn blocks(&self) -> Vec<pg_sys::BlockNumber> {
        self.blocks.clone()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }
}

impl SegmentHandle {
    pub unsafe fn open(relation_oid: u32, path: &Path) -> Result<Option<Self>> {
        let cache = BufferCache::open(relation_oid);
        let buffer = cache.get_buffer(SEARCH_META_BLOCKNO, Some(pg_sys::BUFFER_LOCK_SHARE));
        let blockno = pg_sys::BufferGetBlockNumber(buffer);
        let page = pg_sys::BufferGetPage(buffer);

        let mut offsetno = pg_sys::FirstOffsetNumber;
        // TODO: Implement a way to read the next block if the current block is full
        // TODO: Would a HashMap implementation be more efficient?
        while offsetno <= pg_sys::PageGetMaxOffsetNumber(page) {
            let item_id = pg_sys::PageGetItemId(page, offsetno);
            let item = pg_sys::PageGetItem(page, item_id);
            let segment: SegmentHandleInternal = from_slice(from_raw_parts(
                item as *const u8,
                (*item_id).lp_len() as usize,
            ))?;
            if segment.path == path {
                let internal = SegmentHandleInternal::new(
                    segment.path.clone(),
                    segment.blocks,
                    segment.total_bytes,
                );
                pg_sys::UnlockReleaseBuffer(buffer);
                return Ok(Some(Self {
                    blockno,
                    offsetno,
                    relation_oid,
                    internal,
                }));
            }
            offsetno += 1;
        }

        pg_sys::UnlockReleaseBuffer(buffer);
        Ok(None)
    }

    pub unsafe fn create(relation_oid: u32, internal: SegmentHandleInternal) -> Self {
        let cache = BufferCache::open(relation_oid);
        let mut buffer = cache.get_buffer(SEARCH_META_BLOCKNO, Some(pg_sys::BUFFER_LOCK_SHARE));
        let mut page = pg_sys::BufferGetPage(buffer);
        let special = pg_sys::PageGetSpecialPointer(page) as *mut SegmentHandleSpecialData;

        if pg_sys::PageGetFreeSpace(page) < size_of::<SegmentHandleInternal>() {
            let new_buffer = cache.new_buffer(size_of::<SegmentHandleInternal>());
            (*special).next_blockno = pg_sys::BufferGetBlockNumber(new_buffer);
            pg_sys::MarkBufferDirty(buffer);
            buffer = new_buffer;
            page = pg_sys::BufferGetPage(buffer);
        }

        let serialized: Vec<u8> = serde_json::to_vec(&internal).unwrap();
        let offsetno = pg_sys::PageAddItemExtended(
            page,
            serialized.as_ptr() as pg_sys::Item,
            serialized.len(),
            pg_sys::InvalidOffsetNumber,
            0,
        );

        pg_sys::MarkBufferDirty(buffer);
        pg_sys::UnlockReleaseBuffer(buffer);

        Self {
            blockno: pg_sys::BufferGetBlockNumber(buffer),
            offsetno,
            relation_oid,
            internal,
        }
    }

    pub fn internal(&self) -> &SegmentHandleInternal {
        &self.internal
    }
}
