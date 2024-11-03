use crate::postgres::buffer::{
    BufferCache, LinkedBlockSpecialData, TANTIVY_MANAGED_BLOCKNO, TANTIVY_META_BLOCKNO,
};
use crate::postgres::utils::max_heap_tuple_size;
use pgrx::*;

// Handles Tantivy's atomic_read and atomic_write over block storage
#[derive(Clone, Debug)]
pub struct AtomicDirectory {
    relation_oid: u32,
}

impl AtomicDirectory {
    pub unsafe fn new(relation_oid: u32) -> Self {
        Self { relation_oid }
    }

    pub unsafe fn read_meta(&self) -> Vec<u8> {
        self.read_bytes(TANTIVY_META_BLOCKNO)
    }

    pub unsafe fn read_managed(&self) -> Vec<u8> {
        self.read_bytes(TANTIVY_MANAGED_BLOCKNO)
    }

    pub unsafe fn write_meta(&self, data: &[u8]) {
        self.write_bytes(data, TANTIVY_META_BLOCKNO);
    }

    pub unsafe fn write_managed(&self, data: &[u8]) {
        self.write_bytes(data, TANTIVY_MANAGED_BLOCKNO);
    }

    unsafe fn read_bytes(&self, blockno: pg_sys::BlockNumber) -> Vec<u8> {
        let cache = BufferCache::open(self.relation_oid);
        let mut current_blockno = blockno;
        let mut data = Vec::new();

        while current_blockno != pg_sys::InvalidBlockNumber {
            let buffer = cache.get_buffer(current_blockno, Some(pg_sys::BUFFER_LOCK_SHARE));
            let page = pg_sys::BufferGetPage(buffer);
            let item_id = pg_sys::PageGetItemId(page, pg_sys::FirstOffsetNumber);
            let item = pg_sys::PageGetItem(page, item_id);
            let len = (*item_id).lp_len() as usize;

            data.extend(std::slice::from_raw_parts(item as *const u8, len));

            let special = pg_sys::PageGetSpecialPointer(page) as *mut LinkedBlockSpecialData;
            current_blockno = (*special).next_blockno;
            pg_sys::UnlockReleaseBuffer(buffer);
        }

        data
    }

    unsafe fn write_bytes(&self, data: &[u8], blockno: pg_sys::BlockNumber) {
        const MAX_HEAP_TUPLE_SIZE: usize = unsafe { max_heap_tuple_size() };
        let cache = BufferCache::open(self.relation_oid);
        let mut buffer = cache.get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE));
        let mut page = pg_sys::BufferGetPage(buffer);

        for (i, chunk) in data.chunks(MAX_HEAP_TUPLE_SIZE).enumerate() {
            if i > 0 {
                let special = pg_sys::PageGetSpecialPointer(page) as *mut LinkedBlockSpecialData;
                if (*special).next_blockno == pg_sys::InvalidBlockNumber {
                    let new_buffer = cache.new_buffer(size_of::<LinkedBlockSpecialData>());
                    (*special).next_blockno = pg_sys::BufferGetBlockNumber(new_buffer);
                    pg_sys::MarkBufferDirty(buffer);
                    pg_sys::UnlockReleaseBuffer(buffer);
                    buffer = new_buffer;
                    page = pg_sys::BufferGetPage(buffer);
                } else {
                    let next_blockno = (*special).next_blockno;
                    pg_sys::MarkBufferDirty(buffer);
                    pg_sys::UnlockReleaseBuffer(buffer);
                    buffer = cache.get_buffer(next_blockno, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE));
                    page = pg_sys::BufferGetPage(buffer);
                }
            }

            if pg_sys::PageGetMaxOffsetNumber(page) == pg_sys::InvalidOffsetNumber {
                pg_sys::PageAddItemExtended(
                    page,
                    chunk.as_ptr() as pg_sys::Item,
                    chunk.len(),
                    pg_sys::FirstOffsetNumber,
                    0,
                );
            } else {
                pg_sys::PageIndexTupleOverwrite(
                    page,
                    pg_sys::FirstOffsetNumber,
                    chunk.as_ptr() as pg_sys::Item,
                    chunk.len(),
                );
            }
        }

        pg_sys::MarkBufferDirty(buffer);
        pg_sys::UnlockReleaseBuffer(buffer);
    }
}
