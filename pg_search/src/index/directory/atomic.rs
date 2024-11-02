use crate::postgres::buffer::{BufferCache, TANTIVY_MANAGED_BLOCKNO, TANTIVY_META_BLOCKNO};
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

    // TODO: Handle read_bytes and write_bytes where data is larger than a page
    unsafe fn read_bytes(&self, blockno: pg_sys::BlockNumber) -> Vec<u8> {
        let cache = BufferCache::open(self.relation_oid);
        let buffer = cache.get_buffer(blockno, None);
        let page = pg_sys::BufferGetPage(buffer);
        let item_id = pg_sys::PageGetItemId(page, pg_sys::FirstOffsetNumber);
        let item = pg_sys::PageGetItem(page, item_id);
        let len = (*item_id).lp_len() as usize;

        let mut vec = Vec::with_capacity(len);
        std::ptr::copy(item as *mut u8, vec.as_mut_ptr(), len);
        vec.set_len(len);
        pg_sys::ReleaseBuffer(buffer);
        vec
    }

    unsafe fn write_bytes(&self, data: &[u8], blockno: pg_sys::BlockNumber) {
        let cache = BufferCache::open(self.relation_oid);
        // It is our responsibility to ensure that this buffer is already locked and pinned
        let buffer = cache.get_buffer(blockno, None);
        let page = pg_sys::BufferGetPage(buffer);

        if pg_sys::PageGetMaxOffsetNumber(page) == pg_sys::InvalidOffsetNumber {
            pg_sys::PageAddItemExtended(
                page,
                data.as_ptr() as pg_sys::Item,
                data.len(),
                pg_sys::FirstOffsetNumber,
                0,
            );
        } else {
            pg_sys::PageIndexTupleOverwrite(
                page,
                pg_sys::FirstOffsetNumber,
                data.as_ptr() as pg_sys::Item,
                data.len(),
            );
        }

        // for (i, chunk) in data.chunks(MAX_HEAP_TUPLE_SIZE).enumerate() {
        //     if i > 0 {
        //         let special = pg_sys::PageGetSpecialPointer(page) as *mut LinkedBlockSpecialData;
        //         if (*special).next_blockno == pg_sys::InvalidBlockNumber {
        //             let new_buffer = cache.new_buffer(size_of::<LinkedBlockSpecialData>());
        //             (*special).next_blockno = pg_sys::BufferGetBlockNumber(new_buffer);
        //             pg_sys::MarkBufferDirty(buffer);
        //             pg_sys::ReleaseBuffer(buffer);
        //             buffer = new_buffer;
        //             page = pg_sys::BufferGetPage(buffer);
        //         } else {
        //             let next_blockno = (*special).next_blockno;
        //             pg_sys::ReleaseBuffer(buffer);
        //             buffer = cache.get_buffer(next_blockno, None);
        //             page = pg_sys::BufferGetPage(buffer);
        //         }
        //     }

        // }

        pg_sys::MarkBufferDirty(buffer);
        pg_sys::ReleaseBuffer(buffer);
    }
}
