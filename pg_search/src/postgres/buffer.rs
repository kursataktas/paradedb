use pgrx::*;
use std::ptr::null_mut;

/// The first block of the index is the metadata block, which is essentially a map for how the rest of the blocks are organized.
/// It is our responsibility to ensure that the metadata block is the first block by creating it immediately when the index is built.
pub const SEGMENT_HANDLE_BLOCKNO: pg_sys::BlockNumber = 0;
/// The second block is used for Tantivy's INDEX_WRITER_LOCK
pub const INDEX_WRITER_LOCK_BLOCKNO: pg_sys::BlockNumber = 1;
/// The third block is used for Tantivy's meta.json
pub const META_BLOCKNO: pg_sys::BlockNumber = 2;
/// The fourth block is used for Tantivy's managed.json
pub const MANAGED_BLOCKNO: pg_sys::BlockNumber = 3;

// Reads and writes buffers from the buffer cache for a pg_sys::Relation
#[derive(Clone, Debug)]
pub struct BufferCache {
    boxed: PgBox<pg_sys::RelationData>,
}

impl BufferCache {
    pub unsafe fn open(relation_oid: u32) -> Self {
        let relation = pg_sys::RelationIdGetRelation(relation_oid.into());
        Self {
            boxed: PgBox::from_pg(relation),
        }
    }

    pub unsafe fn new_buffer(&self, special_size: usize) -> pg_sys::Buffer {
        // Providing an InvalidBlockNumber creates a new page
        let mut blockno = pg_sys::GetFreeIndexPage(self.boxed.as_ptr());
        let mut buffer = self.get_buffer(blockno, None);

        // If we can't acquire a conditional lock on the buffer, it's being used by another process
        // and we need to look for a new one
        while blockno != pg_sys::InvalidBlockNumber && !pg_sys::ConditionalLockBuffer(buffer) {
            pg_sys::ReleaseBuffer(buffer);
            blockno = pg_sys::GetFreeIndexPage(self.boxed.as_ptr());
            buffer = self.get_buffer(blockno, None);
        }

        if blockno == pg_sys::InvalidBlockNumber {
            pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);
        }

        let page = pg_sys::BufferGetPage(buffer);
        if pg_sys::PageIsNew(page) {
            pg_sys::PageInit(page, pg_sys::BufferGetPageSize(buffer), special_size);
        }

        pg_sys::MarkBufferDirty(buffer);
        // Returns the BlockNumber of the newly-created page
        buffer
    }

    pub unsafe fn get_buffer(
        &self,
        blockno: pg_sys::BlockNumber,
        lock: Option<u32>,
    ) -> pg_sys::Buffer {
        let buffer = pg_sys::ReadBufferExtended(
            self.boxed.as_ptr(),
            pg_sys::ForkNumber::MAIN_FORKNUM,
            blockno,
            pg_sys::ReadBufferMode::RBM_NORMAL,
            null_mut(),
        );
        if let Some(lock) = lock {
            pg_sys::LockBuffer(buffer, lock as i32);
        }
        buffer
    }

    pub unsafe fn record_free_index_page(&self, blockno: pg_sys::BlockNumber) {
        pgrx::info!("Recording free index page for blockno {}", blockno);
        pg_sys::RecordFreeIndexPage(self.boxed.as_ptr(), blockno);
    }
}

impl Drop for BufferCache {
    fn drop(&mut self) {
        unsafe {
            pg_sys::RelationClose(self.boxed.as_ptr());
        }
    }
}
