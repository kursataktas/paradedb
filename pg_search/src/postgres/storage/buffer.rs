use pgrx::*;
use std::ptr::null_mut;

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
        let blockno = pg_sys::GetFreeIndexPage(self.boxed.as_ptr());
        let buffer = self.get_buffer(
            pg_sys::InvalidBlockNumber,
            Some(pg_sys::BUFFER_LOCK_EXCLUSIVE),
        );
        pg_sys::PageInit(
            pg_sys::BufferGetPage(buffer),
            pg_sys::BufferGetPageSize(buffer),
            special_size,
        );
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
        pgrx::info!("recording free buffer: {}", blockno);
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
