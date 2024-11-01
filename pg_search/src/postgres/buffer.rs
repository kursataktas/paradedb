use pgrx::*;
use std::ptr::null_mut;

pub const METADATA_BLOCKNO: pg_sys::BlockNumber = 0; // Stores metadata for the entire index
pub const SEGMENT_HANDLE_BLOCKNO: pg_sys::BlockNumber = 1; // Stores SegmentHandles
pub const INDEX_WRITER_LOCK_BLOCKNO: pg_sys::BlockNumber = 2; // Used for Tantivy's INDEX_WRITER_LOCK
pub const TANTIVY_META_BLOCKNO: pg_sys::BlockNumber = 3; // Used for Tantivy's meta.json
pub const TANTIVY_MANAGED_BLOCKNO: pg_sys::BlockNumber = 4; // Used for Tantivy's managed.json

pub struct MetaPageData {
    pub segment_handle_insert_blockno: pg_sys::BlockNumber,
}

pub struct LinkedBlockSpecialData {
    pub next_blockno: pg_sys::BlockNumber,
}

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
        let mut unlock_relation = false;
        let mut blockno = pg_sys::GetFreeIndexPage(self.boxed.as_ptr());

        if blockno == pg_sys::InvalidBlockNumber {
            pg_sys::LockRelationForExtension(self.boxed.as_ptr(), pg_sys::ExclusiveLock as i32);
            unlock_relation = true;
        }

        let mut buffer = self.get_buffer(blockno, None);

        // If we can't acquire a conditional lock on the buffer, it's being used by another process
        // and we need to look for a new one
        while blockno != pg_sys::InvalidBlockNumber && !pg_sys::ConditionalLockBuffer(buffer) {
            pg_sys::ReleaseBuffer(buffer);
            blockno = pg_sys::GetFreeIndexPage(self.boxed.as_ptr());

            if blockno == pg_sys::InvalidBlockNumber {
                pg_sys::LockRelationForExtension(self.boxed.as_ptr(), pg_sys::ExclusiveLock as i32);
                unlock_relation = true;
            }

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

        if unlock_relation {
            pg_sys::UnlockRelationForExtension(self.boxed.as_ptr(), pg_sys::ExclusiveLock as i32);
        }
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
