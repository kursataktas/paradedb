// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use anyhow::Result;
use once_cell::sync::Lazy;
use pgrx::pg_sys;
use std::path::PathBuf;
use std::sync::Arc;
use std::path::Path;
use std::{io, result};
use tantivy::directory::{DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr};
use tantivy::{
    directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    directory::{MANAGED_LOCK, META_LOCK},
};
use tantivy::Directory;

use crate::index::atomic::AtomicDirectory;
use crate::index::reader::file_handle::FileHandleReader;
use crate::index::segment_handle::SegmentHandle;
use crate::index::writer::io::IoWriter;
use crate::postgres::buffer::BufferCache;

/// Defined by Tantivy in core/mod.rs
pub static META_FILEPATH: Lazy<&'static Path> = Lazy::new(|| Path::new("meta.json"));
pub static MANAGED_FILEPATH: Lazy<&'static Path> = Lazy::new(|| Path::new(".managed.json"));

/// We maintain our own tantivy::directory::Directory implementation for finer-grained
/// control over the locking behavior, which enables us to manage Writer instances
/// across multiple connections.
#[derive(Clone, Debug)]
pub struct BlockingDirectory {
    relation_oid: u32,
}

pub struct BlockingLock {
    buffer: pg_sys::Buffer,
}

impl BlockingLock {
    pub unsafe fn new(relation_oid: u32, blockno: pg_sys::BlockNumber) -> Self {
        let cache = BufferCache::open(relation_oid);
        let buffer = cache.get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE));

        Self { buffer }
    }
}

impl Drop for BlockingLock {
    fn drop(&mut self) {
        pgrx::info!("BlockingLock drop");
        unsafe { pg_sys::UnlockReleaseBuffer(self.buffer) };
    }
}

impl BlockingDirectory {
    pub fn new(relation_oid: u32) -> Self {
        Self { relation_oid }
    }

    /// ambulkdelete wants to know how many pages were deleted, but the Directory trait doesn't let delete
    /// return a value, so we provide our own
    pub fn delete_with_stats(&self, path: &Path) -> result::Result<u32, DeleteError> {
        unsafe {
            let mut pages_deleted = 0;
            let segment_handle = SegmentHandle::open(self.relation_oid, path).unwrap();
            if let Some(segment_handle) = segment_handle {
                let cache = BufferCache::open(self.relation_oid);
                let blocknos = segment_handle.internal().blocks();
                for blockno in blocknos {
                    let buffer = cache.get_buffer(blockno, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE));
                    let page = pg_sys::BufferGetPage(buffer);

                    let max_offset = pg_sys::PageGetMaxOffsetNumber(page);
                    if max_offset > pg_sys::InvalidOffsetNumber {
                        for offsetno in pg_sys::FirstOffsetNumber..=max_offset {
                            pg_sys::PageIndexTupleDelete(page, pg_sys::FirstOffsetNumber);
                        }
                    }

                    cache.record_free_index_page(blockno);
                    pg_sys::MarkBufferDirty(buffer);
                    pg_sys::UnlockReleaseBuffer(buffer);

                    pages_deleted += 1;
                }
            }

            Ok(pages_deleted)
        }
    }
}

impl Directory for BlockingDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let handle = unsafe {
            SegmentHandle::open(self.relation_oid, path)
                .unwrap()
                .unwrap()
        };

        Ok(Arc::new(FileHandleReader::new(
            self.relation_oid,
            path,
            handle,
        )))
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        pgrx::info!("open_write: {:?}", path);
        Ok(io::BufWriter::new(Box::new(unsafe {
            IoWriter::new(self.relation_oid, path)
        })))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        pgrx::info!("atomic_write: {:?}", path);
        let directory = unsafe { AtomicDirectory::new(self.relation_oid) };
        if path.to_path_buf() == *META_FILEPATH {
            unsafe { directory.write_meta(data) };
        } else if path.to_path_buf() == *MANAGED_FILEPATH {
            unsafe { directory.write_managed(data) };
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("atomic_write unexpected path: {:?}", path),
            ));
        };

        Ok(())
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        pgrx::info!("atomic_read: {:?}", path);
        let directory = unsafe { AtomicDirectory::new(self.relation_oid) };
        let data = if path.to_path_buf() == *META_FILEPATH {
            unsafe { directory.read_meta() }
        } else if path.to_path_buf() == *MANAGED_FILEPATH {
            unsafe { directory.read_managed() }
        } else {
            return Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)));
        };

        if data.is_empty() {
            return Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)));
        }

        Ok(data)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        let _ = self.delete_with_stats(path)?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        todo!("directory exists");
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        unsafe {
            let directory = unsafe { AtomicDirectory::new(self.relation_oid) };
            let mut blockno = None;

            if lock.filepath == META_LOCK.filepath {
                blockno = Some(directory.meta_blockno);
            } else if lock.filepath == MANAGED_LOCK.filepath {
                blockno = Some(directory.managed_blockno);
            }

            if let Some(blockno) = blockno {
                pgrx::info!("acquire_lock: {:?}", lock);
                Ok(DirectoryLock::from(Box::new(BlockingLock::new(
                    self.relation_oid,
                    blockno,
                ))))
            } else {
                Err(LockError::wrap_io_error(io::Error::new(
                    io::ErrorKind::Other,
                    format!("acquire_lock unexpected lock {:?}", lock),
                )))
            }
        }
    }

    // Internally, tantivy only uses this API to detect new commits to implement the
    // `OnCommitWithDelay` `ReloadPolicy`. Not implementing watch in a `Directory` only prevents
    // the `OnCommitWithDelay` `ReloadPolicy` to work properly.
    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        unimplemented!("OnCommitWithDelay ReloadPolicy not supported");
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}
