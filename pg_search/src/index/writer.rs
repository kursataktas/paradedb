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

use crate::{
    index::SearchIndex,
    postgres::types::TantivyValueError,
    schema::{
        SearchDocument, SearchFieldConfig, SearchFieldName, SearchFieldType, SearchIndexSchema,
    },
};
use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use pgrx::pg_sys;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::HashSet, path::Path};
use std::{io, result};
use tantivy::directory::{DirectoryLock, FileHandle, Lock, WatchCallback, WatchHandle, WritePtr};
use tantivy::{
    directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    directory::{MANAGED_LOCK, META_LOCK},
    indexer::{self, AddOperation},
    IndexSettings,
};
use tantivy::{schema::Field, Directory, Index};
use thiserror::Error;

use super::directory::{SearchDirectoryError, SearchFs, WriterDirectory};
use crate::index::WriterResources;
use crate::postgres::storage::atomic_directory::AtomicDirectory;
use crate::postgres::storage::buffer::BufferCache;
use crate::postgres::storage::segment_handle;
use crate::postgres::storage::segment_reader;
use crate::postgres::storage::segment_writer;

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
    blockno: Option<u32>,
    relation_oid: u32,
}

impl Drop for BlockingLock {
    fn drop(&mut self) {
        if let Some(blockno) = self.blockno {
            unsafe {
                let cache = BufferCache::open(self.relation_oid);
                let buffer = cache.get_buffer(blockno, None);
                pg_sys::UnlockReleaseBuffer(buffer);
            }
        }
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
            let segment_handle =
                segment_handle::SegmentHandle::open(self.relation_oid, &path).unwrap();
            if let Some(segment_handle) = segment_handle {
                let cache = BufferCache::open(self.relation_oid);
                let blocknos = segment_handle.internal().blocks();
                for blockno in blocknos {
                    let buffer = cache.get_buffer(blockno, None);
                    let page = pg_sys::BufferGetPage(buffer);

                    let max_offset = pg_sys::PageGetMaxOffsetNumber(page);
                    if max_offset > pg_sys::InvalidOffsetNumber {
                        for offsetno in pg_sys::FirstOffsetNumber..=max_offset {
                            pg_sys::PageIndexTupleDelete(page, pg_sys::FirstOffsetNumber);
                        }
                    }

                    cache.record_free_index_page(blockno);
                    pg_sys::MarkBufferDirty(buffer);
                    pg_sys::ReleaseBuffer(buffer);

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
            segment_handle::SegmentHandle::open(self.relation_oid, path)
                .unwrap()
                .unwrap()
        };

        Ok(Arc::new(segment_reader::SegmentReader::new(
            self.relation_oid,
            path,
            handle,
        )))
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        Ok(io::BufWriter::new(Box::new(unsafe {
            segment_writer::SegmentWriter::new(self.relation_oid, path)
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

            if lock.filepath == (*META_LOCK).filepath {
                let buffer = directory
                    .cache
                    .get_buffer(directory.meta_blockno, Some(pg_sys::BUFFER_LOCK_EXCLUSIVE));
                blockno = Some(directory.meta_blockno);
                pg_sys::ReleaseBuffer(buffer);
            } else if lock.filepath == (*MANAGED_LOCK).filepath {
                let buffer = directory.cache.get_buffer(
                    directory.managed_blockno,
                    Some(pg_sys::BUFFER_LOCK_EXCLUSIVE),
                );
                blockno = Some(directory.managed_blockno);
                pg_sys::ReleaseBuffer(buffer);
            }

            Ok(DirectoryLock::from(Box::new(BlockingLock {
                blockno,
                relation_oid: self.relation_oid,
            })))
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

/// A global store of which indexes have been created during a transaction,
/// so that they can be committed or rolled back in case of an abort.
static mut PENDING_INDEX_CREATES: Lazy<HashSet<WriterDirectory>> = Lazy::new(HashSet::new);

/// A global store of which indexes have been dropped during a transaction,
/// so that they can be committed or rolled back in case of an abort.
static mut PENDING_INDEX_DROPS: Lazy<HashSet<WriterDirectory>> = Lazy::new(HashSet::new);

/// The entity that interfaces with Tantivy indexes.
pub struct SearchIndexWriter {
    pub underlying_index: Index,
    pub underlying_writer: indexer::SegmentWriter,
    pub current_opstamp: tantivy::Opstamp,
    pub segment: tantivy::Segment,
}

impl SearchIndexWriter {
    pub fn new(index: Index, resources: WriterResources) -> Result<Self> {
        let (_, memory_budget) = resources.resources();
        let segment = index.new_segment();
        let current_opstamp = index.load_metas()?.opstamp;
        let underlying_writer =
            indexer::SegmentWriter::for_segment(memory_budget, segment.clone())?;

        Ok(Self {
            underlying_index: index,
            underlying_writer,
            current_opstamp,
            segment,
        })
    }

    pub fn insert(&mut self, document: SearchDocument) -> Result<(), IndexError> {
        // Add the Tantivy document to the index.
        let tantivy_document: tantivy::TantivyDocument = document.into();
        self.current_opstamp += 1;
        self.underlying_writer.add_document(AddOperation {
            opstamp: self.current_opstamp,
            document: tantivy_document,
        })?;

        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        self.current_opstamp += 1;
        let max_doc = self.underlying_writer.max_doc();
        self.underlying_writer.finalize()?;
        let segment = self.segment.with_max_doc(max_doc);
        let committed_meta = segment.index().load_metas()?;
        let mut segments = committed_meta.segments.clone();
        segments.push(segment.meta().clone());

        let new_meta = tantivy::IndexMeta {
            segments,
            opstamp: self.current_opstamp,
            index_settings: committed_meta.index_settings,
            schema: committed_meta.schema,
            payload: committed_meta.payload,
        };

        self.underlying_index
            .directory()
            .atomic_write(*META_FILEPATH, &serde_json::to_vec(&new_meta)?)?;

        Ok(())
    }

    pub fn abort(self) -> Result<()> {
        // TODO: Implement rollback
        Ok(())
    }

    pub fn create_index(
        directory: WriterDirectory,
        fields: Vec<(SearchFieldName, SearchFieldConfig, SearchFieldType)>,
        key_field_index: usize,
    ) -> Result<()> {
        let schema = SearchIndexSchema::new(fields, key_field_index)?;

        let tantivy_dir_path = directory.tantivy_dir_path(true)?;
        let tantivy_dir = BlockingDirectory::new(directory.index_oid);
        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..IndexSettings::default()
        };
        let mut underlying_index = Index::create(tantivy_dir, schema.schema.clone(), settings)?;

        SearchIndex::setup_tokenizers(&mut underlying_index, &schema);

        let new_self = SearchIndex {
            underlying_index,
            directory: directory.clone(),
            schema,
        };

        // Serialize SearchIndex to disk so it can be initialized by other connections.
        new_self.directory.save_index(&new_self)?;

        // Mark in our global store that this index is pending create, in case it
        // needs to be rolled back on abort.
        Self::mark_pending_create(&directory);

        Ok(())
    }

    pub fn mark_pending_create(directory: &WriterDirectory) -> bool {
        unsafe { PENDING_INDEX_CREATES.insert(directory.clone()) }
    }

    pub fn mark_pending_drop(directory: &WriterDirectory) -> bool {
        unsafe { PENDING_INDEX_DROPS.insert(directory.clone()) }
    }

    pub fn clear_pending_creates() {
        unsafe { PENDING_INDEX_CREATES.clear() }
    }

    pub fn clear_pending_drops() {
        unsafe { PENDING_INDEX_DROPS.clear() }
    }

    pub fn pending_creates() -> impl Iterator<Item = &'static WriterDirectory> {
        unsafe { PENDING_INDEX_CREATES.iter() }
    }

    pub fn pending_drops() -> impl Iterator<Item = &'static WriterDirectory> {
        unsafe { PENDING_INDEX_DROPS.iter() }
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    TantivyError(#[from] tantivy::TantivyError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    #[error(transparent)]
    TantivyValueError(#[from] TantivyValueError),

    #[error("couldn't remove index files on drop_index: {0}")]
    DeleteDirectory(#[from] SearchDirectoryError),

    #[error("key_field column '{0}' cannot be NULL")]
    KeyIdNull(String),
}
