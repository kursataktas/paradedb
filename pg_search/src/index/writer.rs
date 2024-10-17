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
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::{collections::HashSet, path::Path};
use std::{fs, io, result};
use tantivy::directory::{
    DirectoryClone, DirectoryLock, FileHandle, FileSlice, Lock, WatchCallback, WatchHandle,
    WritePtr,
};
use tantivy::{
    directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    IndexSettings,
};
use tantivy::{directory::MmapDirectory, schema::Field, Directory, Index, IndexWriter};
use thiserror::Error;

use crate::postgres::utils::{bm25_write_managed, bm25_write_meta, read_managed, read_meta};

use super::directory::{SearchDirectoryError, SearchFs, WriterDirectory};

/// We maintain our own tantivy::directory::Directory implementation for finer-grained
/// control over the locking behavior, which enables us to manage Writer instances
/// across multiple connections.
#[derive(Debug)]
pub struct BlockingDirectory(pub u32);

impl DirectoryClone for BlockingDirectory {
    fn box_clone(&self) -> Box<dyn Directory> {
        pgrx::info!("box_clone");
        Box::new(BlockingDirectory(self.0))
    }
}

impl Directory for BlockingDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        todo!("get_file_handle");
        // self.0.get_file_handle(path)
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        todo!("open_read");
        // self.0.open_read(path)
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        todo!("open_write");
        // self.0.open_write(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        if path.ends_with("meta.json") {
            pgrx::info!("writing {} bytes", data.len());
            unsafe { bm25_write_meta(self.0, data) };
        } else if path.ends_with(".managed.json") {
            pgrx::info!("writing {} bytes", data.len());
            unsafe { bm25_write_managed(self.0, data) };
        } else {
            todo!("write {:?}", path);
        }
        Ok(())
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        let data = match path.ends_with(".meta.json") {
            true => unsafe { read_meta(self.0) },
            false => unsafe { read_managed(self.0) },
        };

        if data.is_empty() {
            return Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)));
        }

        pgrx::info!("read {} bytes", data.len());

        let file_slice = FileSlice::from(data);
        let bytes = file_slice
            .read_bytes()
            .map_err(|io_error| OpenReadError::IoError {
                io_error: Arc::new(io_error),
                filepath: path.to_path_buf(),
            })?
            .as_slice()
            .to_owned();

        pgrx::info!("got file slice {:?}", String::from_utf8_lossy(&bytes));
        Ok(bytes)
    }

    fn delete(&self, path: &Path) -> result::Result<(), DeleteError> {
        todo!("delete");
        // pgrx::info!("delete: {:?}", path);
        // self.0.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        todo!("exists");
        // pgrx::info!("exists: {:?}", path);
        // self.0.exists(path)
    }

    fn acquire_lock(&self, lock: &Lock) -> result::Result<DirectoryLock, LockError> {
        todo!("acquire_lock");
        // pgrx::info!("acquire_lock: {:?}", lock.filepath);
        // // This is the only change we actually need to make to the Directory trait impl.
        // // We want the acquire_lock behavior to block and wait for a lock to be available,
        // // instead of panicking. Internally, Tantivy just polls for its availability.
        // let blocking_lock = Lock {
        //     filepath: lock.filepath.clone(),
        //     is_blocking: true,
        // };
        // self.0.acquire_lock(&blocking_lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        todo!("watch");
        // pgrx::info!("watch");
        // self.0.watch(watch_callback)
    }

    fn sync_directory(&self) -> io::Result<()> {
        pgrx::info!("sync_directory");
        Ok(())
        // pgrx::info!("sync_directory");
        // self.0.sync_directory()
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
    // this is an Option<> because on drop we need to take ownership of the underlying
    // IndexWriter instance so we can, in the background, wait for all merging threads to finish
    pub underlying_writer: Option<IndexWriter>,
    pub wants_merge: bool,
}

impl Drop for SearchIndexWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.underlying_writer.take() {
            // wait for all merging threads to finish.  we do this in the background
            // because we don't want to block the connection that created this SearchIndexWriter
            // from being able to do more work.
            if self.wants_merge {
                std::thread::spawn(move || {
                    if let Err(e) = writer.wait_merging_threads() {
                        pgrx::warning!("`wait_merging_threads` failed: {e}");
                    }
                });
            }
        }
    }
}

impl SearchIndexWriter {
    pub fn insert(&self, document: SearchDocument) -> Result<(), IndexError> {
        // Add the Tantivy document to the index.
        self.underlying_writer
            .as_ref()
            .unwrap()
            .add_document(document.into())?;

        Ok(())
    }

    pub fn delete(&self, ctid_field: &Field, ctid_values: &[u64]) -> Result<(), IndexError> {
        for ctid in ctid_values {
            let ctid_term = tantivy::Term::from_field_u64(*ctid_field, *ctid);
            self.underlying_writer
                .as_ref()
                .unwrap()
                .delete_term(ctid_term);
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.underlying_writer
            .as_mut()
            .unwrap()
            .commit()
            .context("error committing to tantivy index")?;

        Ok(())
    }

    pub fn abort(&mut self) -> Result<(), IndexError> {
        self.underlying_writer.as_mut().unwrap().rollback()?;
        Ok(())
    }

    pub fn vacuum(&self) -> Result<(), IndexError> {
        self.underlying_writer
            .as_ref()
            .unwrap()
            .garbage_collect_files()
            .wait()?;
        Ok(())
    }

    pub fn create_index(
        directory: WriterDirectory,
        fields: Vec<(SearchFieldName, SearchFieldConfig, SearchFieldType)>,
        key_field_index: usize,
    ) -> Result<()> {
        let schema = SearchIndexSchema::new(fields, key_field_index)?;

        let tantivy_dir_path = directory.tantivy_dir_path(true)?;
        let tantivy_dir = BlockingDirectory(directory.index_oid);
        let mut underlying_index =
            Index::create(tantivy_dir, schema.schema.clone(), IndexSettings::default())?;

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
