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
    indexer::{AddOperation, SegmentWriter},
    IndexSettings,
};
use tantivy::{schema::Field, Directory, Index};
use thiserror::Error;

use crate::index::atomic::AtomicDirectory;
use crate::index::directory::blocking::{BlockingDirectory, META_FILEPATH};
use crate::index::directory::writer::{SearchDirectoryError, SearchFs, WriterDirectory};
use crate::index::WriterResources;
use crate::postgres::storage::buffer::BufferCache;
use crate::postgres::storage::segment_handle;

/// A global store of which indexes have been created during a transaction,
/// so that they can be committed or rolled back in case of an abort.
static mut PENDING_INDEX_CREATES: Lazy<HashSet<WriterDirectory>> = Lazy::new(HashSet::new);

/// A global store of which indexes have been dropped during a transaction,
/// so that they can be committed or rolled back in case of an abort.
static mut PENDING_INDEX_DROPS: Lazy<HashSet<WriterDirectory>> = Lazy::new(HashSet::new);

/// The entity that interfaces with Tantivy indexes.
pub struct SearchIndexWriter {
    pub underlying_writer: SegmentWriter,
    pub current_opstamp: tantivy::Opstamp,
    pub segment: tantivy::Segment,
}

impl SearchIndexWriter {
    pub fn new(index: Index, resources: WriterResources) -> Result<Self> {
        let (_, memory_budget) = resources.resources();
        let segment = index.new_segment();
        let current_opstamp = index.load_metas()?.opstamp;
        let underlying_writer = SegmentWriter::for_segment(memory_budget, segment.clone())?;

        Ok(Self {
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
        let index = segment.index();
        let committed_meta = index.load_metas()?;
        let mut segments = committed_meta.segments.clone();
        segments.push(segment.meta().clone());

        let new_meta = tantivy::IndexMeta {
            segments,
            opstamp: self.current_opstamp,
            index_settings: committed_meta.index_settings,
            schema: committed_meta.schema,
            payload: committed_meta.payload,
        };

        index
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
