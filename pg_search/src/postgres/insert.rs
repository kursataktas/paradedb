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

use crate::index::SearchIndexWriter;
use crate::index::{SearchIndex, WriterResources};
use crate::postgres::index::open_search_index;
use crate::postgres::options::SearchIndexCreateOptions;
use crate::postgres::utils::row_to_search_document;
use anyhow::Result;
use pgrx::{pg_guard, pg_sys, pgrx_extern_c_guard, PgMemoryContexts, PgRelation, PgTupleDesc};
use std::ffi::CStr;
use std::panic::{catch_unwind, resume_unwind};

pub struct InsertState {
    pub index: SearchIndex,
    pub writer: Option<SearchIndexWriter>,
    pub relation: pg_sys::Relation,
    abort_on_drop: bool,
}

impl InsertState {
    pub fn try_commit(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.commit()?;
        }
        Ok(())
    }
}

impl InsertState {
    unsafe fn new(
        indexrel: &PgRelation,
        writer_resources: WriterResources,
    ) -> anyhow::Result<Self> {
        let index = open_search_index(indexrel)?;
        let options = indexrel.rd_options as *mut SearchIndexCreateOptions;
        let writer = index.get_writer(writer_resources, options.as_ref().unwrap())?;
        Ok(Self {
            index,
            writer: Some(writer),
            abort_on_drop: false,
            relation: index_relation,
        })
    }
}

pub unsafe fn init_insert_state(
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    writer_resources: WriterResources,
) -> *mut InsertState {
    assert!(!index_info.is_null());
    let state = (*index_info).ii_AmCache;
    if state.is_null() {
        // we don't have any cached state yet, so create it now
        let state = InsertState::new(index_relation, writer_resources)
            .expect("should be able to open new SearchIndex for writing");

        // leak it into the MemoryContext for this scan (as specified by the IndexInfo argument)
        //
        // When that memory context is freed by Postgres is when we'll do our tantivy commit/abort
        // of the changes made during `aminsert`
        //
        // SAFETY: `leak_and_drop_on_delete` palloc's memory in CurrentMemoryContext, but in this
        // case we want the thing it allocates to be palloc'd in the `ii_Context`
        PgMemoryContexts::For((*index_info).ii_Context)
            .switch_to(|mcxt| (*index_info).ii_AmCache = mcxt.leak_and_drop_on_delete(state).cast())
    };

    (*index_info).ii_AmCache.cast()
}

#[allow(clippy::too_many_arguments)]
#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, isnull, heap_tid, index_info)
}

#[cfg(feature = "pg13")]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, isnull, heap_tid, index_info)
}

#[inline(always)]
unsafe fn aminsert_internal(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    ctid: pg_sys::ItemPointer,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    let result = catch_unwind(|| {
        let state = &mut *init_insert_state(index_relation, index_info, WriterResources::Statement);
        let tupdesc = PgTupleDesc::from_pg_unchecked((*index_relation).rd_att);
        let search_index = &state.index;
        let writer = state.writer.as_mut().expect("writer should not be null");
        let search_document =
            row_to_search_document(*ctid, &tupdesc, values, isnull, &search_index.schema)
                .unwrap_or_else(|err| {
                    panic!(
                        "error creating index entries for index '{}': {err}",
                        CStr::from_ptr((*(*index_relation).rd_rel).relname.data.as_ptr())
                            .to_string_lossy()
                    );
                });
        search_index
            .insert(writer, search_document)
            .expect("insertion into index should succeed");
        state.try_commit().expect("commit should succeed");
        true
    });

    match result {
        Ok(result) => result,
        Err(e) => {
            // bubble up the panic that we caught during `catch_unwind()`
            resume_unwind(e)
        }
    }
}
