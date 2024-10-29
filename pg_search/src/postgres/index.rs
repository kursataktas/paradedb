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

use crate::index::directory::blocking::BlockingDirectory;
use crate::index::{SearchIndex, SearchIndexError};
use crate::postgres::build::get_fields;
use crate::schema::SearchIndexSchema;
use pgrx::{pg_sys, PgRelation};
use tantivy::Index;

/// Open the underlying [`SearchIndex`] for the specified Postgres index relation
pub fn open_search_index(
    index_relation: &PgRelation,
) -> anyhow::Result<SearchIndex, SearchIndexError> {
    let database_oid = unsafe { pg_sys::MyDatabaseId };
    let index_oid = index_relation.oid();
    let (fields, key_field_index) = unsafe { get_fields(index_relation) };
    let schema = SearchIndexSchema::new(fields, key_field_index)?;
    let tantivy_dir = BlockingDirectory::new(index_oid.into());
    let underlying_index = Index::open(tantivy_dir)?;

    Ok(SearchIndex {
        schema,
        underlying_index,
        index_oid,
    })
}
