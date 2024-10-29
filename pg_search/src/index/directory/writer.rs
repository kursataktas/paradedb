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

use derive_more::AsRef;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// The top-level folder name for ParadeDB extension inside the Postgres data directory.
#[derive(AsRef)]
#[as_ref(forward)]
pub struct SearchIndexDirPath(pub PathBuf);
/// The name of the index-specific configuration file, enabling loading an index across connections.
#[derive(AsRef)]
#[as_ref(forward)]
pub struct SearchIndexConfigFilePath(pub PathBuf);
/// The name of the directory where the Tantivy index will be created.
#[derive(AsRef)]
#[as_ref(forward)]
pub struct TantivyDirPath(pub PathBuf);
/// The name of the directory where pipe files will be created for transfer to the writer process.
#[derive(AsRef)]
#[as_ref(forward)]
pub struct WriterTransferPipeFilePath(pub PathBuf);

#[allow(unused)]
/// The file location for a pg_search index is:
/// $data_directory/pg_search/$database_oid/$index_oid/$relfilenode
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchIndexEntity {
    pub database_oid: u32,
    pub index_oid: u32,
    pub relfilenode: u32,
}

impl SearchIndexEntity {
    /// Useful in a connection process, where the database oid is available in the environment.
    pub fn from_oids(database_oid: u32, index_oid: u32, relfilenode: u32) -> Self {
        Self {
            database_oid,
            index_oid,
            relfilenode,
        }
    }
}
