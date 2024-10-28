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

use crate::index::WriterResources;
use crate::postgres::index::open_search_index;
use pgrx::*;

#[pg_guard]
pub extern "C" fn amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let info = unsafe { PgBox::from_pg(info) };
    if stats.is_null() || info.analyze_only {
        pgrx::info!("no stats");
        return stats;
    }

    let pages_deleted = unsafe { (*stats).pages_deleted };
    // If pages were deleted, then ambulkdelete already merged segments, no need to merge again
    if pages_deleted > 0 {
        pgrx::info!("pages were deleted, no need to merge segments");
        unsafe { pg_sys::IndexFreeSpaceMapVacuum(info.index) };
        return stats;
    }

    // TODO: Implement merging segments
    // Do we need to garbage collect?

    stats
}
