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
use crate::postgres::options::SearchIndexCreateOptions;
use pgrx::*;

#[pg_guard]
pub extern "C" fn amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let info = unsafe { PgBox::from_pg(info) };
    if info.analyze_only {
        return stats;
    }

    let needs_merge = stats.is_null() || unsafe { (*stats).pages_deleted == 0 };
    let index_relation = unsafe { PgRelation::from_pg(info.index) };
    let index_oid: u32 = index_relation.oid().into();
    let (request_sender, request_receiver) = crossbeam::channel::unbounded::<ChannelRequest>();
    let (response_sender, response_receiver) = crossbeam::channel::unbounded::<ChannelResponse>();

    std::thread::spawn(move || {
        let channel_directory =
            ChannelDirectory::new(request_sender.clone(), response_receiver.clone());
        let channel_index = Index::open(channel_directory).expect("channel index should open");
        let reader = channel_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .unwrap();
        let (parallelism, memory_budget) = WriterResources::Vacuum.resources();
        let mut writer: IndexWriter = channel_index
            .writer_with_num_threads(parallelism.into(), memory_budget)
            .unwrap();

        if needs_merge {
            let merge_policy = writer.get_merge_policy();
            let segments = channel_index.load_metas().unwrap().segments;
            let candidates = merge_policy.compute_merge_candidates(segments.as_slice());
            // TODO: Parallelize this?
            for candidate in candidates {
                writer.merge(&candidate.0).wait().unwrap();
            }
        }

        writer.garbage_collect_files().wait().unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        request_sender.send(ChannelRequest::Terminate).unwrap();
    });

    let blocking_directory = BlockingDirectory::new(index_oid);
    let handler = ChannelRequestHandler::open(
        blocking_directory,
        index_oid,
        response_sender,
        request_receiver,
    );
    let _ = handler.receive_blocking(Some(|_| false)).unwrap();
    unsafe { pg_sys::IndexFreeSpaceMapVacuum(info.index) };
    stats
}
