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

use crate::index::channel::directory::{
    ChannelDirectory, ChannelRequest, ChannelRequestHandler, ChannelResponse,
};
use crate::index::reader::FFType;
use crate::index::writer::BlockingDirectory;
use crate::index::WriterResources;
use crate::postgres::index::open_search_index;
use crate::postgres::storage::segment_handle::SegmentHandle;
use crate::postgres::storage::segment_reader::SegmentReader;
use crate::postgres::storage::segment_writer::SegmentWriter;
use pgrx::{pg_sys::ItemPointerData, *};
use std::io::Write;
use tantivy::directory::FileHandle;
use tantivy::index::Index;
use tantivy::indexer::IndexWriter;
use tantivy::Directory;

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    pgrx::info!("ambulkdelete");
    let info = unsafe { PgBox::from_pg(info) };
    let mut stats = unsafe { PgBox::from_pg(stats) };
    let index_relation = unsafe { PgRelation::from_pg(info.index) };
    let index_oid: u32 = index_relation.oid().into();
    let search_index =
        open_search_index(&index_relation).expect("should be able to open search index");
    let request_channel = crossbeam::channel::unbounded::<ChannelRequest>();
    let response_channel = crossbeam::channel::unbounded::<ChannelResponse>();
    let request_channel_clone = request_channel.clone();
    let response_channel_clone = response_channel.clone();

    std::thread::spawn(move || {
        let (request_sender, _) = request_channel;
        let (_, response_receiver) = response_channel;
        let channel_directory =
            ChannelDirectory::new(request_channel_clone, response_channel_clone, index_oid);
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

        for segment_reader in reader.searcher().segment_readers() {
            let fast_fields = segment_reader.fast_fields();
            let ctid_ff = FFType::new(fast_fields, "ctid");
            if let FFType::U64(ff) = ctid_ff {
                let ctids: Vec<u64> = ff.iter().collect();
                request_sender
                    .send(ChannelRequest::ShouldDeleteCtids(ctids))
                    .unwrap();
                let ctids_to_delete = match response_receiver.recv().unwrap() {
                    ChannelResponse::ShouldDeleteCtids(ctids) => ctids,
                    _ => panic!("unexpected response in bulkdelete thread"),
                };
                for ctid in ctids_to_delete {
                    let ctid_field = channel_index.schema().get_field("ctid").unwrap();
                    let ctid_term = tantivy::Term::from_field_u64(ctid_field, ctid);
                    writer.delete_term(ctid_term);
                }
            }
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        request_sender.send(ChannelRequest::Terminate).unwrap();
    });

    let blocking_directory = BlockingDirectory::new(index_oid);
    let handler = ChannelRequestHandler::open(
        blocking_directory,
        index_oid,
        response_channel.0,
        request_channel.1,
    );
    let should_delete = callback.map(|actual_callback| {
        move |ctid_val: u64| unsafe {
            let mut ctid = ItemPointerData::default();
            crate::postgres::utils::u64_to_item_pointer(ctid_val, &mut ctid);
            actual_callback(&mut ctid, callback_state)
        }
    });

    handler.receive_blocking(should_delete).unwrap();

    if stats.is_null() {
        stats = unsafe {
            PgBox::from_pg(
                pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBulkDeleteResult>()).cast(),
            )
        };
    }

    // TODO: Populate stats
    stats.into_pg()
}
