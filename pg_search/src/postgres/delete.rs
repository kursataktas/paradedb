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
use crate::index::channel::directory::{ChannelDirectory, ChannelRequest, ChannelResponse};
use crate::index::writer::BlockingDirectory;
use crate::index::SearchIndex;
use crate::postgres::index::open_search_index;
use crate::postgres::storage::segment_handle::SegmentHandle;
use crate::postgres::storage::segment_reader::SegmentReader;
use pgrx::{pg_sys::ItemPointerData, *};
use tantivy::directory::FileHandle;
use tantivy::index::Index;
use tantivy::indexer::IndexWriter;
use tantivy::{Directory, IndexReader};

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
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
        let channel_directory =
            ChannelDirectory::new(request_channel_clone, response_channel_clone, index_oid);
        let underlying_index = Index::open(channel_directory).expect("channel index should open");
        let channel_index = SearchIndex {
            underlying_index,
            directory: search_index.directory,
            schema: search_index.schema,
        };
        let reader = channel_index
            .get_reader()
            .unwrap_or_else(|err| panic!("error loading index reader in bulkdelete: {err}"));
        let writer = channel_index
            .get_writer()
            .unwrap_or_else(|err| panic!("error loading index writer in bulkdelete: {err}"));
    });

    let blocking_directory = BlockingDirectory::new(index_oid);
    for message in request_channel.1.iter() {
        match message {
            ChannelRequest::AtomicRead(path) => {
                let data = blocking_directory.atomic_read(&path).unwrap();
                response_channel
                    .0
                    .send(ChannelResponse::Bytes(data))
                    .unwrap();
            }
            ChannelRequest::AtomicWrite(path, data) => {
                blocking_directory.atomic_write(&path, &data).unwrap();
                response_channel
                    .0
                    .send(ChannelResponse::AtomicWriteAck)
                    .unwrap();
            }
            ChannelRequest::GetSegmentHandle(path) => {
                let handle = unsafe { SegmentHandle::open(index_oid, &path).unwrap() };
                response_channel
                    .0
                    .send(ChannelResponse::SegmentHandle(handle))
                    .unwrap();
            }
            ChannelRequest::SegmentRead(path, range, handle) => {
                let reader = SegmentReader::new(index_oid, &path, handle);
                let data = reader.read_bytes(range).unwrap();
                response_channel
                    .0
                    .send(ChannelResponse::Bytes(data.as_slice().to_owned()))
                    .unwrap();
            }
            message => panic!("unexpected message in bulkdelete thread: {:?}", message),
        }
    }

    if stats.is_null() {
        stats = unsafe {
            PgBox::from_pg(
                pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBulkDeleteResult>()).cast(),
            )
        };
    }

    pgrx::info!("got here");

    // if let Some(actual_callback) = callback {
    //     let should_delete = |ctid_val| unsafe {
    //         let mut ctid = ItemPointerData::default();
    //         crate::postgres::utils::u64_to_item_pointer(ctid_val, &mut ctid);
    //         actual_callback(&mut ctid, callback_state)
    //     };
    //     match channel_index.delete(&reader, &writer, should_delete) {
    //         Ok((deleted, not_deleted)) => {
    //             stats.pages_deleted += deleted;
    //             stats.num_pages += not_deleted;
    //         }
    //         Err(err) => {
    //             panic!("error: {err:?}")
    //         }
    //     }
    // }

    // writer
    //     .commit()
    //     .unwrap_or_else(|err| panic!("error committing to index in ambulkdelete: {err}"));

    stats.into_pg()
}
