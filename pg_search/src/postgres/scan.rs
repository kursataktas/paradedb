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

use crate::index::reader::{SearchIndexReader, SearchResults};
use crate::index::SearchIndex;
use crate::postgres::index::open_search_index;
use crate::postgres::options::SearchIndexCreateOptions;
use crate::postgres::{parallel, ScanStrategy};
use crate::query::SearchQueryInput;
use pgrx::pg_sys::IndexScanDesc;
use pgrx::*;
use tantivy::query::Query;

pub struct Bm25ScanState {
    need_scores: bool,
    reader: SearchIndexReader,
    query: Box<dyn Query>,
    results: SearchResults,
    itup: (Vec<pg_sys::Datum>, Vec<bool>),
    key_field: String,
    key_field_oid: PgOid,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    indexrel: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    unsafe {
        let scandesc = pg_sys::RelationGetIndexScan(indexrel, nkeys, norderbys);

        // we may or may not end up doing an Index Only Scan, but regardless we only need to do
        // this one time
        (*scandesc).xs_hitupdesc = (*indexrel).rd_att;

        scandesc
    }
}

// An annotation to guard the function for PostgreSQL's threading model.
#[pg_guard]
pub extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: ::std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
    fn key_to_search_query_input(key: &pg_sys::ScanKeyData) -> SearchQueryInput {
        match ScanStrategy::try_from(key.sk_strategy).expect("`key.sk_strategy` is unrecognized") {
            ScanStrategy::TextQuery => unsafe {
                let query_string = String::from_datum(key.sk_argument, false)
                    .expect("ScanKey.sk_argument must not be null");
                SearchQueryInput::Parse {
                    query_string,
                    lenient: None,
                    conjunction_mode: None,
                }
            },
            ScanStrategy::SearchQueryInput => unsafe {
                SearchQueryInput::from_datum(key.sk_argument, false)
                    .expect("ScanKey.sk_argument must not be null")
            },
        }
    }

    let (indexrel, keys) = unsafe {
        // SAFETY:  assert the pointers we're going to use are non-null
        assert!(!scan.is_null());
        assert!(!(*scan).indexRelation.is_null());
        assert!(!keys.is_null());
        assert!(nkeys > 0); // Ensure there's at least one key provided for the search.

        let indexrel = (*scan).indexRelation;
        let keys = std::slice::from_raw_parts(keys as *const pg_sys::ScanKeyData, nkeys as usize);

        (PgRelation::from_pg(indexrel), keys)
    };

    // build a Boolean "must" clause of all the ScanKeys
    let mut search_query_input = key_to_search_query_input(&keys[0]);
    for key in &keys[1..] {
        let key = key_to_search_query_input(key);

        search_query_input = SearchQueryInput::Boolean {
            must: vec![search_query_input, key],
            should: vec![],
            must_not: vec![],
        };
    }

    // Create the index and scan state
    let search_index = open_search_index(&indexrel).expect("should be able to open search index");
    let search_reader = search_index
        .get_reader()
        .expect("SearchState should construct cleanly");

    unsafe {
        parallel::maybe_init_parallel_scan(scan, &search_reader.searcher);

        let options = (*(*scan).indexRelation).rd_options as *mut SearchIndexCreateOptions;
        let key_field = (*options)
            .get_key_field()
            .expect("bm25 index should have a key_field")
            .0;

        let need_scores = search_query_input.contains_more_like_this();
        let query = search_index.query(&search_query_input, &search_reader);
        let results = if let Some(segment_number) = parallel::maybe_claim_segment(scan) {
            search_reader.search_segment(
                need_scores,
                (*scan).xs_want_itup.then(|| key_field.clone()),
                segment_number,
                &query,
            )
        } else if pg_sys::ParallelWorkerNumber > -1 {
            SearchResults::None
        } else {
            search_reader.search_via_channel(
                need_scores,
                (*scan).xs_want_itup.then(|| key_field.clone()),
                SearchIndex::executor(),
                &query,
            )
        };

        let natts = (*(*scan).xs_hitupdesc).natts as usize;
        let scan_state = if (*scan).xs_want_itup {
            Bm25ScanState {
                need_scores,
                reader: search_reader,
                query,
                results,
                itup: (vec![pg_sys::Datum::null(); natts], vec![true; natts]),
                key_field,
                key_field_oid: PgOid::from(
                    (*(*scan).xs_hitupdesc).attrs.as_slice(natts)[0].atttypid,
                ),
            }
        } else {
            Bm25ScanState {
                need_scores,
                reader: search_reader,
                query,
                results,
                itup: (vec![], vec![]),
                key_field,
                key_field_oid: PgOid::Invalid,
            }
        };

        (*scan).opaque = PgMemoryContexts::CurrentMemoryContext
            .leak_and_drop_on_delete(scan_state)
            .cast();
    }
}

#[pg_guard]
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    let state = unsafe {
        // SAFETY:  We set `scan.opaque` to a leaked pointer of type `PgSearchScanState` above in
        // amrescan, which is always called prior to this function
        (*scan).opaque.cast::<Bm25ScanState>().as_mut()
    }
    .expect("no scan.opaque state");

    unsafe {
        (*scan).xs_recheck = false;
    }

    loop {
        match state.results.next() {
            Some((scored, _)) => unsafe {
                let tid = &mut (*scan).xs_heaptid;
                crate::postgres::utils::u64_to_item_pointer(scored.ctid, tid);

                if (*scan).xs_want_itup {
                    match scored
                        .key
                        .expect("should have retrieved the key_field")
                        .try_into_datum(state.key_field_oid)
                        .expect("key_field value should convert to a Datum")
                    {
                        // got a valid Datum
                        Some(key_field_datum) => {
                            state.itup.0[0] = key_field_datum;
                            state.itup.1[0] = false;
                        }

                        // we got a NULL for the key_field.  Highly unlikely but definitely possible
                        None => {
                            state.itup.0[0] = pg_sys::Datum::null();
                            state.itup.1[0] = true;
                        }
                    }

                    let values = state.itup.0.as_mut_ptr();
                    let nulls = state.itup.1.as_mut_ptr();

                    if (*scan).xs_hitup.is_null() {
                        (*scan).xs_hitup =
                            pg_sys::heap_form_tuple((*scan).xs_hitupdesc, values, nulls);
                    } else {
                        pg_sys::ffi::pg_guard_ffi_boundary(|| {
                            extern "C" {
                                fn heap_compute_data_size(
                                    tupleDesc: pg_sys::TupleDesc,
                                    values: *mut pg_sys::Datum,
                                    isnull: *mut bool,
                                ) -> pg_sys::Size;
                                fn heap_fill_tuple(
                                    tupleDesc: pg_sys::TupleDesc,
                                    values: *mut pg_sys::Datum,
                                    isnull: *mut bool,
                                    data: *mut ::core::ffi::c_char,
                                    data_size: pg_sys::Size,
                                    infomask: *mut pg_sys::uint16,
                                    bit: *mut pg_sys::bits8,
                                );
                            }
                            let data_len =
                                heap_compute_data_size((*scan).xs_hitupdesc, values, nulls);
                            let td = (*(*scan).xs_hitup).t_data;

                            // TODO:  seems like this could crash with a varlena "key_field" of varrying sizes per row
                            heap_fill_tuple(
                                (*scan).xs_hitupdesc,
                                values,
                                nulls,
                                td.cast::<std::ffi::c_char>().add((*td).t_hoff as usize),
                                data_len,
                                &mut (*td).t_infomask,
                                (*td).t_bits.as_mut_ptr(),
                            );
                        });
                    }
                }

                return true;
            },
            None => {
                if search_next_segment(scan, state) {
                    // loop back around to start returning results from this segment
                    continue;
                }

                // we are done returning results
                return false;
            }
        }
    }
}

#[pg_guard]
pub extern "C" fn amgetbitmap(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    assert!(!tbm.is_null());
    assert!(!scan.is_null());

    let state = unsafe {
        // SAFETY:  We set `scan.opaque` to a leaked pointer of type `PgSearchScanState` above in
        // amrescan, which is always called prior to this function
        (*scan).opaque.cast::<Bm25ScanState>().as_mut()
    }
    .expect("no scan.opaque state");

    let mut cnt = 0i64;
    loop {
        for (scored, _) in &mut state.results {
            let mut tid = pg_sys::ItemPointerData::default();
            crate::postgres::utils::u64_to_item_pointer(scored.ctid, &mut tid);

            unsafe {
                // SAFETY:  `tbm` has been asserted to be non-null and our `&mut tid` has been
                // initialized as a stack-allocated ItemPointerData
                pg_sys::tbm_add_tuples(tbm, &mut tid, 1, false);
            }

            cnt += 1;
        }

        // check if the bitmap scan needs to claim another individual segment
        if search_next_segment(scan, state) {
            continue;
        }

        break;
    }

    cnt
}

// if there's a segment to be claimed for parallel query execution, do that now
fn search_next_segment(scan: IndexScanDesc, state: &mut Bm25ScanState) -> bool {
    if let Some(segment_number) = parallel::maybe_claim_segment(scan) {
        state.results = state.reader.search_segment(
            state.need_scores,
            unsafe { (*scan).xs_want_itup.then(|| state.key_field.clone()) },
            segment_number,
            &state.query,
        );
        return true;
    }
    false
}

#[pg_guard]
pub extern "C" fn amcanreturn(indexrel: pg_sys::Relation, attno: i32) -> bool {
    if attno != 1 {
        // currently, we only support returning the "key_field", which will always be the first
        // index attribute
        return false;
    }

    unsafe {
        assert!(!indexrel.is_null());
        assert!(!(*indexrel).rd_att.is_null());
        let tupdesc = PgTupleDesc::from_pg_unchecked((*indexrel).rd_att);

        let att = tupdesc
            .get((attno - 1) as usize)
            .expect("attno should exist in index tupledesc");

        // we can only return a field if it's one of the below types -- basically pass-by-value (non tokenized) data types
        [
            pg_sys::INT4OID,
            pg_sys::INT8OID,
            pg_sys::FLOAT4OID,
            pg_sys::FLOAT8OID,
            pg_sys::BOOLOID,
        ]
        .contains(&att.atttypid)
    }
}
