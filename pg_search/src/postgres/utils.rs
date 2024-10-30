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

use std::str::FromStr;

use crate::index::IndexError;
use crate::postgres::types::TantivyValue;
use crate::schema::{SearchDocument, SearchIndexSchema};
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use pgrx::itemptr::{item_pointer_get_both, item_pointer_set_all};
use pgrx::*;

/// Finds and returns the first `USING bm25` index on the specified relation, or [`None`] if there
/// aren't any
pub fn locate_bm25_index(heaprelid: pg_sys::Oid) -> Option<PgRelation> {
    unsafe {
        let heaprel = PgRelation::open(heaprelid);
        for index in heaprel.indices(pg_sys::AccessShareLock as _) {
            if !index.rd_indam.is_null()
                && (*index.rd_indam).ambuild == Some(crate::postgres::build::ambuild)
            {
                return Some(index);
            }
        }
        None
    }
}

/// Rather than using pgrx' version of this function, we use our own, which doesn't leave 2
/// empty bytes in the middle of the 64bit representation.  A ctid being only 48bits means
/// if we leave the upper 16 bits (2 bytes) empty, tantivy will have a better chance of
/// bitpacking or compressing these values.
#[inline(always)]
pub fn item_pointer_to_u64(ctid: pg_sys::ItemPointerData) -> u64 {
    let (blockno, offno) = item_pointer_get_both(ctid);
    let blockno = blockno as u64;
    let offno = offno as u64;

    // shift the BlockNumber left 16 bits -- the length of the OffsetNumber we OR onto the end
    // pgrx's version shifts left 32, which is wasteful
    (blockno << 16) | offno
}

/// Rather than using pgrx' version of this function, we use our own, which doesn't leave 2
/// empty bytes in the middle of the 64bit representation.  A ctid being only 48bits means
/// if we leave the upper 16 bits (2 bytes) empty, tantivy will have a better chance of
/// bitpacking or compressing these values.
#[inline(always)]
pub fn u64_to_item_pointer(value: u64, tid: &mut pg_sys::ItemPointerData) {
    // shift right 16 bits to pop off the OffsetNumber, leaving only the BlockNumber
    // pgrx's version must shift right 32 bits to be in parity with `item_pointer_to_u64()`
    let blockno = (value >> 16) as pg_sys::BlockNumber;
    let offno = value as pg_sys::OffsetNumber;
    item_pointer_set_all(tid, blockno, offno);
}

pub unsafe fn row_to_search_documents(
    ctid: pg_sys::ItemPointerData,
    tupdesc: &PgTupleDesc,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    schema: &SearchIndexSchema,
) -> Result<Vec<SearchDocument>, IndexError> {
    enum MergeStrategy {
        Array,
        Json,
        Field,
    }

    let ctid_index_value = item_pointer_to_u64(ctid);
    let (json_fields, other_fields): (Vec<_>, Vec<_>) = tupdesc
        .iter()
        .enumerate()
        .filter_map(|(attno, attribute)| {
            let attname = attribute.name().to_string();

            schema
                .get_search_field(&attname.clone().into())
                .filter(|_| !(*isnull.add(attno)))
                .map(move |field| (attno, attribute, field))
        })
        .map(move |(attno, attribute, search_field)| {
            let attribute_type_oid = attribute.type_oid();
            let array_type = pg_sys::get_element_type(attribute_type_oid.value());
            let (base_oid, is_array) = if array_type != pg_sys::InvalidOid {
                (PgOid::from(array_type), true)
            } else {
                (attribute_type_oid, false)
            };

            let is_json = matches!(
                base_oid,
                PgOid::BuiltIn(pg_sys::BuiltinOid::JSONBOID | pg_sys::BuiltinOid::JSONOID)
            );

            let datum = *values.add(attno);

            if is_array {
                (MergeStrategy::Array, search_field.id, datum, base_oid)
            } else if is_json {
                (MergeStrategy::Json, search_field.id, datum, base_oid)
            } else {
                (MergeStrategy::Field, search_field.id, datum, base_oid)
            }
        })
        .partition(|(strategy, _, _, _)| matches!(strategy, MergeStrategy::Json));

    if !other_fields
        .iter() // Check if key field was filtered out for being null.
        .any(|(_, id, _, _)| id == &schema.key_field().id)
    {
        return Err(IndexError::KeyIdNull);
    }

    let json_tantivy_values_for_columns = json_fields
        .into_iter()
        .map(|(_, field_id, datum, base_oid)| {
            match TantivyValue::try_from_datum_json(datum, base_oid) {
                Ok(iter) => iter
                    .into_iter()
                    .map(|value| (field_id, value.tantivy_schema_value()))
                    .collect(),
                Err(err) => panic!("error processing json data: {err}"),
            }
        })
        .collect::<Vec<Vec<_>>>();

    let mut documents: Vec<SearchDocument> = json_tantivy_values_for_columns
        .iter()
        .enumerate()
        .flat_map(|(current_column_index, json_values_for_column)| {
            json_values_for_column
                .iter()
                .map(move |(field_id, field_value)| {
                    let mut document = schema.new_document();
                    document.insert(schema.ctid_field().id, ctid_index_value.into());
                    document.insert(*field_id, field_value.clone());
                    (document, current_column_index)
                })
        })
        .map(|(mut document, skip_index)| {
            for (index, column_json_values) in json_tantivy_values_for_columns.iter().enumerate() {
                if index != skip_index {
                    for (other_field_id, other_field_value) in column_json_values {
                        document.insert(*other_field_id, other_field_value.clone());
                    }
                }
            }
            document
        })
        .collect();

    // If there were no JSON fields, make sure we have at least one document.
    if documents.is_empty() {
        documents.push(schema.new_document());
    }

    // Insert non-JSON fields into all documents
    for (strategy, field_id, datum, base_oid) in other_fields {
        match strategy {
            MergeStrategy::Array => {
                let datum_values = TantivyValue::try_from_datum_array(datum, base_oid)
                    .unwrap_or_else(|err| panic!("could not read array datum: {err}"));
                for value in datum_values {
                    for document in &mut documents {
                        document.insert(field_id, value.tantivy_schema_value());
                    }
                }
            }
            _ => {
                let value = TantivyValue::try_from_datum(datum, base_oid)
                    .unwrap_or_else(|err| panic!("could not read datum: {err}"));
                for document in &mut documents {
                    document.insert(field_id, value.tantivy_schema_value());
                }
            }
        }
    }

    Ok(documents)
}

/// Utility function for easy `f64` to `u32` conversion
fn f64_to_u32(n: f64) -> Result<u32> {
    let truncated = n.trunc();
    if truncated.is_nan()
        || truncated.is_infinite()
        || truncated < 0.0
        || truncated > u32::MAX.into()
    {
        return Err(anyhow!("overflow in f64 to u32"));
    }

    Ok(truncated as u32)
}

/// Seconds are represented by `f64` in pgrx, with a maximum of microsecond precision
fn convert_pgrx_seconds_to_chrono(orig: f64) -> Result<(u32, u32, u32)> {
    let seconds = f64_to_u32(orig)?;
    let microseconds = f64_to_u32((orig * 1_000_000.0) % 1_000_000.0)?;
    let nanoseconds = f64_to_u32((orig * 1_000_000_000.0) % 1_000_000_000.0)?;
    Ok((seconds, microseconds, nanoseconds))
}

pub fn convert_pg_date_string(typeoid: PgOid, date_string: &str) -> tantivy::DateTime {
    match typeoid {
        PgOid::BuiltIn(PgBuiltInOids::DATEOID | PgBuiltInOids::DATERANGEOID) => {
            let d = pgrx::datum::Date::from_str(date_string)
                .expect("must be valid postgres date format");
            let micros = NaiveDate::from_ymd_opt(d.year(), d.month().into(), d.day().into())
                .expect("must be able to parse date format")
                .and_hms_opt(0, 0, 0)
                .expect("must be able to set date default time")
                .and_utc()
                .timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID | PgBuiltInOids::TSRANGEOID) => {
            // Since [`pgrx::Timestamp`]s are tied to the Postgres instance's timezone,
            // to figure out *which* timezone it's actually in, we convert to a
            // [`pgrx::TimestampWithTimeZone`].
            // Once the offset is known, we can create and return a [`chrono::NaiveDateTime`]
            // with the appropriate offset.
            let t = pgrx::datum::Timestamp::from_str(date_string)
                .expect("must be a valid postgres timestamp");
            let twtz: datum::TimestampWithTimeZone = t.into();
            let (seconds, _micros, _nanos) = convert_pgrx_seconds_to_chrono(twtz.second())
                .expect("must not overflow converting pgrx seconds");
            let micros =
                NaiveDate::from_ymd_opt(twtz.year(), twtz.month().into(), twtz.day().into())
                    .expect("must be able to convert date timestamp")
                    .and_hms_opt(twtz.hour().into(), twtz.minute().into(), seconds)
                    .expect("must be able to parse timestamp format")
                    .and_utc()
                    .timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID | pg_sys::BuiltinOid::TSTZRANGEOID) => {
            let twtz = pgrx::datum::TimestampWithTimeZone::from_str(date_string)
                .expect("must be a valid postgres timestamp with time zone")
                .to_utc();
            let (seconds, _micros, _nanos) = convert_pgrx_seconds_to_chrono(twtz.second())
                .expect("must not overflow converting pgrx seconds");
            let micros =
                NaiveDate::from_ymd_opt(twtz.year(), twtz.month().into(), twtz.day().into())
                    .expect("must be able to convert timestamp with timezone")
                    .and_hms_opt(twtz.hour().into(), twtz.minute().into(), seconds)
                    .expect("must be able to parse timestamp with timezone")
                    .and_utc()
                    .timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => {
            let t =
                pgrx::datum::Time::from_str(date_string).expect("must be a valid postgres time");
            let (hour, minute, second, micros) = t.to_hms_micro();
            let naive_time =
                NaiveTime::from_hms_micro_opt(hour.into(), minute.into(), second.into(), micros)
                    .expect("must be able to parse time");
            let naive_date = NaiveDate::from_ymd_opt(1970, 1, 1).expect("default date");
            let micros = naive_date.and_time(naive_time).and_utc().timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMETZOID) => {
            let twtz = pgrx::datum::TimeWithTimeZone::from_str(date_string)
                .expect("must be a valid postgres time with time zone")
                .to_utc();
            let (hour, minute, second, micros) = twtz.to_hms_micro();
            let naive_time =
                NaiveTime::from_hms_micro_opt(hour.into(), minute.into(), second.into(), micros)
                    .expect("must be able to parse time with time zone");
            let naive_date = NaiveDate::from_ymd_opt(1970, 1, 1).expect("default date");
            let micros = naive_date.and_time(naive_time).and_utc().timestamp_micros();
            tantivy::DateTime::from_timestamp_micros(micros)
        }
        _ => panic!("Unsupported typeoid: {typeoid:?}"),
    }
}
