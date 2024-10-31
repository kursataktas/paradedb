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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use crate::index::IndexError;
use crate::postgres::types::{JsonPath, TantivyValue};
use crate::schema::{SearchDocument, SearchField, SearchFieldConfig, SearchIndexSchema};
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime};
use pgrx::itemptr::{item_pointer_get_both, item_pointer_set_all};
use pgrx::*;
use tantivy::schema::OwnedValue;

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

    // JSON fields require special processing. If a JSON path is configured
    // with 'nested', we need to make a new field for each member of nested arrays.
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

            let is_nested = matches!(
                search_field.config,
                SearchFieldConfig::Json { nested: true, .. }
            );

            if is_nested && !is_json {
                panic!(
                    "search field {} configured as 'nested', but it is not a JSON field",
                    search_field.name
                );
            }

            // Nested json fields don't correspond to an actual Postgres column, so we
            // won't try and look up datums with the field name.
            let datum = if is_nested {
                None
            } else {
                Some(*values.add(attno))
            };

            if is_array {
                (MergeStrategy::Array, search_field, datum, base_oid)
            } else if is_json {
                (MergeStrategy::Json, search_field, datum, base_oid)
            } else {
                (MergeStrategy::Field, search_field, datum, base_oid)
            }
        })
        .partition(|(strategy, _, _, _)| matches!(strategy, MergeStrategy::Json));

    if !other_fields
        .iter() // Check if key field was filtered out for being null.
        .any(|(_, search_field, _, _)| &search_field.id == &schema.key_field().id)
    {
        return Err(IndexError::KeyIdNull);
    }

    let json_field_lookup: HashMap<JsonPath, &SearchField> = json_fields
        .iter()
        .map(|(_, f, _, _)| (JsonPath::from(f.name.0.as_ref()), *f))
        .collect();

    let nested_lookup: HashSet<&JsonPath> = json_field_lookup
        .iter()
        .filter(|(_, f)| matches!(f.config, SearchFieldConfig::Json { nested: true, .. }))
        .map(|(path, _)| path)
        .collect();

    let mut document = schema.new_document(ctid_index_value);
    for (_, search_field, datum, base_oid) in json_fields {
        let path = JsonPath::from(search_field.name.0.as_ref());
        // JSON nested fields won't have datums.
        if let Some(datum) = datum {
            for (path, value) in
                TantivyValue::try_from_datum_json(&nested_lookup, path, datum, base_oid)
                    .expect("must be able to retrieve json values from datum")
            {
                let parent = path.parent();
                let key = path.key;
                let is_nested = nested_lookup.contains(&parent);

                let search_field = if is_nested {
                    json_field_lookup
                        .get(&parent)
                        .expect("search field should exist for json path")
                } else {
                    search_field
                };

                document.insert(search_field.id, OwnedValue::Object(vec![(key, value.0)]))
            }
        }
    }

    // Insert non-JSON fields into all documents
    for (strategy, search_field, datum, base_oid) in other_fields {
        match strategy {
            MergeStrategy::Array => {
                let datum = datum.expect("non-nested JSON field should have an associated datum");
                let datum_values = TantivyValue::try_from_datum_array(datum, base_oid)
                    .unwrap_or_else(|err| panic!("could not read array datum: {err}"));
                for value in datum_values {
                    document.insert(search_field.id, value.tantivy_schema_value());
                }
            }
            _ => {
                let datum = datum.expect("non-nested JSON field should have an associated datum");
                let value = TantivyValue::try_from_datum(datum, base_oid)
                    .unwrap_or_else(|err| panic!("could not read datum: {err}"));
                document.insert(search_field.id, value.tantivy_schema_value());
            }
        }
    }

    Ok(vec![document])
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
