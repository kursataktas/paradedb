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

//! Tests for ParadeDB's Custom Scan implementation

mod fixtures;

use fixtures::*;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
fn attribute_1_of_table_has_wrong_type(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id, ) = "SELECT id, description FROM paradedb.bm25_search WHERE description @@@ 'keyboard' OR id = 1 ORDER BY id LIMIT 1"
        .fetch_one::<(i32,)>(&mut conn);
    assert_eq!(id, 1);
}

#[rstest]
fn generates_custom_scan_for_or(mut conn: PgConnection) {
    use serde_json::Value;

    SimpleProductsTable::setup().execute(&mut conn);

    let (plan, ) = "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard' OR description @@@ 'shoes'".fetch_one::<(Value,)>(&mut conn);
    let plan = plan
        .get(0)
        .unwrap()
        .as_object()
        .unwrap()
        .get("Plan")
        .unwrap()
        .as_object()
        .unwrap();
    eprintln!("{plan:#?}");
    assert_eq!(
        plan.get("Custom Plan Provider"),
        Some(&Value::String(String::from("ParadeDB Scan")))
    );
}

#[rstest]
fn generates_custom_scan_for_and(mut conn: PgConnection) {
    use serde_json::Value;

    SimpleProductsTable::setup().execute(&mut conn);

    "SET enable_indexscan TO off;".execute(&mut conn);

    let (plan, ) = "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard' AND description @@@ 'shoes'".fetch_one::<(Value,)>(&mut conn);
    let plan = plan
        .get(0)
        .unwrap()
        .as_object()
        .unwrap()
        .get("Plan")
        .unwrap()
        .as_object()
        .unwrap();
    eprintln!("{plan:#?}");
    assert_eq!(
        plan.get("Custom Plan Provider"),
        Some(&Value::String(String::from("ParadeDB Scan")))
    );
}

#[rstest]
fn field_on_left(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id,) =
        "SELECT id FROM paradedb.bm25_search WHERE description @@@ 'keyboard' ORDER BY id ASC"
            .fetch_one::<(i32,)>(&mut conn);
    assert_eq!(id, 1);
}

#[rstest]
fn table_on_left(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id, ) =
        "SELECT id FROM paradedb.bm25_search WHERE bm25_search @@@ 'description:keyboard' ORDER BY id ASC"
            .fetch_one::<(i32,)>(&mut conn);
    assert_eq!(id, 1);
}

#[rstest]
fn scores_project(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id, score) =
        "SELECT id, paradedb.score(id) FROM paradedb.bm25_search WHERE description @@@ 'keyboard' ORDER BY paradedb.score(id) DESC LIMIT 1"
            .fetch_one::<(i32, f32)>(&mut conn);
    assert_eq!(id, 2);
    assert_eq!(score, 3.2668595);
}

#[rstest]
fn snippets_project(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id, snippet) =
        "SELECT id, paradedb.snippet(description) FROM paradedb.bm25_search WHERE description @@@ 'keyboard' ORDER BY paradedb.score(id) DESC LIMIT 1"
            .fetch_one::<(i32, String)>(&mut conn);
    assert_eq!(id, 2);
    assert_eq!(snippet, String::from("Plastic <b>Keyboard</b>"));
}

#[rstest]
fn scores_and_snippets_project(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id, score, snippet) =
        "SELECT id, paradedb.score(id), paradedb.snippet(description) FROM paradedb.bm25_search WHERE description @@@ 'keyboard' ORDER BY paradedb.score(id) DESC LIMIT 1"
            .fetch_one::<(i32, f32, String)>(&mut conn);
    assert_eq!(id, 2);
    assert_eq!(score, 3.2668595);
    assert_eq!(snippet, String::from("Plastic <b>Keyboard</b>"));
}

#[rstest]
fn mingets(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let (id, snippet) =
        "SELECT id, paradedb.snippet(description, '<MING>', '</MING>') FROM paradedb.bm25_search WHERE description @@@ 'teddy bear'"
            .fetch_one::<(i32, String)>(&mut conn);
    assert_eq!(id, 40);
    assert_eq!(
        snippet,
        String::from("Plush <MING>teddy</MING> <MING>bear</MING>")
    );
}

#[rstest]
fn scores_with_expressions(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let result = r#"
select id,
    description,
    paradedb.score(id),
    rating,
    paradedb.score(id) * rating    /* testing this, specifically */
from paradedb.bm25_search 
where metadata @@@ 'color:white' 
order by 5 desc, score desc
limit 1;        
        "#
    .fetch_one::<(i32, String, f32, i32, f64)>(&mut conn);
    assert_eq!(
        result,
        (
            25,
            "Anti-aging serum".into(),
            3.2455924,
            4,
            12.982369422912598
        )
    );
}

#[rstest]
fn simple_join_with_scores_and_both_sides(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let result = r#"
select a.id, 
    a.score, 
    b.id, 
    b.score
from (select paradedb.score(id), * from paradedb.bm25_search) a
inner join (select paradedb.score(id), * from paradedb.bm25_search) b on a.id = b.id
where a.description @@@ 'bear' AND b.description @@@ 'teddy bear';"#
        .fetch_one::<(i32, f32, i32, f32)>(&mut conn);
    assert_eq!(result, (40, 3.3322046, 40, 6.664409));
}

#[rstest]
fn simple_join_with_scores_or_both_sides(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let result = r#"
select a.id, 
    a.score, 
    b.id, 
    b.score
from (select paradedb.score(id), * from paradedb.bm25_search) a
inner join (select paradedb.score(id), * from paradedb.bm25_search) b on a.id = b.id
where a.description @@@ 'bear' OR b.description @@@ 'teddy bear';"#
        .fetch_one::<(i32, f32, i32, f32)>(&mut conn);
    assert_eq!(result, (40, 4.332205, 40, 7.664409));
}

#[rstest]
fn add_scores_across_joins_issue1753(mut conn: PgConnection) {
    r#"
CALL paradedb.create_bm25_test_table(table_name => 'mock_items', schema_name => 'public');
CALL paradedb.create_bm25(
    	index_name => 'mock_items',
        table_name => 'mock_items',
    	schema_name => 'public',
        key_field => 'id',
        text_fields => paradedb.field('description') || paradedb.field('category'),
    	numeric_fields => paradedb.field('rating'),
    	boolean_fields => paradedb.field('in_stock'),
    	json_fields => paradedb.field('metadata'),
        datetime_fields => paradedb.field('created_at') || paradedb.field('last_updated_date') || paradedb.field('latest_available_time'));


CALL paradedb.create_bm25_test_table(
  schema_name => 'public',
  table_name => 'orders',
  table_type => 'Orders'
);
CALL paradedb.create_bm25(
  index_name => 'orders_idx',
  table_name => 'orders',
  key_field => 'order_id',
  text_fields => paradedb.field('customer_name')
);

ALTER TABLE orders
ADD CONSTRAINT foreign_key_product_id
FOREIGN KEY (product_id)
REFERENCES mock_items(id);   
    "#.execute(&mut conn);

    // this one doesn't plan a custom scan at all, so scores come back as NaN
    let result = r#"
SELECT o.order_id, m.description, paradedb.score(o.order_id) + paradedb.score(m.id) as score
FROM orders o
JOIN mock_items m ON o.product_id = m.id
WHERE o.customer_name @@@ 'Johnson' AND m.description @@@ 'shoes'
ORDER BY order_id
LIMIT 1;"#
        .fetch_one::<(i32, String, f32)>(&mut conn);
    assert_eq!(result, (3, "Sleek running shoes".into(), 5.406531));
}

#[rustfmt::skip]
#[rstest]
fn join_issue_1776(mut conn: PgConnection) {
    r#"CALL paradedb.create_bm25_test_table(
          schema_name => 'public',
          table_name => 'mock_items'
        );
        
        CALL paradedb.create_bm25(
          index_name => 'search_idx',
          table_name => 'mock_items',
          key_field => 'id',
          text_fields => paradedb.field('description') || paradedb.field('category'),
          numeric_fields => paradedb.field('rating'),
          boolean_fields => paradedb.field('in_stock'),
          datetime_fields => paradedb.field('created_at'),
          json_fields => paradedb.field('metadata')
        );
        
        CALL paradedb.create_bm25_test_table(
          schema_name => 'public',
          table_name => 'orders',
          table_type => 'Orders'
        );
        
        ALTER TABLE orders
        ADD CONSTRAINT foreign_key_product_id
        FOREIGN KEY (product_id)
        REFERENCES mock_items(id);
        
        CALL paradedb.create_bm25(
          index_name => 'orders_idx',
          table_name => 'orders',
          key_field => 'order_id',
          text_fields => paradedb.field('customer_name')
        );
    "#
    .execute(&mut conn);

    let results = r#"
        SELECT o.order_id, m.description, o.customer_name, paradedb.score(o.order_id) as orders_score, paradedb.score(m.id) as items_score
        FROM orders o
        JOIN mock_items m ON o.product_id = m.id
        WHERE o.customer_name @@@ 'Johnson' AND m.description @@@ 'shoes' OR m.description @@@ 'Smith'
        ORDER BY order_id
        LIMIT 5;
    "#.fetch_result::<(i32, String, String, f32, f32)>(&mut conn).expect("query failed");

    assert_eq!(results[0], (3, "Sleek running shoes".into(), "Alice Johnson".into(), 4.921624, 2.4849067));
    assert_eq!(results[1], (6, "White jogging shoes".into(), "Alice Johnson".into(), 4.921624, 2.4849067));
    assert_eq!(results[2], (36,"White jogging shoes".into(), "Alice Johnson".into(), 4.921624, 2.4849067));
}

#[rustfmt::skip]
#[rstest]
fn join_issue_1826(mut conn: PgConnection) {
    r#"CALL paradedb.create_bm25_test_table(
          schema_name => 'public',
          table_name => 'mock_items'
        );
        
        CALL paradedb.create_bm25(
          index_name => 'search_idx',
          table_name => 'mock_items',
          key_field => 'id',
          text_fields => paradedb.field('description') || paradedb.field('category'),
          numeric_fields => paradedb.field('rating'),
          boolean_fields => paradedb.field('in_stock'),
          datetime_fields => paradedb.field('created_at'),
          json_fields => paradedb.field('metadata')
        );
        
        CALL paradedb.create_bm25_test_table(
          schema_name => 'public',
          table_name => 'orders',
          table_type => 'Orders'
        );
        
        ALTER TABLE orders
        ADD CONSTRAINT foreign_key_product_id
        FOREIGN KEY (product_id)
        REFERENCES mock_items(id);
        
        CALL paradedb.create_bm25(
          index_name => 'orders_idx',
          table_name => 'orders',
          key_field => 'order_id',
          text_fields => paradedb.field('customer_name')
        );
    "#
    .execute(&mut conn);

    let results = r#"
        SELECT o.order_id, m.description, o.customer_name, paradedb.score(o.order_id) as orders_score, paradedb.score(m.id) as items_score
        FROM orders o
        JOIN mock_items m ON o.product_id = m.id
        WHERE o.customer_name @@@ 'Johnson' AND m.description @@@ 'shoes' OR m.description @@@ 'Smith'
        ORDER BY paradedb.score(m.id) desc, m.id asc
        LIMIT 1;
    "#.fetch_result::<(i32, String, String, f32, f32)>(&mut conn).expect("query failed");

    assert_eq!(results[0], (3, "Sleek running shoes".into(), "Alice Johnson".into(), 4.921624, 2.4849067));
}

/// tests that an ERROR raised in the middle of executing a our custom scan doesn't leave open
/// tantivy file handles
#[rstest]
fn leaky_file_handles(mut conn: PgConnection) {
    r#"
        CREATE OR REPLACE FUNCTION raise_exception(int, int) RETURNS bool LANGUAGE plpgsql AS $$
        DECLARE
        BEGIN
            IF $1 = $2 THEN
                RAISE EXCEPTION 'error! % = %', $1, $2;
            END IF;
            RETURN false;
        END;
        $$;
    "#
    .execute(&mut conn);

    let (pid,) = "SELECT pg_backend_pid()".fetch_one::<(i32,)>(&mut conn);
    SimpleProductsTable::setup().execute(&mut conn);

    // this will raise an error when it hits id #12
    let result = "SELECT id, paradedb.score(id), raise_exception(id, 12) FROM paradedb.bm25_search WHERE category @@@ 'electronics' ORDER BY paradedb.score(id) DESC, id LIMIT 10"
        .execute_result(&mut conn);
    assert!(result.is_err());
    assert_eq!(
        "error returned from database: error! 12 = 12",
        &format!("{}", result.err().unwrap())
    );

    fn tantivy_files_still_open(pid: i32) -> bool {
        let output = std::process::Command::new("lsof")
            .arg("-p")
            .arg(pid.to_string())
            .output()
            .expect("`lsof` command should not fail`");

        let stdout = String::from_utf8_lossy(&output.stdout);
        eprintln!("stdout: {}", stdout);
        stdout.contains("/tantivy/")
    }

    // see if there's still some open tantivy files
    if tantivy_files_still_open(pid) {
        // if there are, they're probably (hopefully!) from where we the postgres connection
        // is waiting on merge threads in the background.  So we'll give it 5 seconds and try again

        eprintln!("sleeping for 5s and checking open files again");
        std::thread::sleep(std::time::Duration::from_secs(5));

        // this time asserting for real
        assert!(!tantivy_files_still_open(pid));
    }
}
