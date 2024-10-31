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

use super::SearchIndex;
use crate::query::SearchQueryInput;
use crate::schema::{SearchFieldName, SearchIndexSchema};
use anyhow::Result;
use pgrx::PgRelation;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use tantivy::collector::{Collector, TopDocs};
use tantivy::fastfield::Column;
use tantivy::query::QueryParser;
use tantivy::schema::{FieldType, Value};
use tantivy::{
    query::Query, DocAddress, DocId, Order, Score, Searcher, SegmentOrdinal, TantivyDocument,
    TantivyError,
};
use tantivy::{snippet::SnippetGenerator, Executor};
use tracing::debug;

const CACHE_NUM_BLOCKS: usize = 10;

/// Represents a matching document from a tantivy search.  Typically it is returned as an Iterator
/// Item alongside the originating tantivy [`DocAddress`]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchIndexScore {
    pub ctid: u64,
    pub bm25: f32,
}

impl SearchIndexScore {
    #[inline]
    pub fn new(ffcolumn: &Column<u64>, doc: DocId, score: Score) -> Self {
        Self {
            ctid: ffcolumn
                .first(doc)
                .expect("ctid should have a non-null value"),
            bm25: score,
        }
    }
}

impl PartialOrd for SearchIndexScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.bm25.partial_cmp(&other.bm25)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// An iterator of the different styles of search results we can return
#[derive(Default)]
pub enum SearchResults {
    #[default]
    None,

    TopNByScore(usize, std::vec::IntoIter<(OrderedScore, DocAddress)>),

    TopNByField(usize, std::vec::IntoIter<(SearchIndexScore, DocAddress)>),

    #[allow(clippy::type_complexity)]
    Channel(std::iter::Flatten<crossbeam::channel::IntoIter<Vec<(SearchIndexScore, DocAddress)>>>),

    SingleSegment(usize, std::vec::IntoIter<(SearchIndexScore, DocAddress)>),
}

#[derive(PartialEq, Clone)]
pub struct OrderedScore {
    dir: SortDirection,
    score: SearchIndexScore,
}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let cmp = self.score.partial_cmp(&other.score);
        match self.dir {
            SortDirection::Desc => cmp,
            SortDirection::Asc => cmp.map(|o| o.reverse()),
        }
    }
}

impl Debug for SearchResults {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchResults::None => write!(f, "SearchResults::None"),
            SearchResults::TopNByScore(count, iter) => {
                write!(f, "SearchResults::TopNByScore({count}, {:?})", iter.len())
            }
            SearchResults::TopNByField(count, iter) => {
                write!(f, "SearchResults::TopNByField({count}, {:?})", iter.len())
            }
            SearchResults::Channel(iter) => {
                write!(f, "SearchResults::FastPath(~{:?})", iter.size_hint())
            }
            SearchResults::SingleSegment(count, iter) => {
                write!(f, "SearchResults::SingleSegment({count}, {:?})", iter.len())
            }
        }
    }
}

impl Iterator for SearchResults {
    type Item = (SearchIndexScore, DocAddress);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SearchResults::None => None,
            SearchResults::TopNByScore(_, iter) => iter
                .next()
                .map(|(OrderedScore { score, .. }, doc_address)| (score, doc_address)),
            SearchResults::TopNByField(_, iter) => iter.next(),
            SearchResults::Channel(iter) => iter.next(),
            SearchResults::SingleSegment(_, iter) => iter.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            SearchResults::None => (0, Some(0)),
            SearchResults::TopNByScore(_, iter) => iter.size_hint(),
            SearchResults::TopNByField(_, iter) => iter.size_hint(),
            SearchResults::Channel(iter) => iter.size_hint(),
            SearchResults::SingleSegment(_, iter) => iter.size_hint(),
        }
    }

    #[inline]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        match self {
            SearchResults::None => 0,
            SearchResults::TopNByScore(count, _) => count,
            SearchResults::TopNByField(count, _) => count,
            SearchResults::Channel(iter) => iter.count(),
            SearchResults::SingleSegment(count, _) => count,
        }
    }
}

impl SearchResults {
    pub fn len(&self) -> Option<usize> {
        match self {
            SearchResults::None => Some(0),
            SearchResults::TopNByScore(count, _) => Some(*count),
            SearchResults::TopNByField(count, _) => Some(*count),
            SearchResults::Channel(_) => None,
            SearchResults::SingleSegment(count, _) => Some(*count),
        }
    }
}

#[derive(Clone)]
pub struct SearchIndexReader {
    pub searcher: Searcher,
    pub schema: SearchIndexSchema,
    pub underlying_reader: tantivy::IndexReader,
}

impl SearchIndexReader {
    pub fn new(search_index: &SearchIndex) -> Result<Self> {
        let schema = search_index.schema.clone();
        let reader = search_index
            .underlying_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        Ok(SearchIndexReader {
            searcher,
            schema: schema.clone(),
            underlying_reader: reader,
        })
    }

    pub fn get_doc(&self, doc_address: DocAddress) -> tantivy::Result<TantivyDocument> {
        self.searcher.doc(doc_address)
    }

    /// Scan the index and use the provided callback to search for Documents with ctid
    /// values that need to be deleted.
    pub fn get_ctids_to_delete(
        &self,
        should_delete: impl Fn(u64) -> bool,
    ) -> Result<(Vec<u64>, u32)> {
        let mut not_deleted: u32 = 0;
        let mut ctids_to_delete: Vec<u64> = vec![];

        let ctid_field = self.schema.ctid_field().id.0;
        for segment_reader in self.searcher.segment_readers() {
            let store_reader = segment_reader
                .get_store_reader(CACHE_NUM_BLOCKS)
                .expect("Failed to get store reader");

            for doc in store_reader.iter::<TantivyDocument>(segment_reader.alive_bitset()) {
                // if a document failed to deserialize, that's probably a hard error indicating the
                // index is corrupt.  So return that back to the caller immediately
                let doc = doc?;

                if let Some(ctid) = doc.get_first(ctid_field).and_then(|ctid| ctid.as_u64()) {
                    if should_delete(ctid) {
                        ctids_to_delete.push(ctid);
                    } else {
                        not_deleted += 1;
                    }
                } else {
                    // NB:  in a perfect world, this shouldn't happen.  But we did have a bug where
                    // the "ctid" field was not being `STORED`, which caused this
                    debug!(
                        "document `{doc:?}` in segment `{}` has no ctid",
                        segment_reader.segment_id()
                    );
                }
            }
        }

        Ok((ctids_to_delete, not_deleted))
    }

    /// Returns the index size, in bytes, according to tantivy
    pub fn byte_size(&self) -> Result<u64> {
        Ok(self
            .underlying_reader
            .searcher()
            .space_usage()
            .map(|space| space.total().get_bytes())?)
    }

    pub fn snippet_generator(&self, field_name: &str, query: &dyn Query) -> SnippetGenerator {
        let field = self
            .schema
            .get_search_field(&SearchFieldName(field_name.into()))
            .expect("cannot generate snippet, field does not exist");

        match self.schema.schema.get_field_entry(field.into()).field_type() {
            FieldType::Str(_) => {
                SnippetGenerator::create(&self.searcher, query, field.into())
                    .unwrap_or_else(|err| panic!("failed to create snippet generator for field: {field_name}... {err}"))
            }
            _ => panic!("failed to create snippet generator for field: {field_name}... can only highlight text fields")
        }
    }

    /// Search the Tantivy index for matching documents, in the background, streaming the matching
    /// documents back as they're found.
    ///
    /// The order of returned docs is unspecified.
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search_via_channel(
        &self,
        need_scores: bool,
        sort_segments_by_ctid: bool,
        executor: &'static Executor,
        query: &dyn Query,
    ) -> SearchResults {
        let (sender, receiver) = crossbeam::channel::unbounded();
        let collector =
            collector::ChannelCollector::new(need_scores, sort_segments_by_ctid, sender);
        let searcher = self.searcher.clone();
        let schema = self.schema.schema.clone();

        let owned_query = query.box_clone();
        std::thread::spawn(move || {
            searcher
                .search_with_executor(
                    &owned_query,
                    &collector,
                    executor,
                    if need_scores {
                        tantivy::query::EnableScoring::Enabled {
                            searcher: &searcher,
                            statistics_provider: &searcher,
                        }
                    } else {
                        tantivy::query::EnableScoring::Disabled {
                            schema: &schema,
                            searcher_opt: Some(&searcher),
                        }
                    },
                )
                .expect("failed to search")
        });

        SearchResults::Channel(receiver.into_iter().flatten())
    }

    /// Search a specific index segment for matching documents.
    ///
    /// The order of returned docs is unspecified.
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search_segment(
        &self,
        need_scores: bool,
        segment_ord: SegmentOrdinal,
        query: &dyn Query,
    ) -> SearchResults {
        let collector = vec_collector::VecCollector::new(need_scores);
        let weight = query
            .weight(if need_scores {
                tantivy::query::EnableScoring::Enabled {
                    searcher: &self.searcher,
                    statistics_provider: &self.searcher,
                }
            } else {
                tantivy::query::EnableScoring::Disabled {
                    schema: &self.schema.schema,
                    searcher_opt: Some(&self.searcher),
                }
            })
            .expect("weight should be constructable");
        let segment_reader = self.searcher.segment_reader(segment_ord);
        let results = collector
            .collect_segment(weight.as_ref(), segment_ord, segment_reader)
            .expect("single segment collection should succeed");
        SearchResults::SingleSegment(results.len(), results.into_iter())
    }

    /// Search the Tantivy index for the "top N" matching documents.
    ///
    /// The documents are returned in score order.  Most relevant first if `sortdir` is [`SortDirection::Desc`],
    /// or least relevant first if it's [`SortDirection::Asc`].
    ///
    /// It has no understanding of Postgres MVCC visibility.  It is the caller's responsibility to
    /// handle that, if it's necessary.
    pub fn search_top_n(
        &self,
        executor: &'static Executor,
        query: &dyn Query,
        sort_field: Option<String>,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        if let Some(sort_field) = sort_field {
            self.top_by_field(executor, query, sort_field, sortdir, n)
        } else {
            self.top_by_score(executor, query, sortdir, n)
        }
    }

    fn top_by_field(
        &self,
        executor: &Executor,
        query: &dyn Query,
        sort_field: String,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        impl From<SortDirection> for tantivy::Order {
            fn from(value: SortDirection) -> Self {
                match value {
                    SortDirection::Asc => Order::Asc,
                    SortDirection::Desc => Order::Desc,
                }
            }
        }

        let sort_field = self
            .schema
            .get_search_field(&SearchFieldName(sort_field.clone()))
            .expect("sort field should exist in index schema");

        let collector =
            TopDocs::with_limit(n).order_by_u64_field(&sort_field.name.0, sortdir.into());
        let top_docs = self
            .searcher
            .search_with_executor(
                query,
                &collector,
                executor,
                tantivy::query::EnableScoring::Enabled {
                    searcher: &self.searcher,
                    statistics_provider: &self.searcher,
                },
            )
            .expect("failed to search");

        let top_docs = top_docs
            .into_iter()
            .map(|(_, doc_address)| {
                let ctid = self
                    .searcher
                    .segment_reader(doc_address.segment_ord)
                    .fast_fields()
                    .u64("ctid")
                    .expect("ctid should be a fast field");
                (
                    SearchIndexScore::new(&ctid, doc_address.doc_id, 1.0),
                    doc_address,
                )
            })
            .collect::<Vec<_>>();

        SearchResults::TopNByField(top_docs.len(), top_docs.into_iter())
    }

    fn top_by_score(
        &self,
        executor: &Executor,
        query: &dyn Query,
        sortdir: SortDirection,
        n: usize,
    ) -> SearchResults {
        let collector =
            TopDocs::with_limit(n).tweak_score(move |segment_reader: &tantivy::SegmentReader| {
                let ctid_ff = segment_reader
                    .fast_fields()
                    .u64("ctid")
                    .expect("ctid should be a fast field");

                move |doc: DocId, original_score: Score| OrderedScore {
                    dir: sortdir,
                    score: SearchIndexScore::new(&ctid_ff, doc, original_score),
                }
            });

        let top_docs = self
            .searcher
            .search_with_executor(
                query,
                &collector,
                executor,
                tantivy::query::EnableScoring::Enabled {
                    searcher: &self.searcher,
                    statistics_provider: &self.searcher,
                },
            )
            .expect("failed to search")
            .into_iter();

        SearchResults::TopNByScore(top_docs.len(), top_docs.into_iter())
    }

    pub fn estimate_docs(
        &self,
        indexrel: &PgRelation,
        mut query_parser: QueryParser,
        search_query_input: SearchQueryInput,
    ) -> Option<usize> {
        let readers = self.searcher.segment_readers();
        let (ordinal, largest_reader) = readers
            .iter()
            .enumerate()
            .max_by_key(|(_, reader)| reader.num_docs())?;

        let collector = tantivy::collector::Count;
        let schema = self.schema.schema.clone();
        let query = &search_query_input
            .clone()
            .into_tantivy_query(&(indexrel, &self.schema), &mut query_parser, &self.searcher)
            .expect("must be able to parse query");
        let weight = match query.weight(tantivy::query::EnableScoring::Disabled {
            schema: &schema,
            searcher_opt: Some(&self.searcher),
        }) {
            // created the Weight, no problem
            Ok(weight) => weight,

            // got an error trying to create the weight.  This *likely* means
            // the query requires scoring, so try again with scoring enabled.
            // I've seen this with the `MoreLikeThis` query type.
            //
            // NB:  we could just return `None` here and let the caller deal with it?
            //      a deciding factor might be if users complain that query planning
            //      is too slow when they use constructs like `MoreLikeThis`
            Err(TantivyError::InvalidArgument(_)) => query
                .weight(tantivy::query::EnableScoring::Enabled {
                    searcher: &self.searcher,
                    statistics_provider: &self.searcher,
                })
                .expect("creating a Weight from a Query should not fail"),

            // something completely unexpected happen, so just panic
            Err(e) => panic!("{:?}", e),
        };

        let count = collector
            .collect_segment(weight.as_ref(), ordinal as SegmentOrdinal, largest_reader)
            .expect("counting docs in the largest segment should not fail")
            .max(1); // want to assume at least 1 matching document
        let segment_doc_proportion =
            largest_reader.num_docs() as f64 / self.searcher.num_docs() as f64;

        Some((count as f64 / segment_doc_proportion).ceil() as usize)
    }
}

mod collector {
    use crate::index::reader::SearchIndexScore;
    use tantivy::collector::{Collector, SegmentCollector};
    use tantivy::fastfield::Column;
    use tantivy::{DocAddress, DocId, Score, SegmentOrdinal, SegmentReader};

    /// A [`Collector`] that uses a crossbeam channel to stream the results directly out of
    /// each segment, in parallel, as tantivy find each doc.
    pub struct ChannelCollector {
        need_scores: bool,
        sender: crossbeam::channel::Sender<Vec<(SearchIndexScore, DocAddress)>>,
        sort_segments_by_ctid: bool,
    }

    impl ChannelCollector {
        pub fn new(
            need_scores: bool,
            sort_segments_by_ctid: bool,
            sender: crossbeam::channel::Sender<Vec<(SearchIndexScore, DocAddress)>>,
        ) -> Self {
            Self {
                need_scores,
                sender,
                sort_segments_by_ctid,
            }
        }
    }

    impl Collector for ChannelCollector {
        type Fruit = ();
        type Child = ChannelSegmentCollector;

        fn for_segment(
            &self,
            segment_local_id: SegmentOrdinal,
            segment_reader: &SegmentReader,
        ) -> tantivy::Result<Self::Child> {
            Ok(ChannelSegmentCollector {
                segment_ord: segment_local_id,
                sender: self.sender.clone(),
                fruit: Vec::new(),
                ctid_ff: segment_reader
                    .fast_fields()
                    .u64("ctid")
                    .expect("ctid should be a u64 fast field"),
                sort_by_ctid: self.sort_segments_by_ctid,
            })
        }

        fn requires_scoring(&self) -> bool {
            self.need_scores
        }

        fn merge_fruits(&self, _segment_fruits: Vec<()>) -> tantivy::Result<Self::Fruit> {
            Ok(())
        }
    }

    pub struct ChannelSegmentCollector {
        segment_ord: SegmentOrdinal,
        sender: crossbeam::channel::Sender<Vec<(SearchIndexScore, DocAddress)>>,
        fruit: Vec<(SearchIndexScore, DocAddress)>,
        ctid_ff: Column<u64>,
        sort_by_ctid: bool,
    }

    impl SegmentCollector for ChannelSegmentCollector {
        type Fruit = ();

        fn collect(&mut self, doc: DocId, score: Score) {
            let doc_address = DocAddress::new(self.segment_ord, doc);
            self.fruit.push((
                SearchIndexScore::new(&self.ctid_ff, doc, score),
                doc_address,
            ))
        }

        fn harvest(mut self) -> Self::Fruit {
            if self.sort_by_ctid {
                self.fruit.sort_by_key(|(scored, _)| scored.ctid);
            }

            // if send fails that likely means the receiver was dropped so we have nowhere
            // to send the result.  That's okay
            self.sender.send(self.fruit).ok();
        }
    }
}

mod vec_collector {
    use crate::index::reader::SearchIndexScore;
    use tantivy::collector::{Collector, SegmentCollector};
    use tantivy::fastfield::Column;
    use tantivy::{DocAddress, DocId, Score, SegmentOrdinal, SegmentReader};

    /// A [`Collector`] that collects all matching documents into a [`Vec`].  
    pub struct VecCollector {
        need_scores: bool,
    }

    impl VecCollector {
        pub fn new(need_scores: bool) -> Self {
            Self { need_scores }
        }
    }

    impl Collector for VecCollector {
        type Fruit = Vec<Vec<(SearchIndexScore, DocAddress)>>;
        type Child = VecSegmentCollector;

        fn for_segment(
            &self,
            segment_local_id: SegmentOrdinal,
            segment_reader: &SegmentReader,
        ) -> tantivy::Result<Self::Child> {
            Ok(VecSegmentCollector {
                segment_ord: segment_local_id,
                results: Default::default(),
                ctid_ff: segment_reader
                    .fast_fields()
                    .u64("ctid")
                    .expect("ctid should be a u64 fast field"),
            })
        }

        fn requires_scoring(&self) -> bool {
            self.need_scores
        }

        fn merge_fruits(
            &self,
            segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
        ) -> tantivy::Result<Self::Fruit> {
            // NB:  we never call this function, but best to implement it anyways
            Ok(segment_fruits)
        }
    }

    pub struct VecSegmentCollector {
        segment_ord: SegmentOrdinal,
        results: Vec<(SearchIndexScore, DocAddress)>,
        ctid_ff: Column<u64>,
    }

    impl SegmentCollector for VecSegmentCollector {
        type Fruit = Vec<(SearchIndexScore, DocAddress)>;

        fn collect(&mut self, doc: DocId, score: Score) {
            let doc_address = DocAddress::new(self.segment_ord, doc);
            self.results.push((
                SearchIndexScore::new(&self.ctid_ff, doc, score),
                doc_address,
            ));
        }

        fn harvest(self) -> Self::Fruit {
            self.results
        }
    }
}
