//! Contains the IOx query engine
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder, ChunkSummary},
    delete_predicate::DeletePredicate,
    partition_metadata::{InfluxDbType, PartitionAddr, TableSummary},
};
use datafusion::physical_plan::SendableRecordBatchStream;
use exec::stringset::StringSet;
use observability_deps::tracing::{debug, trace};
use predicate::{
    predicate::{Predicate, PredicateMatch},
    rpc_predicate::QueryDatabaseMeta,
};
use schema::selection::Selection;
use schema::{sort::SortKey, Schema, TIME_COLUMN_NAME};

use hashbrown::HashMap;
use std::{collections::BTreeSet, fmt::Debug, iter::FromIterator, sync::Arc};

pub mod exec;
pub mod frontend;
pub mod func;
pub mod group_by;
pub mod plan;
pub mod provider;
pub mod pruning;
pub mod statistics;
pub mod util;

pub use exec::context::{DEFAULT_CATALOG, DEFAULT_SCHEMA};

/// Trait for an object (designed to be a Chunk) which can provide
/// metadata
pub trait QueryChunkMeta: Sized {
    /// Return a reference to the summary of the data
    fn summary(&self) -> Option<&TableSummary>;

    /// return a reference to the summary of the data held in this chunk
    fn schema(&self) -> Arc<Schema>;

    /// return a reference to delete predicates of the chunk
    fn delete_predicates(&self) -> &[Arc<DeletePredicate>];

    /// return true if the chunk has delete predicates
    fn has_delete_predicates(&self) -> bool {
        !self.delete_predicates().is_empty()
    }

    /// return column names participating in the all delete predicates
    /// in lexicographical order with one exception that time column is last
    /// This order is to be consistent with Schema::primary_key
    fn delete_predicate_columns(&self) -> Vec<&str> {
        // get all column names but time
        let mut col_names = BTreeSet::new();
        for pred in self.delete_predicates() {
            for expr in &pred.exprs {
                if expr.column != schema::TIME_COLUMN_NAME {
                    col_names.insert(expr.column.as_str());
                }
            }
        }

        // convert to vector
        let mut column_names = Vec::from_iter(col_names);

        // Now add time column to the end of the vector
        // Since time range is a must in the delete predicate, time column must be in this list
        column_names.push(TIME_COLUMN_NAME);

        column_names
    }
}

/// A `QueryCompletedToken` is returned by `record_query` implementations of
/// a `QueryDatabase`. It is used to trigger side-effects (such as query timing)
/// on query completion.
pub struct QueryCompletedToken<'a> {
    f: Option<Box<dyn FnOnce() + Send + 'a>>,
}

impl<'a> Debug for QueryCompletedToken<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCompletedToken").finish()
    }
}

impl<'a> QueryCompletedToken<'a> {
    pub fn new(f: impl FnOnce() + Send + 'a) -> Self {
        Self {
            f: Some(Box::new(f)),
        }
    }
}

impl<'a> Drop for QueryCompletedToken<'a> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            (f)()
        }
    }
}

/// A `Database` is the main trait implemented by the IOx subsystems
/// that store actual data.
///
/// Databases store data organized by partitions and each partition stores
/// data in Chunks.
pub trait QueryDatabase: QueryDatabaseMeta + Debug + Send + Sync {
    type Chunk: QueryChunk;

    /// Return the partition keys for data in this DB
    fn partition_addrs(&self) -> Vec<PartitionAddr>;

    /// Returns a set of chunks within the partition with data that may match
    /// the provided predicate. If possible, chunks which have no rows that can
    /// possibly match the predicate may be omitted.
    fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<Self::Chunk>>;

    /// Return a summary of all chunks in this database, in all partitions
    fn chunk_summaries(&self) -> Vec<ChunkSummary>;

    /// Record that particular type of query was run / planned
    fn record_query(
        &self,
        query_type: impl Into<String>,
        query_text: impl Into<String>,
    ) -> QueryCompletedToken<'_>;
}

/// Collection of data that shares the same partition key
pub trait QueryChunk: QueryChunkMeta + Debug + Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// returns the Id of this chunk. Ids are unique within a
    /// particular partition.
    fn id(&self) -> ChunkId;

    /// Returns the ChunkAddr of this chunk
    fn addr(&self) -> ChunkAddr;

    /// Returns the name of the table stored in this chunk
    fn table_name(&self) -> &str;

    /// Returns true if the chunk may contain a duplicate "primary
    /// key" within itself
    fn may_contain_pk_duplicates(&self) -> bool;

    /// Returns the result of applying the `predicate` to the chunk
    /// using an efficient, but inexact method, based on metadata.
    ///
    /// NOTE: This method is suitable for calling during planning, and
    /// may return PredicateMatch::Unknown for certain types of
    /// predicates.
    fn apply_predicate_to_metadata(
        &self,
        predicate: &Predicate,
    ) -> Result<PredicateMatch, Self::Error>;

    /// Returns a set of Strings with column names from the specified
    /// table that have at least one row that matches `predicate`, if
    /// the predicate can be evaluated entirely on the metadata of
    /// this Chunk. Returns `None` otherwise
    fn column_names(
        &self,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Return a set of Strings containing the distinct values in the
    /// specified columns. If the predicate can be evaluated entirely
    /// on the metadata of this Chunk. Returns `None` otherwise
    ///
    /// The requested columns must all have String type.
    fn column_values(
        &self,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error>;

    /// Provides access to raw `QueryChunk` data as an
    /// asynchronous stream of `RecordBatch`es filtered by a *required*
    /// predicate. Note that not all chunks can evaluate all types of
    /// predicates and this function will return an error
    /// if requested to evaluate with a predicate that is not supported
    ///
    /// This is the analog of the `TableProvider` in DataFusion
    ///
    /// The reason we can't simply use the `TableProvider` trait
    /// directly is that the data for a particular Table lives in
    /// several chunks within a partition, so there needs to be an
    /// implementation of `TableProvider` that stitches together the
    /// streams from several different `QueryChunk`s.
    fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error>;

    /// Returns true if data of this chunk is sorted
    fn is_sorted_on_pk(&self) -> bool;

    /// Returns the sort key of the chunk if any
    fn sort_key(&self) -> Option<SortKey<'_>>;

    /// Returns chunk type which is either MUB, RUB, OS
    fn chunk_type(&self) -> &str;

    /// Order of this chunk relative to other overlapping chunks.
    fn order(&self) -> ChunkOrder;
}

/// Implement ChunkMeta for something wrapped in an Arc (like Chunks often are)
impl<P> QueryChunkMeta for Arc<P>
where
    P: QueryChunkMeta,
{
    fn summary(&self) -> Option<&TableSummary> {
        self.as_ref().summary()
    }

    fn schema(&self) -> Arc<Schema> {
        self.as_ref().schema()
    }

    fn delete_predicates(&self) -> &[Arc<DeletePredicate>] {
        let pred = self.as_ref().delete_predicates();
        debug!(?pred, "Delete predicate in QueryChunkMeta");
        pred
    }
}

/// return true if all the chunks inlcude statistics
pub fn chunks_have_stats<C>(chunks: &[C]) -> bool
where
    C: QueryChunkMeta,
{
    // If at least one of the provided chunk cannot provide stats,
    // do not need to compute potential duplicates. We will treat
    // as all of them have duplicates
    chunks.iter().all(|c| c.summary().is_some())
}

pub fn compute_sort_key_for_chunks<'a, C>(schema: &'a Schema, chunks: &'a [C]) -> SortKey<'a>
where
    C: QueryChunkMeta,
{
    if !chunks_have_stats(chunks) {
        // chunks have not enough stats, return its  pk that is
        // sorted lexicographically but time column always last
        let pk = schema.primary_key();
        let mut sort_key = SortKey::with_capacity(pk.len());
        for col in pk {
            sort_key.push(col, Default::default())
        }
        sort_key
    } else {
        let summaries = chunks
            .iter()
            .map(|x| x.summary().expect("Chunk should have summary"));
        compute_sort_key(summaries)
    }
}

/// Compute a sort key that orders lower cardinality columns first
///
/// In the absence of more precise information, this should yield a
/// good ordering for RLE compression
pub fn compute_sort_key<'a>(summaries: impl Iterator<Item = &'a TableSummary>) -> SortKey<'a> {
    let mut cardinalities: HashMap<&str, u64> = Default::default();
    for summary in summaries {
        for column in &summary.columns {
            if column.influxdb_type != Some(InfluxDbType::Tag) {
                continue;
            }

            let mut cnt = 0;
            if let Some(count) = column.stats.distinct_count() {
                cnt = count.get();
            }
            *cardinalities.entry(column.name.as_str()).or_default() += cnt;
        }
    }

    trace!(cardinalities=?cardinalities, "cardinalities of of columns to compute sort key");

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    // Sort by (cardinality, column_name) to have deterministic order if same cardinality
    cardinalities.sort_by_key(|x| (x.1, x.0));

    let mut key = SortKey::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        key.push(col, Default::default())
    }
    key.push(TIME_COLUMN_NAME, Default::default());

    trace!(computed_sort_key=?key, "Value of sort key from compute_sort_key");

    key
}

// Note: I would like to compile this module only in the 'test' cfg,
// but when I do so then other modules can not find them. For example:
//
// error[E0433]: failed to resolve: could not find `test` in `storage`
//   --> src/server/mutable_buffer_routes.rs:353:19
//     |
// 353 |     use query::test::TestDatabaseStore;
//     |                ^^^^ could not find `test` in `query`

//
//#[cfg(test)]
pub mod test;
