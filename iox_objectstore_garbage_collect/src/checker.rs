use chrono::{DateTime, Utc};
use iox_catalog::interface::ParquetFileRepo;
use object_store::ObjectMeta;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Could not create the catalog"))]
    CreatingCatalog {
        source: clap_blocks::catalog_dsn::Error,
    },

    #[snafu(display("Expected a file name"))]
    FileNameMissing,

    #[snafu(display(r#""{uuid}" is not a valid ID"#))]
    MalformedId { source: uuid::Error, uuid: String },

    #[snafu(display("The catalog could not be queried for {object_store_id}"))]
    GetFile {
        source: iox_catalog::interface::Error,
        object_store_id: uuid::Uuid,
    },

    #[snafu(display("The deleter task exited unexpectedly"))]
    DeleterExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn perform(
    args: Arc<crate::Args>,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let catalog = args.catalog().await.context(CreatingCatalogSnafu)?;
    let cutoff = args.cutoff();

    let mut repositories = catalog.repositories().await;
    let parquet_files = repositories.parquet_files();

    while let Some(item) = items.recv().await {
        if should_delete(&item, cutoff, parquet_files).await? {
            deleter.send(item).await.context(DeleterExitedSnafu)?;
        }
    }

    Ok(())
}

async fn should_delete(
    item: &ObjectMeta,
    cutoff: DateTime<Utc>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Result<bool> {
    if cutoff < item.last_modified {
        info!(
            location = %item.location,
            deleting = false,
            reason = "too new",
            cutoff = %cutoff,
            last_modified = %item.last_modified,
        );
        // Not old enough; do not delete
        return Ok(false);
    }

    let file_name = item.location.parts().last().context(FileNameMissingSnafu)?;

    if let Some(uuid) = file_name.as_ref().strip_suffix(".parquet") {
        if let Ok(object_store_id) = uuid.parse() {
            let parquet_file = parquet_files
                .get_by_object_store_id(object_store_id)
                .await
                .context(GetFileSnafu { object_store_id })?;

            if parquet_file.is_some() {
                info!(
                    location = %item.location,
                    deleting = false,
                    reason = "exists in catalog",
                );
                // We have a reference to this file; do not delete
                return Ok(false);
            } else {
                info!(
                    location = %item.location,
                    deleting = true,
                    reason = "not in catalog",
                );
            }
        } else {
            info!(
                location = %item.location,
                deleting = true,
                uuid,
                reason = "not a valid UUID",
            );
        }
    } else {
        info!(
            location = %item.location,
            deleting = true,
            file_name = %file_name.as_ref(),
            reason = "not a .parquet file",
        );
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use data_types::{
        ColumnId, ColumnSet, KafkaPartition, NamespaceId, ParquetFile, ParquetFileParams,
        PartitionId, SequenceNumber, SequencerId, TableId, Timestamp, INITIAL_COMPACTION_LEVEL,
    };
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use object_store::path::Path;
    use once_cell::sync::Lazy;
    use parquet_file::ParquetFilePath;
    use uuid::Uuid;

    static OLDER_TIME: Lazy<DateTime<Utc>> =
        Lazy::new(|| Utc.datetime_from_str("2022-01-01T00:00:00z", "%+").unwrap());
    static NEWER_TIME: Lazy<DateTime<Utc>> =
        Lazy::new(|| Utc.datetime_from_str("2022-02-02T00:00:00z", "%+").unwrap());

    async fn test_catalog() -> (Arc<dyn Catalog>, ParquetFile) {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_parquet_file_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();
        let sequencer = repos
            .sequencers()
            .create_or_get(&kafka, KafkaPartition::new(1))
            .await
            .unwrap();
        let partition = repos
            .partitions()
            .create_or_get("one".into(), sequencer.id, table.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id: Uuid::new_v4(),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(140),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            row_count: 0,
            compaction_level: INITIAL_COMPACTION_LEVEL,
            created_at: Timestamp::new(1),
            column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        };

        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        (catalog, parquet_file)
    }

    #[tokio::test]
    async fn dont_delete_new_file_in_catalog() {
        let (catalog, file_in_catalog) = test_catalog().await;
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            file_in_catalog.sequencer_id,
            file_in_catalog.partition_id,
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
        };

        assert!(!should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn dont_delete_new_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            SequencerId::new(3),
            PartitionId::new(4),
            Uuid::new_v4(),
        )
        .object_store_path();

        let cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
        };

        assert!(!should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn dont_delete_new_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let cutoff = *OLDER_TIME;
        let last_modified = *NEWER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
        };

        assert!(!should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn dont_delete_old_file_in_catalog() {
        let (catalog, file_in_catalog) = test_catalog().await;
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            file_in_catalog.namespace_id,
            file_in_catalog.table_id,
            file_in_catalog.sequencer_id,
            file_in_catalog.partition_id,
            file_in_catalog.object_store_id,
        )
        .object_store_path();

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
        };

        assert!(!should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn delete_old_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let location = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            SequencerId::new(3),
            PartitionId::new(4),
            Uuid::new_v4(),
        )
        .object_store_path();

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
        };

        assert!(should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn delete_old_file_with_unparseable_path() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let cutoff = *NEWER_TIME;
        let last_modified = *OLDER_TIME;

        let item = ObjectMeta {
            location: Path::from("not-a-uuid.parquet"),
            last_modified,
            size: 0,
        };

        assert!(should_delete(&item, cutoff, parquet_files).await.unwrap());
    }
}
