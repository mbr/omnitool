use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{BooleanBuilder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, TryStreamExt};

use crate::imap::ImapPool;

/// A datasource for imap mailboxes.
#[derive(Debug)]
pub struct ImapMailboxesDataSource {
    /// The schema for our only table.
    schema: SchemaRef,
    /// Connection pool for IMAP.
    pool: Arc<ImapPool>,
}

impl ImapMailboxesDataSource {
    pub fn new(pool: Arc<ImapPool>) -> Self {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            // RFC 3501 attributes
            Field::new("no_inferiors", DataType::Boolean, false),
            Field::new("no_select", DataType::Boolean, false),
            Field::new("marked", DataType::Boolean, true),
            // RFC 5258 attributes
            Field::new("has_children", DataType::Boolean, true),
            // RFC 6154 attributes
            Field::new("all", DataType::Boolean, false),
            Field::new("archive", DataType::Boolean, false),
            Field::new("drafts", DataType::Boolean, false),
            Field::new("flagged", DataType::Boolean, false),
            Field::new("junk", DataType::Boolean, false),
            Field::new("sent", DataType::Boolean, false),
            Field::new("trash", DataType::Boolean, false),
            // RFC 8457 attributes
            Field::new("important", DataType::Boolean, false),
            Field::new("subscribed", DataType::Boolean, false),
            Field::new("non_existent", DataType::Boolean, false),
            Field::new("remote", DataType::Boolean, false),
        ]);

        Self {
            schema: Arc::new(schema),
            pool,
        }
    }
}

#[async_trait]
impl TableProvider for ImapMailboxesDataSource {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        dbg!(projection);
        dbg!(filters);
        dbg!(limit);

        Ok(Arc::new(ImapExecPlan::new(
            self.schema.clone(),
            self.pool.clone(),
        )))
    }
}

#[derive(Debug)]
struct ImapExecPlan {
    properties: PlanProperties,
    pool: Arc<ImapPool>,
}

impl ImapExecPlan {
    fn new(schema: SchemaRef, pool: Arc<ImapPool>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self { properties, pool }
    }
}

impl DisplayAs for ImapExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for ImapExecPlan {
    fn name(&self) -> &str {
        "ImapExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        let pool = self.pool.clone();
        let name_stream = futures::stream::once(async move {
            let mut imap_session = pool
                .get()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            // let mut imap_session = df_get_imap_session(pool).await?;

            // Sadly we have to buffer them all here, since `.list` takes self by reference.
            let name_results: Vec<_> = imap_session
                .list(None, Some("*"))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .collect()
                .await;

            drop(imap_session);

            Result::<_, DataFusionError>::Ok(futures::stream::iter(name_results))
        })
        .try_flatten();

        let self_schema = self.schema();
        let stream = name_stream.and_then(move |name| {
            let schema = self_schema.clone();
            async move {
                use async_imap::types::NameAttribute;

                let mut name_col = StringBuilder::new();
                // RFC 3501 attributes
                let mut noinferiors_col = BooleanBuilder::new();
                let mut noselect_col = BooleanBuilder::new();
                let mut marked_col = BooleanBuilder::new();
                // RFC 5258 attributes
                let mut has_children_col = BooleanBuilder::new();
                // RFC 6154 attributes
                let mut all_col = BooleanBuilder::new();
                let mut archive_col = BooleanBuilder::new();
                let mut drafts_col = BooleanBuilder::new();
                let mut flagged_col = BooleanBuilder::new();
                let mut junk_col = BooleanBuilder::new();
                let mut sent_col = BooleanBuilder::new();
                let mut trash_col = BooleanBuilder::new();
                // RFC 8457 attributes
                let mut important_col = BooleanBuilder::new();
                let mut subscribed_col = BooleanBuilder::new();
                let mut nonexistent_col = BooleanBuilder::new();
                let mut remote_col = BooleanBuilder::new();

                name_col.append_value(name.name());

                // Process attributes
                let mut has_noinferiors = false;
                let mut has_noselect = false;
                let mut marked = None;
                let mut has_children = None;
                let mut has_all = false;
                let mut has_archive = false;
                let mut has_drafts = false;
                let mut has_flagged = false;
                let mut has_junk = false;
                let mut has_sent = false;
                let mut has_trash = false;
                let mut has_important = false;
                let mut has_subscribed = false;
                let mut has_nonexistent = false;
                let mut has_remote = false;

                for attr in name.attributes() {
                    match attr {
                        NameAttribute::NoInferiors => has_noinferiors = true,
                        NameAttribute::NoSelect => has_noselect = true,
                        NameAttribute::Marked => marked = Some(true),
                        NameAttribute::Unmarked => marked = Some(false),
                        NameAttribute::All => has_all = true,
                        NameAttribute::Archive => has_archive = true,
                        NameAttribute::Drafts => has_drafts = true,
                        NameAttribute::Flagged => has_flagged = true,
                        NameAttribute::Junk => has_junk = true,
                        NameAttribute::Sent => has_sent = true,
                        NameAttribute::Trash => has_trash = true,
                        NameAttribute::Extension(s) => match s.as_ref() {
                            "\\HasChildren" => has_children = Some(true),
                            "\\HasNoChildren" => has_children = Some(false),
                            "\\Important" => has_important = true,
                            "\\Subscribed" => has_subscribed = true,
                            "\\NonExistent" => has_nonexistent = true,
                            "\\Remote" => has_remote = true,
                            _ => {}
                        },
                        _ => {}
                    }
                }

                // Append values to columns
                noinferiors_col.append_value(has_noinferiors);
                noselect_col.append_value(has_noselect);

                // marked column is nullable - true if Marked, false if Unmarked, null if neither
                match marked {
                    Some(v) => marked_col.append_value(v),
                    None => marked_col.append_null(),
                }

                // has_children is nullable
                match has_children {
                    Some(v) => has_children_col.append_value(v),
                    None => has_children_col.append_null(),
                }

                all_col.append_value(has_all);
                archive_col.append_value(has_archive);
                drafts_col.append_value(has_drafts);
                flagged_col.append_value(has_flagged);
                junk_col.append_value(has_junk);
                sent_col.append_value(has_sent);
                trash_col.append_value(has_trash);
                important_col.append_value(has_important);
                subscribed_col.append_value(has_subscribed);
                nonexistent_col.append_value(has_nonexistent);
                remote_col.append_value(has_remote);

                let record_batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(name_col.finish()),
                        Arc::new(noinferiors_col.finish()),
                        Arc::new(noselect_col.finish()),
                        Arc::new(marked_col.finish()),
                        Arc::new(has_children_col.finish()),
                        Arc::new(all_col.finish()),
                        Arc::new(archive_col.finish()),
                        Arc::new(drafts_col.finish()),
                        Arc::new(flagged_col.finish()),
                        Arc::new(junk_col.finish()),
                        Arc::new(sent_col.finish()),
                        Arc::new(trash_col.finish()),
                        Arc::new(important_col.finish()),
                        Arc::new(subscribed_col.finish()),
                        Arc::new(nonexistent_col.finish()),
                        Arc::new(remote_col.finish()),
                    ],
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                Result::<_, DataFusionError>::Ok(record_batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
