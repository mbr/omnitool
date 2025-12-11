use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{ListBuilder, RecordBatch, StringBuilder},
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
            Field::new("separator", DataType::Utf8, true),
            Field::new(
                "flags",
                DataType::List(Arc::new(Field::new("flag", DataType::Utf8, false))),
                false,
            ),
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
                let mut name_col = StringBuilder::new();
                let mut separator_col = StringBuilder::new();
                let flags_field = Field::new("flag", DataType::Utf8, false);
                let mut flags_col =
                    ListBuilder::new(StringBuilder::new()).with_field(flags_field.clone());

                name_col.append_value(name.name());

                // Append separator
                match name.delimiter() {
                    Some(delim) => separator_col.append_value(delim),
                    None => separator_col.append_null(),
                }

                for attr in name.attributes() {
                    flags_col.values().append_value(format!("{:?}", attr));
                }
                flags_col.append(true);

                let record_batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(name_col.finish()),
                        Arc::new(separator_col.finish()),
                        Arc::new(flags_col.finish()),
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
