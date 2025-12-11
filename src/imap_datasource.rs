use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{RecordBatch, StringBuilder, UInt32Builder},
    datatypes::{DataType, Field, Schema},
};
use async_imap::types::Name;
use async_trait::async_trait;
use bb8::PooledConnection;
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
        memory::MemoryStream,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};

use crate::imap::{ImapConMan, ImapPool, ImapSession};

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
            Field::new("count", DataType::UInt32, false),
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

    //
    // let pool = self.pool.clone();
    // let connect_and_query = futures::stream::once(async move {
    //     let mut imap_session = pool
    //         .get()
    //         .await
    //         .map_err(|e| DataFusionError::External(Box::new(e)))?;
    //
    //     // Sadly we have to buffer them all here.
    //     let name_results: Vec<_> = imap_session
    //         .list(None, Some("*"))
    //         .await
    //         .map_err(|e| DataFusionError::External(Box::new(e)))?
    //         .collect()
    //         .await;
    //
    //     Ok(futures::stream::iter(name_results))
    // })
    // .try_flatten();

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        // `name_stream` is a future that returns a Result<impl Stream, DataFusionError>
        let names_fut =
            df_get_imap_session(self.pool.clone()).and_then(|mut imap_session| async move {
                let name_stream = imap_session
                    .list(None, Some("*"))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let name_stream_df_error =
                    name_stream.map_err(|e| DataFusionError::External(Box::new(e)));

                Ok(name_stream_df_error)
            });

        // Turn into nested streams using once, then flatten.
        let name_stream = TryStreamExt::try_flatten(futures::stream::once(names_fut));

        let stream = name_stream.and_then(|_name| async {
            let mut name_col = StringBuilder::new();
            let mut count_col = UInt32Builder::new();

            name_col.append_value("INBOX");
            count_col.append_value(123);

            let record_batch = RecordBatch::try_new(
                self.schema(),
                vec![Arc::new(name_col.finish()), Arc::new(count_col.finish())],
            )?;

            Ok(record_batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

async fn df_get_imap_session(
    pool: Arc<ImapPool>,
) -> Result<PooledConnection<'static, ImapConMan>, DataFusionError> {
    pool.get_owned()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
