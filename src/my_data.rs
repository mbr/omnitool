use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{RecordBatch, StringBuilder, UInt32Builder},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{TableProvider, TableType},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        memory::MemoryStream,
    },
};

#[derive(Debug)]
pub struct MyDataSource {
    /// The schema for our only table.
    schema: SchemaRef,
}

impl Default for MyDataSource {
    fn default() -> Self {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::UInt32, false),
        ]);

        Self {
            schema: Arc::new(schema),
        }
    }
}

#[async_trait]
impl TableProvider for MyDataSource {
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

        Ok(Arc::new(MyExecPlan::new(self.schema.clone())))
    }
}

#[derive(Debug)]
struct MyExecPlan {
    properties: PlanProperties,
}

impl MyExecPlan {
    fn new(schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self { properties }
    }
}

impl DisplayAs for MyExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for MyExecPlan {
    fn name(&self) -> &str {
        "MyExecPlan"
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

        let mut name_col = StringBuilder::new();
        let mut count_col = UInt32Builder::new();

        // Insert data:
        name_col.append_value("INBOX");
        count_col.append_value(123);

        let record_batch = RecordBatch::try_new(
            self.schema(),
            vec![Arc::new(name_col.finish()), Arc::new(count_col.finish())],
        )?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![record_batch],
            self.schema(),
            None,
        )?))
    }
}
