use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{ArrayRef, ListBuilder, RecordBatch, StringBuilder, UInt32Builder},
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

/// Formats a NameAttribute as a plain string.
fn format_name_attribute<'a>(attr: &'a async_imap::types::NameAttribute<'a>) -> &'a str {
    use async_imap::types::NameAttribute;
    match attr {
        NameAttribute::NoInferiors => "NoInferiors",
        NameAttribute::NoSelect => "NoSelect",
        NameAttribute::Marked => "Marked",
        NameAttribute::Unmarked => "Unmarked",
        NameAttribute::All => "All",
        NameAttribute::Archive => "Archive",
        NameAttribute::Drafts => "Drafts",
        NameAttribute::Flagged => "Flagged",
        NameAttribute::Junk => "Junk",
        NameAttribute::Sent => "Sent",
        NameAttribute::Trash => "Trash",
        NameAttribute::Extension(s) => s.as_ref().strip_prefix('\\').unwrap_or(s.as_ref()),
        _ => "Unknown",
    }
}

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
            Field::new("exists", DataType::UInt32, true),
            Field::new("recent", DataType::UInt32, true),
            Field::new("unseen", DataType::UInt32, true),
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
        dbg!(filters);

        let projected_schema = if let Some(ref projection) = projection {
            Arc::new(self.schema().project(projection)?)
        } else {
            self.schema()
        };

        Ok(Arc::new(ImapExecPlan::new(
            projected_schema,
            self.pool.clone(),
            projection.map(ToOwned::to_owned),
            limit,
        )?))
    }
}

#[derive(Debug)]
struct ImapExecPlan {
    properties: PlanProperties,
    pool: Arc<ImapPool>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
}

impl ImapExecPlan {
    fn new(
        projected_schema: SchemaRef,
        pool: Arc<ImapPool>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> datafusion::common::Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            properties,
            pool,
            projected_schema,
            limit,
        })
    }
}

impl DisplayAs for ImapExecPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let limit = match self.limit {
            Some(limit) => limit.to_string(),
            None => String::from("None"),
        };
        match t {
            DisplayFormatType::Default => f.write_str("ImapExecPlan"),
            DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ImapExecPlan: must_example={} limit={}",
                    self.must_examine(),
                    limit
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "ImapExecPlan\nmust_examine={}\nlimit={}",
                    self.must_examine(),
                    limit
                )
            }
        }
    }
}

impl ImapExecPlan {
    /// Returns whether the query must examine mailboxes individually.
    fn must_examine(&self) -> bool {
        self.projected_schema.column_with_name("exists").is_some()
            || self.projected_schema.column_with_name("recent").is_some()
            || self.projected_schema.column_with_name("unseen").is_some()
    }
}

impl ExecutionPlan for ImapExecPlan {
    fn name(&self) -> &str {
        "ImapExecPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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

        let stream = futures::stream::once(self.build_batch());

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

impl ImapExecPlan {
    fn build_batch(
        &self,
    ) -> impl Future<Output = datafusion::common::Result<RecordBatch>> + 'static {
        let pool = self.pool.clone();
        let projected_schema = self.projected_schema.clone();
        let must_examine = self.must_examine();
        let limit = self.limit.unwrap_or(usize::MAX);

        async move {
            let mut name_col = StringBuilder::new();
            let mut separator_col = StringBuilder::new();
            let flags_field = Field::new("flag", DataType::Utf8, false);
            let mut flags_col =
                ListBuilder::new(StringBuilder::new()).with_field(flags_field.clone());
            let mut exists_col = UInt32Builder::new();
            let mut recent_col = UInt32Builder::new();
            let mut unseen_col = UInt32Builder::new();

            let mut imap_session = pool
                .get()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut name_results = imap_session
                .list(None, Some("*"))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .map_err(|e| DataFusionError::External(Box::new(e)))
                .take(limit);

            let mut mailbox_names = Vec::new();
            while let Some(name_result) = name_results.next().await {
                let name = name_result?;

                name_col.append_value(name.name());

                match name.delimiter() {
                    Some(delim) => separator_col.append_value(delim),
                    None => separator_col.append_null(),
                }

                for attr in name.attributes() {
                    flags_col.values().append_value(format_name_attribute(attr));
                }
                flags_col.append(true);

                mailbox_names.push(name.name().to_owned());
            }
            drop(name_results);

            for mailbox_name in mailbox_names {
                let examined = if must_examine {
                    imap_session.examine(&mailbox_name).await.ok()
                } else {
                    None
                };

                match examined {
                    Some(mailbox) => {
                        exists_col.append_value(mailbox.exists);
                        recent_col.append_value(mailbox.recent);
                        unseen_col.append_value(mailbox.unseen.unwrap_or(0));
                    }
                    None => {
                        exists_col.append_null();
                        recent_col.append_null();
                        unseen_col.append_null();
                    }
                }
            }

            let name_col = Arc::new(name_col.finish());
            let separator_col = Arc::new(separator_col.finish());
            let flags_col = Arc::new(flags_col.finish());
            let exists_col = Arc::new(exists_col.finish());
            let recent_col = Arc::new(recent_col.finish());
            let unseen_col = Arc::new(unseen_col.finish());

            let columns: Vec<ArrayRef> = projected_schema
                .fields
                .iter()
                .map(|f| {
                    let col: ArrayRef = match f.name().as_str() {
                        "name" => name_col.clone(),
                        "separator" => separator_col.clone(),
                        "flags" => flags_col.clone(),
                        "exists" => exists_col.clone(),
                        "recent" => recent_col.clone(),
                        "unseen" => unseen_col.clone(),
                        _ => unreachable!(),
                    };
                    col
                })
                .collect();

            let rb = RecordBatch::try_new(projected_schema, columns)?;

            Ok(rb)
        }
    }
}
