use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, ListBuilder, RecordBatch, StringBuilder, UInt32Builder},
    datatypes::{DataType, Field, Schema},
};
use async_imap::types::Name;
use async_trait::async_trait;
use bb8::PooledConnection;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{SchemaProvider, Session},
    common::Result as DfResult,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{Stream, TryFutureExt, TryStreamExt};

use crate::imap::{ImapConMan, ImapPool, ImapSession};

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

#[derive(Debug)]
pub struct MailboxSchemaProvider {
    /// Connection pool for IMAP.
    pool: Arc<ImapPool>,
    /// A handle to the runtime to execute code on in sync function.
    rt_handle: tokio::runtime::Handle,
    /// Schema for a single mailbox.
    schema: SchemaRef,
}

#[derive(Debug)]
pub struct ImapSingleMailboxTableProvider {
    /// Schema for this mailbox.
    schema: SchemaRef,
    /// Connection pool for IMAP.
    pool: Arc<ImapPool>,
    /// Name of the mailbox.
    mailbox_name: String,
}

impl MailboxSchemaProvider {
    /// Constructs a new [`MailboxSchemaProvider`].
    pub async fn new(
        rt_handle: tokio::runtime::Handle,
        pool: Arc<ImapPool>,
    ) -> Result<Self, DataFusionError> {
        use arrow::datatypes::TimeUnit;

        let schema = Schema::new(vec![
            // Identifiers
            Field::new("uid", DataType::UInt32, false),
            Field::new("message_id", DataType::Utf8, true),
            Field::new("thread_id", DataType::Utf8, true),
            Field::new(
                "references",
                DataType::List(Arc::new(Field::new("ref", DataType::Utf8, false))),
                true,
            ),
            // Timestamps
            Field::new("date", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "internal_date",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            // Addresses
            Field::new("from", DataType::Utf8, true),
            Field::new(
                "to",
                DataType::List(Arc::new(Field::new("addr", DataType::Utf8, false))),
                true,
            ),
            Field::new(
                "cc",
                DataType::List(Arc::new(Field::new("addr", DataType::Utf8, false))),
                true,
            ),
            Field::new(
                "bcc",
                DataType::List(Arc::new(Field::new("addr", DataType::Utf8, false))),
                true,
            ),
            Field::new("reply_to", DataType::Utf8, true),
            // Content
            Field::new("subject", DataType::Utf8, true),
            Field::new("size", DataType::UInt64, false),
            // Flags
            Field::new(
                "flags",
                DataType::List(Arc::new(Field::new("flag", DataType::Utf8, false))),
                false,
            ),
            // Structure
            Field::new("has_attachments", DataType::Boolean, true),
            Field::new("attachment_count", DataType::UInt32, true),
            Field::new("content_type", DataType::Utf8, true),
            // Mailing Lists & Organization
            Field::new("list_id", DataType::Utf8, true),
            Field::new("precedence", DataType::Utf8, true),
            // Optional/Extended
            Field::new("importance", DataType::Utf8, true),
            Field::new("delivered_to", DataType::Utf8, true),
        ]);

        Ok(Self {
            schema: Arc::new(schema),
            pool,
            rt_handle,
        })
    }
}

#[async_trait]
impl SchemaProvider for MailboxSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let pool = self.pool.clone();
        self.rt_handle.block_on(async move {
            // This is less-than-idea, since we cannot refresh tables names and report an error.
            let mut imap_session = get_con(&pool).await.expect("failed to get table names");
            let mut table_names: Vec<_> = list_mailboxes(&mut imap_session, "*")
                .await
                .map_ok(|name| name.name().to_owned())
                .try_collect()
                .await
                .expect("read error getting table names");

            // Sort, since we'll be using `binary_search_by_key` to find tables.
            table_names.sort();

            table_names
        })
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(Some(Arc::new(ImapSingleMailboxTableProvider {
            schema: self.schema.clone(),
            pool: self.pool.clone(),
            mailbox_name: name.to_owned(),
        })))
    }

    fn table_exist(&self, name: &str) -> bool {
        // TODO: Actually look up using IMAP session instead.
        self.table_names()
            .binary_search_by_key(&name, |s| s.as_str())
            .is_ok()
    }
}

impl TableProvider for ImapSingleMailboxTableProvider {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        todo!()
    }
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
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut source_level_filters = Vec::with_capacity(filters.len());
        for expr in filters {
            source_level_filters.push(SourceLevelFilter::try_from_expr(expr).ok_or_else(|| {
                DataFusionError::Internal(format!("unexpected filter pushdown of {}", expr))
            })?)
        }

        let projected_schema = if let Some(ref projection) = projection {
            Arc::new(self.schema().project(projection)?)
        } else {
            self.schema()
        };

        Ok(Arc::new(ImapExecPlan::new(
            projected_schema,
            self.pool.clone(),
            limit,
            Arc::from(source_level_filters.into_boxed_slice()),
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .into_iter()
            .map(|expr| {
                SourceLevelFilter::try_from_expr(expr)
                    .map(|slf| slf.push_down_kind())
                    .unwrap_or(TableProviderFilterPushDown::Unsupported)
            })
            .collect())
    }
}

/// Filters at the source level.
///
/// Source level filters are expressed in the query or directly applied after loading.
///
/// Filters implement ordering by their estimated potential of reducing cardinality.
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SourceLevelFilter {
    /// A `name LIKE` or `name ILIKE` condition.
    NameLike {
        pattern: String,
        negated: bool,
        case_insensitive: bool,
    },
    /// A `name = 'value'` or `name != 'value'` condition.
    NameEqual { value: String, negated: bool },
}

impl Display for SourceLevelFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceLevelFilter::NameLike {
                negated,
                case_insensitive,
                pattern,
            } => {
                let not_str = if *negated { "NOT " } else { "" };
                if *case_insensitive {
                    write!(f, "name {}ILIKE {}", not_str, pattern)
                } else {
                    write!(f, "name {}LIKE {}", not_str, pattern)
                }
            }
            SourceLevelFilter::NameEqual { negated, value } => {
                let op = if *negated { "!=" } else { "=" };
                write!(f, "name {} '{}'", op, value)
            }
        }
    }
}

impl SourceLevelFilter {
    /// Tries to create a new [`SourceLevelFilter`] from a given [`Expr`].
    ///
    /// Returns `None` if the specific expression is not supported.
    fn try_from_expr(expr: &Expr) -> Option<SourceLevelFilter> {
        match expr {
            Expr::Like(like) => {
                // Reject if escape_char is set
                if like.escape_char.is_some() {
                    return None;
                }

                if let Expr::Column(col) = like.expr.as_ref()
                    && col.name() == "name"
                {
                    if let Expr::Literal(scalar, _) = like.pattern.as_ref() {
                        if let Some(Some(val)) = scalar.try_as_str() {
                            return Some(SourceLevelFilter::NameLike {
                                negated: like.negated,
                                case_insensitive: like.case_insensitive,
                                pattern: val.to_owned(),
                            });
                        }
                    }
                }
                None
            }
            Expr::BinaryExpr(binary_expr) => {
                use datafusion::logical_expr::Operator;

                // Check if it's an equality or inequality operator
                let negated = match binary_expr.op {
                    Operator::Eq => false,
                    Operator::NotEq => true,
                    _ => return None,
                };

                // Check if left side is the name column
                if let Expr::Column(col) = binary_expr.left.as_ref()
                    && col.name() == "name"
                {
                    // Check if right side is a literal string
                    if let Expr::Literal(scalar, _) = binary_expr.right.as_ref() {
                        if let Some(Some(val)) = scalar.try_as_str() {
                            return Some(SourceLevelFilter::NameEqual {
                                negated,
                                value: val.to_owned(),
                            });
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Returns the type of filter push down supported by this filter.
    ///
    /// Used to communicate whether DataFusion will have to re-apply filtering later on.
    fn push_down_kind(&self) -> TableProviderFilterPushDown {
        match self {
            SourceLevelFilter::NameLike { .. } => TableProviderFilterPushDown::Exact,
            SourceLevelFilter::NameEqual { .. } => TableProviderFilterPushDown::Exact,
        }
    }

    /// Applies the source level filter to a given name.
    ///
    /// Returns `true` if the filter either matches or was not applied.
    fn matches_name_column(&self, name: &str) -> DfResult<bool> {
        match self {
            SourceLevelFilter::NameLike {
                negated,
                case_insensitive,
                pattern,
            } => {
                let matches = like_match(name, pattern, *case_insensitive)?;
                Ok(if *negated { !matches } else { matches })
            }
            SourceLevelFilter::NameEqual { negated, value } => {
                let matches = name == value;
                Ok(if *negated { !matches } else { matches })
            }
        }
    }

    /// Returns, if applicable, an IMAP `LIST` search pattern for this filter.
    ///
    /// The returned query pattern is inexact.
    fn query_string(&self) -> Option<String> {
        match self {
            SourceLevelFilter::NameLike {
                pattern,
                negated,
                case_insensitive,
            } if !negated && !case_insensitive => sql_pattern_to_imap(pattern),
            SourceLevelFilter::NameEqual { value, negated } if !negated => Some(value.clone()),
            _ => None,
        }
    }
}

/// Converts a SQL LIKE pattern to an IMAP LIST pattern.
///
/// The returned search pattern may be more inclusive than the original.
fn sql_pattern_to_imap(pattern: &str) -> Option<String> {
    let imap_pattern = pattern.replace('%', "*").replace('_', "*");
    Some(imap_pattern)
}

/// Applies SQL LIKE pattern matching between a value and a pattern.
///
/// Returns true if the value matches the pattern, false otherwise.
pub fn like_match(
    value: &str,
    pattern: &str,
    case_insensitive: bool,
) -> Result<bool, arrow::error::ArrowError> {
    let value_array = arrow::array::StringArray::from(vec![value]);
    let pattern_array = arrow::array::StringArray::from(vec![pattern]);

    let result: arrow::array::BooleanArray = if case_insensitive {
        arrow::compute::kernels::comparison::ilike(&value_array, &pattern_array)?
    } else {
        arrow::compute::kernels::comparison::like(&value_array, &pattern_array)?
    };

    Ok(result.value(0))
}

#[derive(Debug)]
struct ImapExecPlan {
    properties: PlanProperties,
    pool: Arc<ImapPool>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    source_level_filters: Arc<[SourceLevelFilter]>,
}

impl ImapExecPlan {
    fn new(
        projected_schema: SchemaRef,
        pool: Arc<ImapPool>,
        limit: Option<usize>,
        source_level_filters: Arc<[SourceLevelFilter]>,
    ) -> DfResult<Self> {
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
            source_level_filters,
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
                    "ImapExecPlan: must_example={} limit={} [",
                    self.must_examine(),
                    limit
                )?;
                for (idx, filter) in self.source_level_filters.iter().enumerate() {
                    if idx != 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", filter)?;
                }
                f.write_str("]")?;
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "ImapExecPlan\nmust_examine={}\nlimit={}",
                    self.must_examine(),
                    limit
                )?;
                for filter in self.source_level_filters.iter() {
                    write!(f, "\n{}", filter)?;
                }
                Ok(())
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
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        let stream = futures::stream::once(self.build_batch());

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        // Likely has no effect, since there are no inputs to this node.
        true
    }
}

impl ImapExecPlan {
    fn build_batch(&self) -> impl Future<Output = DfResult<RecordBatch>> + 'static {
        let pool = self.pool.clone();
        let projected_schema = self.projected_schema.clone();
        let must_examine = self.must_examine();
        let mut limit = self.limit.unwrap_or(usize::MAX);
        let source_level_filters = self.source_level_filters.clone();

        async move {
            let mut name_col = StringBuilder::new();
            let mut separator_col = StringBuilder::new();
            let flags_field = Field::new("flag", DataType::Utf8, false);
            let mut flags_col =
                ListBuilder::new(StringBuilder::new()).with_field(flags_field.clone());
            let mut exists_col = UInt32Builder::new();
            let mut recent_col = UInt32Builder::new();
            let mut unseen_col = UInt32Builder::new();

            // Determine the most restrictive IMAP LIST pattern from filters
            // Use the existing PartialOrd implementation to find the most restrictive filter
            let imap_pattern = source_level_filters
                .iter()
                .max()
                .and_then(SourceLevelFilter::query_string)
                .unwrap_or_else(|| "*".to_string());

            let mut imap_session = get_con(&pool).await?;

            let all_names: Vec<_> = list_mailboxes(&mut imap_session, &imap_pattern)
                .await
                .try_collect()
                .await?;

            let mut mailbox_names = Vec::new();
            for name in all_names {
                let mut should_skip = false;
                for filter in source_level_filters.iter() {
                    if !filter.matches_name_column(name.name())? {
                        should_skip = true;
                    }
                }
                if should_skip {
                    continue;
                }

                if limit == 0 {
                    break;
                }
                limit -= 1;

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

/// Fetches an IMAP connection from the connection, with the error wrapped in a [`datafusion`]
/// friendly way.
async fn get_con(pool: &ImapPool) -> DfResult<PooledConnection<'_, ImapConMan>> {
    pool.get()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// Returns a stream of mailboxes matching a certain pattern.
async fn list_mailboxes(
    session: &mut ImapSession,
    pattern: &str,
) -> impl Stream<Item = DfResult<Name>> {
    futures::stream::once(
        session
            .list(None, Some(pattern))
            .map_err(|e| DataFusionError::External(Box::new(e))),
    )
    .map_ok(|name_results| name_results.map_err(|e| DataFusionError::External(Box::new(e))))
    .try_flatten()
}
