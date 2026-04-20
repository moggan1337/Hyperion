//! Hyperion Common Types
//! 
//! Core data structures and utilities shared across all modules.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};

/// Represents a SQL data type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    BigInt,
    Float,
    Double,
    Decimal(u8, u8),
    Varchar(usize),
    Boolean,
    Date,
    Timestamp,
    Null,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}

/// A column in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub ordinal: usize,
}

/// A table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub name: String,
}

impl Schema {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns }
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// A value in the query engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    BigInt(i128),
    Float(f64),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(i32),  // Days since epoch
    Timestamp(i64),  // Milliseconds since epoch
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

/// A row/tuple in the query engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Statistics about a dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics {
    pub row_count: u64,
    pub size_bytes: u64,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub histogram: Vec<HistogramBucket>,
}

/// Histogram bucket for column statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower_bound: Value,
    pub upper_bound: Value,
    pub count: u64,
}

impl Default for Statistics {
    fn default() -> Self {
        Self {
            row_count: 0,
            size_bytes: 0,
            null_count: 0,
            distinct_count: 0,
            min_value: None,
            max_value: None,
            histogram: Vec::new(),
        }
    }
}

/// Node identifier in the distributed cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

/// Location of a data partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionLocation {
    pub node_id: NodeId,
    pub partition_id: PartitionId,
    pub host: String,
    pub port: u16,
}

impl PartitionLocation {
    pub fn new(node_id: NodeId, partition_id: PartitionId, host: String, port: u16) -> Self {
        Self { node_id, partition_id, host, port }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Unique partition identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub String);

/// A distributed table with partition information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedTable {
    pub name: String,
    pub schema: Schema,
    pub partitions: Vec<PartitionLocation>,
    pub partitioning_scheme: PartitioningScheme,
}

/// How a table is partitioned across nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitioningScheme {
    Hash { column: String, num_partitions: usize },
    Range { column: String, boundaries: Vec<Value> },
    Broadcast,
    RoundRobin,
}

/// A reference to a distributed table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRef {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    pub fn new(name: String) -> Self {
        Self { catalog: None, schema: None, name }
    }

    pub fn full_name(&self) -> String {
        match (&self.catalog, &self.schema) {
            (Some(c), Some(s)) => format!("{}.{}.{}", c, s, self.name),
            (Some(c), None) => format!("{}.{}", c, self.name),
            _ => self.name.clone(),
        }
    }
}

/// Configuration for Hyperion engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub default_parallelism: usize,
    pub max_parallelism: usize,
    pub memory_budget_mb: u64,
    pub shuffle_partitions: usize,
    pub broadcast_threshold_bytes: u64,
    pub sort_buffer_size_mb: u64,
    pub hash_join_bucket_count: usize,
    pub enable_cbo: bool,
    pub enable_columnar_execution: bool,
    pub optimizer_timeout_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
            max_parallelism: 64,
            memory_budget_mb: 1024,
            shuffle_partitions: 100,
            broadcast_threshold_bytes: 10 * 1024 * 1024,  // 10MB
            sort_buffer_size_mb: 256,
            hash_join_bucket_count: 1024,
            enable_cbo: true,
            enable_columnar_execution: true,
            optimizer_timeout_ms: 30000,
        }
    }
}

/// Global state for the query engine
pub struct EngineState {
    pub config: EngineConfig,
    pub catalog: RwLock<Catalog>,
    pub node_id: NodeId,
}

impl EngineState {
    pub fn new(node_id: NodeId, config: EngineConfig) -> Self {
        Self {
            config,
            catalog: RwLock::new(Catalog::new()),
            node_id,
        }
    }
}

/// Simple catalog for table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub tables: HashMap<String, DistributedTable>,
    pub sources: HashMap<String, DataSourceConfig>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            sources: HashMap::new(),
        }
    }

    pub fn register_table(&mut self, table: DistributedTable) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn get_table(&self, name: &str) -> Option<&DistributedTable> {
        self.tables.get(name)
    }
}

/// Configuration for a data source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    pub name: String,
    pub source_type: DataSourceType,
    pub connection_string: String,
    pub properties: HashMap<String, String>,
}

/// Type of data source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataSourceType {
    Postgres,
    MySQL,
    ClickHouse,
    BigQuery,
    S3,
    Local,
    Elasticsearch,
    MongoDB,
}

/// Error types for Hyperion
#[derive(Debug, thiserror::Error)]
pub enum HyperionError {
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Planning error: {0}")]
    Planning(String),
    
    #[error("Execution error: {0}")]
    Execution(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("Optimization error: {0}")]
    Optimization(String),
    
    #[error("Catalog error: {0}")]
    Catalog(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, HyperionError>;

/// A task identifier for distributed execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskId {
    pub query_id: String,
    pub stage_id: usize,
    pub task_index: usize,
}

/// Query execution stage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStage {
    pub stage_id: usize,
    pub plan: Arc<PhysicalPlan>,
    pub parallelism: usize,
    pub partitioning: PartitioningScheme,
    pub child_stages: Vec<usize>,
}

/// Metrics for query execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryMetrics {
    pub total_rows: u64,
    pub output_rows: u64,
    pub spilled_bytes: u64,
    pub peak_memory_bytes: u64,
    pub cpu_time_ms: u64,
    pub wall_time_ms: u64,
    pub shuffle_bytes: u64,
    pub scan_bytes: u64,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self::default()
    }
}
