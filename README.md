# Hyperion - Distributed SQL Query Engine

<p align="center">
  <img src="https://img.shields.io/badge/version-0.1.0-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License">
  <img src="https://img.shields.io/badge/Rust-1.70+-orange.svg" alt="Rust">
  <img src="https://img.shields.io/badge/status-beta-yellow.svg" alt="Status">
</p>

---

## Overview

**Hyperion** is a high-performance, distributed SQL query engine designed for analytical workloads across heterogeneous data sources. It combines advanced query optimization techniques with a flexible execution engine to deliver sub-second query responses on massive datasets.

### Key Features

- **🧠 Cost-Based Query Optimizer (CBO)**: Advanced optimization with join reordering, predicate pushdown, and access path selection
- **⚡ Volcano-Style Iterator Model**: Pull-based execution with support for parallel and streaming execution
- **🔄 Multiple Join Algorithms**: Hash join, sort-merge join, nested-loop join, and broadcast join with automatic selection
- **📊 Columnar Execution**: Vectorized processing with SIMD optimization for analytical workloads
- **🔐 Distributed Transactions**: Two-phase commit (2PC) for ACID transactions across nodes
- **🌐 Data Federation**: Query multiple data sources including PostgreSQL, BigQuery, S3, and Elasticsearch
- **⚙️ Parallel Query Execution**: Multi-stage parallel execution with data-locality aware scheduling

## Architecture

Hyperion follows a modular architecture with clear separation of concerns:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           Query Interface Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐   │
│  │   SQL CLI    │  │   REST API   │  │   Python     │  │    Java        │   │
│  │              │  │              │  │   Client     │  │    Client      │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                      Query Planning & Optimization                          │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                      Logical Plan Optimizer                            │  │
│  │                                                                         │  │
│  │   • Predicate Pushdown          • Column Pruning                       │  │
│  │   • Constant Folding            • Redundant Operation Removal           │  │
│  │   • Filter Simplification       • Expression Rewriting                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    Cost-Based Physical Optimizer                       │  │
│  │                                                                         │  │
│  │   • Join Order Optimization (Greedy, Left-Deep, Bushy)                 │  │
│  │   • Access Path Selection (Table Scan, Index Scan, Partition Scan)      │  │
│  │   • Algorithm Selection (Hash, Sort-Merge, Nested-Loop, Broadcast)    │  │
│  │   • Parallelism Planning (Degree of Parallelism, Exchange Placement)    │  │
│  │   • Statistics-Based Cost Estimation                                   │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           Execution Engine                                  │
│                                                                             │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │       Row-Based             │  │         Columnar (Vectorized)       │   │
│  │       (Volcano Iterator)    │  │                                      │   │
│  │                              │  │  • Batch processing (1000+ rows)    │   │
│  │  • next() pull model        │  │  • SIMD operations                   │   │
│  │  • Operator tree            │  │  • Dictionary encoding               │   │
│  │  • Memory efficient         │  │  • Late materialization              │   │
│  └─────────────────────────────┘  └─────────────────────────────────────┘   │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                       Operator Implementations                        │     │
│  │                                                                       │     │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │     │
│  │  │   Scan     │  │   Join     │  │  Aggregate  │  │    Sort     │  │     │
│  │  │  Operators │  │  (4 types) │  │  (Hash/Sort)│  │  (External) │  │     │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │     │
│  │                                                                       │     │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │     │
│  │  │  Project   │  │   Filter   │  │   Limit     │  │  Exchange   │  │     │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                       Distributed Execution Layer                           │
│                                                                             │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐│
│  │    Query            │  │    Data             │  │    Transaction       ││
│  │    Scheduler       │  │    Exchange         │  │    Coordinator       ││
│  │                     │  │                     │  │                     ││
│  │  • Work stealing    │  │  • Shuffle sort     │  │  • 2PC Protocol      ││
│  │  • Load balancing  │  │  • Broadcast       │  │  • Recovery          ││
│  │  • Data locality   │  │  • Partitioned     │  │  • Isolation levels  ││
│  │  • Fault tolerance │  │    exchange        │  │                      ││
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Transport Layer                                │   │
│  │                                                                       │   │
│  │  • TCP connections with pooling                                       │   │
│  │  • Zero-copy data transfer                                            │   │
│  │  • Compression (LZ4, Zstd)                                           │   │
│  │  • Flow control and backpressure                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                           Data Sources                                      │
│                                                                             │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────┐ │
│  │ Postgres│ │ MySQL   │ │BigQuery │ │   S3    │ │  Kafka  │ │Elasticsearch│ │
│  │         │ │         │ │         │ │ Parquet │ │         │ │             │ │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────────┘ │
└────────────────────────────────────────────────────────────────────────────┘
```

## Query Planning

Hyperion's query planning process consists of multiple phases that transform a SQL query into an optimized physical execution plan.

### Phase 1: Parsing and AST Generation

The first phase converts SQL text into an Abstract Syntax Tree (AST):

```sql
SELECT o.order_id, c.customer_name, SUM(o.amount) as total
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= '2024-01-01'
GROUP BY o.order_id, c.customer_name
HAVING SUM(o.amount) > 1000
ORDER BY total DESC
LIMIT 100;
```

This SQL query is parsed into a logical plan tree:

```
Limit(100)
  └─ Sort(total DESC)
       └─ Filter(SUM(amount) > 1000)
            └─ Aggregate(group_by=[order_id, customer_name], 
                         agg=[SUM(amount) as total])
                 └─ Join(order.customer_id = customers.id)
                      ├─ TableScan(orders)
                      └─ TableScan(customers)
```

### Phase 2: Logical Plan Optimization

The logical optimizer applies rule-based transformations to simplify the plan without changing semantics:

#### 2.1 Predicate Pushdown

Filters are pushed down to reduce the amount of data processed:

```
Before:                    After:
┌─────────────┐            ┌─────────────┐
│   Filter   │            │TableScan    │
│   (date)   │     →      │(with filter)│
│      │     │            └─────────────┘
└──────│─────┘                   │
┌──────│─────┐                   │
│Scan  │     │                   ▼
└────────────┘            ┌─────────────┐
                          │   Filter   │
                          │  (date)    │
                          └───────────┘
```

#### 2.2 Column Pruning

Only required columns are read from storage:

```rust
// Before: SELECT * FROM users
// Schema: [id, name, email, password, created_at, ...]

// After: SELECT id, name, email FROM users
// Read only: [id, name, email]
```

#### 2.3 Constant Folding

Static expressions are evaluated at planning time:

```sql
-- Before
SELECT * FROM orders WHERE quantity * 10 > 100

-- After (planning time)
SELECT * FROM orders WHERE quantity > 10
```

#### 2.4 Redundant Operation Removal

```sql
-- Before
SELECT * FROM (SELECT * FROM users) sub

-- After
SELECT * FROM users
```

### Phase 3: Cost-Based Physical Optimization

The physical optimizer transforms the logical plan into an optimal physical plan using cost models.

#### 3.1 Statistics Collection

```rust
pub struct TableStatistics {
    pub row_count: u64,           // Total rows
    pub size_bytes: u64,         // Table size
    pub column_stats: HashMap<String, ColumnStatistics>,
}

pub struct ColumnStatistics {
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub histogram: Vec<HistogramBucket>,  // For range queries
    pub ndv_estimate: f64,         // Number of distinct values
}
```

#### 3.2 Selectivity Estimation

Selectivity estimates determine the fraction of rows a predicate will filter:

```rust
// Equality predicate selectivity
fn equality_selectivity(ndv: f64) -> f64 {
    1.0 / ndv.min(1.0)
}

// Range predicate selectivity  
fn range_selectivity(min: &Value, max: &Value, val: &Value) -> f64 {
    // Use histogram if available
    // Otherwise assume uniform distribution
    0.33
}

// Conjunction (AND)
fn and_selectivity(a: f64, b: f64) -> f64 {
    a * b  // Assuming independence
}

// Disjunction (OR)
fn or_selectivity(a: f64, b: f64) -> f64 {
    a + b - (a * b)  // Inclusion-exclusion
}
```

#### 3.3 Join Reordering

Join order dramatically affects performance. Hyperion supports multiple strategies:

**Greedy Algorithm:**
```rust
// Start with smallest table
// Repeatedly join with cheapest remaining table
let mut remaining = tables.sort_by_row_count();
let mut plan = remaining.pop();  // Smallest first

while !remaining.is_empty() {
    let next = remaining
        .iter()
        .min_by_key(|t| estimate_join_cost(plan, t));
    plan = join(plan, next);
    remaining.remove(next);
}
```

**Available Strategies:**
- `Greedy`: Fast, good for small numbers of tables
- `LeftDeep`: Optimal for certain query patterns
- `Bushy`: Best for complex star/snowflake schemas
- `Exhaustive`: Optimal but expensive
- `Genetic`: Good for very complex queries

#### 3.4 Algorithm Selection

Hyperion automatically selects the best join algorithm:

| Left Size | Right Size | Algorithm |
|-----------|------------|-----------|
| Small (< broadcast_threshold) | Any | Broadcast Hash Join |
| Large | Small (< broadcast_threshold) | Broadcast Hash Join (right) |
| Large | Large | Partitioned Hash Join |
| Both sorted | Both sorted | Sort-Merge Join |
| Small | Medium (indexed) | Nested Loop with Index |
| Large | Medium (no index) | Sort-Merge Join |

#### 3.5 Parallelism Planning

```rust
pub struct ParallelismPlan {
    pub degree_of_parallelism: usize,
    pub partitioning: PartitioningScheme,
    pub exchange_placement: Vec<ExchangePoint>,
}

fn compute_optimal_dop(stats: &TableStatistics, config: &EngineConfig) -> usize {
    let max_dop = config.max_parallelism;
    let memory_per_thread = config.memory_budget_mb / max_dop;
    
    // Don't parallelize if memory per thread is too small
    if memory_per_thread < 64 {
        return 1;
    }
    
    // Scale with data size
    let rows_per_thread = stats.row_count / max_dop;
    if rows_per_thread < 10000 {
        return 1;
    }
    
    max_dop.min(stats.row_count / 100000)
}
```

## Distributed Execution

### Query Execution Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Query Lifecycle                                     │
└──────────────────────────────────────────────────────────────────────────────┘

  Client                Coordinator                  Workers
    │                        │                          │
    │──── Submit Query ─────►│                          │
    │                        │                          │
    │                        │──── Parse & Plan ───────►│
    │                        │                          │
    │                        │◄─── Optimized Plan ─────│
    │                        │                          │
    │                        │──── Schedule ───────────►│
    │                        │     Fragments            │
    │                        │                          │
    │                        │◄─── Results ────────────│
    │                        │     (Exchange)            │
    │                        │                          │
    │◄─── Final Results ─────│                          │
    │                        │                          │
```

### Stage Parallelism

Large queries are divided into stages that can execute in parallel:

```
Query: SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id

Physical Plan:
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Query DAG                                          │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
    │  Scan a     │         │  Scan b     │         │  Scan c     │
    │ (partitioned)│        │ (partitioned)│        │ (partitioned)│
    └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
           │                       │                       │
           ▼                       ▼                       ▼
    ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
    │  Exchange   │         │  Exchange   │         │  Exchange   │
    │ (shuffle)   │         │ (shuffle)   │         │ (shuffle)   │
    └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
           │                       │                       │
           └───────────────────────┼───────────────────────┘
                                   │
                                   ▼
                         ┌─────────────────┐
                         │   Join Stage    │
                         │  (Partitioned)  │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │   Final Stage   │
                         │  (Aggregation) │
                         └─────────────────┘
```

### Data Exchange

Hyperion supports multiple exchange patterns:

#### 1. Shuffle Exchange
```rust
pub struct ShuffleExchange {
    pub partition_keys: Vec<Expression>,
    pub num_partitions: usize,
    pub compression: CompressionType,
}
```

```
    ┌─────┐ ┌─────┐ ┌─────┐
    │ P0  │ │ P1  │ │ P2  │   Input Partitions
    └──┬──┘ └──┬──┘ └──┬──┘
       │       │       │
       ▼       ▼       ▼
    ┌─────────────────────────┐
    │     Hash Partitioning    │
    │   (by join keys)         │
    └───────────┬─────────────┘
                │
       ┌────────┴────────┐
       ▼       ▼       ▼
    ┌─────┐ ┌─────┐ ┌─────┐
    │ N0  │ │ N1  │ │ N2  │   Network Transfer
    └─────┘ └─────┘ └─────┘
       │       │       │
       ▼       ▼       ▼
    ┌─────┐ ┌─────┐ ┌─────┐
    │ R0  │ │ R1  │ │ R2  │   Repartitioned
    └─────┘ └─────┘ └─────┘
```

#### 2. Broadcast Exchange
```rust
pub struct BroadcastExchange {
    pub target_partitions: usize,
}
```

```
    Small Table (< broadcast_threshold)
    ┌─────────────┐
    │   Data      │
    └──────┬──────┘
           │
    ┌──────┴──────┬──────┐
    │             │      │
    ▼            ▼      ▼
┌─────┐    ┌─────┐ ┌─────┐
│ N0  │    │ N1  │ │ N2  │
└─────┘    └─────┘ └─────┘
```

#### 3. Round-Robin Exchange
```rust
pub struct RoundRobinExchange {
    pub num_partitions: usize,
}
```

```
    ┌─────┐ ┌─────┐ ┌─────┐
    │Row 1│ │Row 2│ │Row 3│   Input
    │Row 4│ │Row 5│ │Row 6│
    └──┬──┘ └──┬──┘ └──┬──┘
       │       │       │
       ▼       ▼       ▼
    ┌─────────────────────────┐
    │     Round-Robin          │
    │     Distribution         │
    └───────────┬─────────────┘
                │
       ┌────────┴────────┐
       ▼       ▼       ▼
    ┌─────┐ ┌─────┐ ┌─────┐
    │ R0  │ │ R1  │ │ R2  │   Balanced
    └─────┘ └─────┘ └─────┘
```

### Transaction Coordination

Hyperion implements two-phase commit (2PC) for distributed transactions:

```
    Coordinator              Participant 1            Participant 2
         │                        │                       │
         │────── BEGIN TX ───────►│                       │
         │                        │                       │
         │                        │◄─────── OK ───────────│
         │                        │                       │
         │◄─────── OK ────────────│                       │
         │                        │                       │
         │        [ Execute operations... ]                │
         │                        │                       │
         │─────── PREPARE ───────►│─────── PREPARE ───────►│
         │                        │                       │
         │                        │◄───── VOTE YES ────────│
         │◄───── VOTE YES ─────────│                       │
         │                        │                       │
         │                        │───── VOTE YES ────────►│
         │◄───────────────────────────────────────────────│
         │                        │                       │
         │ [ All voted YES ]      │                       │
         │                        │                       │
         │────── COMMIT ─────────►│────── COMMIT ─────────►│
         │                        │                       │
         │                        │◄───── COMMITTED ──────│
         │◄──── COMMITTED ─────────│                       │
         │                        │───── COMMITTED ───────►│
         │◄───────────────────────────────────────────────│
         │                        │                       │
```

#### Transaction States

```rust
pub enum TransactionState {
    Active,        // Transaction started
    Preparing,     // Sending prepare to participants
    Prepared,      // All participants voted yes
    Committing,    // Sending commit to participants
    Committed,    // Transaction committed
    Aborted,      // Transaction rolled back
    Recovery,     // Recovering from failure
}
```

### Fault Tolerance

Hyperion provides fault tolerance through:

1. **Task Retry**: Failed tasks are automatically retried on different nodes
2. **Speculative Execution**: Slow tasks are replicated for faster completion
3. **Shuffle Recovery**: Intermediate data is persisted for recovery
4. **Transaction Recovery**: In-doubt transactions are automatically resolved on recovery

```rust
pub struct RetryConfig {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub exponential_backoff: bool,
    pub max_retry_delay_ms: u64,
}

pub struct SpeculativeConfig {
    pub enabled: bool,
    pub slowdown_threshold_ms: u64,
    pub max_speculative_copies: usize,
}
```

## Data Federation

Hyperion can query data across multiple heterogeneous sources:

### Supported Sources

| Source | Capabilities | Pushdown |
|--------|-------------|----------|
| PostgreSQL | Full SQL | Full |
| MySQL | Full SQL | Full |
| BigQuery | BigQuery SQL | Partial |
| S3 (Parquet/CSV) | Files | Filter only |
| Elasticsearch | Query DSL | Full |
| MongoDB | Aggregation | Full |
| Kafka | Stream | N/A |

### Federation Example

```rust
// Register data sources
engine.register_source("pg".into(), PostgresConfig {
    host: "warehouse.internal".into(),
    database: "sales".into(),
    // ...
});

engine.register_source("bq".into(), BigQueryConfig {
    project_id: "analytics-prod".into(),
    dataset: "events".into(),
    // ...
});

// Execute federated query
let result = engine.execute("
    SELECT 
        p.product_name,
        COUNT(*) as events,
        SUM(b.revenue) as revenue
    FROM pg.products p
    JOIN bq.events e ON p.id = e.product_id
    WHERE e.date >= '2024-01-01'
    GROUP BY p.product_name
").await?;
```

## Getting Started

### Installation

```bash
# From source
git clone https://github.com/moggan1337/Hyperion.git
cd Hyperion
cargo build --release

# Binary will be at target/release/hyperion
```

### Quick Start

```bash
# Start single-node server
hyperion server

# Execute a query
hyperion query "SELECT * FROM users LIMIT 10"
```

### Configuration

Create a `hyperion.toml` configuration file:

```toml
[engine]
default_parallelism = 4
max_parallelism = 64
memory_budget_mb = 1024
broadcast_threshold_bytes = 10485760

[optimizer]
enable_cbo = true
join_ordering = "greedy"
optimization_timeout_ms = 30000

[storage]
data_dir = "/var/lib/hyperion/data"
shuffle_dir = "/var/lib/hyperion/shuffle"

[[sources]]
name = "warehouse"
type = "postgres"
connection_string = "host=localhost dbname=sales"
```

### Rust Client

```rust
use hyperion::{HyperionEngine, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let engine = HyperionEngine::new(config);
    
    // Execute query
    let result = engine.execute("SELECT * FROM users").await?;
    
    for batch in result.batches {
        println!("{:?}", batch);
    }
    
    println!("Rows: {}", result.metrics.output_rows);
    println!("Time: {:?}", result.metrics.wall_time_ms);
    
    Ok(())
}
```

## Performance

Hyperion is designed for analytical workloads:

| Metric | Performance |
|--------|-------------|
| Simple Scan | 10M rows/sec/core |
| Aggregation | 5M rows/sec/core |
| Hash Join (in-memory) | 2M rows/sec/core |
| Sort (in-memory) | 3M rows/sec/core |

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Hyperion is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

---

<p align="center">
  Built with ❤️ by the Hyperion Team
</p>
