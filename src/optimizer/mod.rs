//! Hyperion Cost-Based Optimizer (CBO)
//! 
//! Implements rule-based and cost-based optimization for distributed queries.

use crate::common::*;
use crate::planner::{LogicalPlan, Expression, SortSpec, LogicalJoin, JoinType};
use std::collections::HashMap;

/// Cost model for query optimization
#[derive(Debug, Clone)]
pub struct CostModel {
    pub row_size_bytes: f64,
    pub cpu_cost_per_row: f64,
    pub network_cost_per_byte: f64,
    pub disk_cost_per_byte: f64,
    pub memory_cost_per_byte: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            row_size_bytes: 100.0,
            cpu_cost_per_row: 0.01,
            network_cost_per_byte: 0.001,
            disk_cost_per_byte: 0.0001,
            memory_cost_per_byte: 0.00001,
        }
    }
}

/// Estimated cost of a physical plan
#[derive(Debug, Clone)]
pub struct PlanCost {
    pub estimated_rows: f64,
    pub estimated_bytes: f64,
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,
    pub disk_cost: f64,
}

impl PlanCost {
    pub fn total(&self) -> f64 {
        self.cpu_cost + self.memory_cost + self.network_cost + self.disk_cost
    }
}

/// Statistics about table data
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    pub row_count: u64,
    pub size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

/// Statistics for a column
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub histogram: Vec<HistogramBucket>,
    pub ndv_estimate: f64,  // Number of distinct values estimate
}

impl Default for ColumnStatistics {
    fn default() -> Self {
        Self {
            null_count: 0,
            distinct_count: 0,
            min_value: None,
            max_value: None,
            histogram: Vec::new(),
            ndv_estimate: 0.0,
        }
    }
}

/// Selectivity estimation
pub struct SelectivityEstimator {
    cost_model: CostModel,
}

impl SelectivityEstimator {
    pub fn new(cost_model: CostModel) -> Self {
        Self { cost_model }
    }

    /// Estimate selectivity of a predicate
    pub fn estimate_selectivity(&self, expr: &Expression, stats: &TableStatistics) -> f64 {
        match expr {
            Expression::Equal(col, val) => {
                self.equality_selectivity(col, val, stats)
            }
            Expression::LessThan(col, val) => {
                self.range_selectivity(col, val, true, false, stats)
            }
            Expression::LessThanOrEqual(col, val) => {
                self.range_selectivity(col, val, true, true, stats)
            }
            Expression::GreaterThan(col, val) => {
                self.range_selectivity(col, val, false, false, stats)
            }
            Expression::GreaterThanOrEqual(col, val) => {
                self.range_selectivity(col, val, false, true, stats)
            }
            Expression::Between { expr, low, high } => {
                let lo_sel = self.range_selectivity(expr, low, false, true, stats);
                let hi_sel = self.range_selectivity(expr, high, true, true, stats);
                (lo_sel + hi_sel).min(1.0)
            }
            Expression::IsNull(col) => {
                let col_name = Self::get_column_name(col);
                stats.column_stats.get(&col_name)
                    .map(|s| s.null_count as f64 / stats.row_count.max(1) as f64)
                    .unwrap_or(0.05)
            }
            Expression::IsNotNull(col) => {
                1.0 - self.estimate_selectivity(&Expression::IsNull(col.clone()), stats)
            }
            Expression::In { expr, values } => {
                let ndv = self.get_ndv(expr, stats);
                (values.len() as f64 / ndv).min(1.0)
            }
            Expression::And(conditions) => {
                conditions.iter()
                    .map(|c| self.estimate_selectivity(c, stats))
                    .fold(1.0, |acc, s| acc * s)
            }
            Expression::Or(conditions) => {
                let sel: f64 = conditions.iter()
                    .map(|c| self.estimate_selectivity(c, stats))
                    .sum();
                sel.min(1.0)
            }
            Expression::Not(inner) => {
                1.0 - self.estimate_selectivity(inner, stats)
            }
            Expression::Like { expr, pattern } => {
                // Approximate selectivity for LIKE patterns
                let pattern_str = match pattern.as_ref() {
                    Expression::Literal(Value::String(s)) => s,
                    _ => return 0.3,
                };
                if pattern_str.starts_with('%') && pattern_str.ends_with('%') {
                    0.5  // Contains pattern
                } else if pattern_str.starts_with('%') {
                    0.3  // Ends with
                } else if pattern_str.ends_with('%') {
                    0.3  // Starts with
                } else {
                    0.1  // Exact prefix
                }
            }
            Expression::Literal(Value::Boolean(true)) => 1.0,
            Expression::Literal(Value::Boolean(false)) => 0.0,
            _ => 0.1,  // Default selectivity for unknown predicates
        }
    }

    fn equality_selectivity(&self, col: &Expression, _val: &Expression, stats: &TableStatistics) -> f64 {
        let col_name = Self::get_column_name(col);
        let ndv = stats.column_stats.get(&col_name)
            .map(|s| s.ndv_estimate)
            .unwrap_or(stats.row_count as f64 / 10.0);
        
        if ndv <= 0.0 {
            1.0 / stats.row_count.max(1) as f64
        } else {
            (1.0 / ndv).min(1.0)
        }
    }

    fn range_selectivity(&self, col: &Expression, val: &Expression, _low: bool, _inclusive: bool, stats: &TableStatistics) -> f64 {
        let col_name = Self::get_column_name(col);
        
        // Use histogram if available
        if let Some(col_stats) = stats.column_stats.get(&col_name) {
            if !col_stats.histogram.is_empty() {
                return self.histogram_range_selectivity(col_stats, val);
            }
        }
        
        // Default: uniform distribution assumption
        0.33
    }

    fn histogram_range_selectivity(&self, col_stats: &ColumnStatistics, val: &Expression) -> f64 {
        // Simplified histogram selectivity
        if let Expression::Literal(val_lit) = val {
            let val = val_lit;
            
            // Find bucket containing value
            for bucket in &col_stats.histogram {
                if bucket.lower_bound <= *val && bucket.upper_bound >= *val {
                    return bucket.count as f64 / col_stats.histogram.iter().map(|b| b.count).sum::<u64>().max(1) as f64;
                }
            }
            
            // Value outside histogram range
            return 0.01;
        }
        
        0.33  // Default
    }

    fn get_ndv(&self, col: &Expression, stats: &TableStatistics) -> f64 {
        let col_name = Self::get_column_name(col);
        stats.column_stats.get(&col_name)
            .map(|s| s.ndv_estimate)
            .unwrap_or(stats.row_count as f64 / 10.0)
    }

    fn get_column_name(expr: &Expression) -> String {
        match expr {
            Expression::ColumnReference { name, .. } => name.clone(),
            _ => String::new(),
        }
    }
}

/// Join ordering strategies
#[derive(Debug, Clone, Copy)]
pub enum JoinOrderingStrategy {
    Greedy,
    LeftDeep,
    Bushy,
    Exhaustive,
    Genetic,
}

/// Physical join algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinAlgorithm {
    /// Broadcast hash join - small table sent to all nodes
    BroadcastHash { build_side: BuildSide },
    /// Partitioned hash join - both sides partitioned on join keys
    PartitionedHash { bucket_count: usize },
    /// Sort-merge join with optional pre-sorting
    SortMerge { left_sorted: bool, right_sorted: bool },
    /// Nested loop join with hints
    NestedLoop { outer_side: OuterSide, parallel: bool },
    /// Hash join with graceful degradation to nested loop
    GracefulHash { max_memory_bytes: u64 },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BuildSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OuterSide {
    Left,
    Right,
}

/// Physical aggregation strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationStrategy {
    /// Hash-based aggregation - groups rows by hash of group-by keys
    Hash { bucket_count: usize },
    /// Sort-based aggregation - sort then scan for group boundaries
    Sort { buffer_size_mb: u64 },
    /// Two-phase aggregation - partial then final
    TwoPhase { partial_strategy: Box<AggregationStrategy> },
    /// Streaming aggregation for memory-constrained environments
    Streaming { chunk_size: usize },
}

/// Physical exchange strategies for distributed execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExchangeStrategy {
    /// No exchange needed - single partition
    Local,
    /// Broadcast to all partitions
    Broadcast { target_partitions: usize },
    /// Repartition by hash of keys
    RepartitionHash { keys: Vec<Expression>, num_partitions: usize },
    /// Repartition by range of keys
    RepartitionRange { keys: Vec<Expression>, boundaries: Vec<Vec<Value>> },
    /// Random partitioning for load balancing
    RoundRobin { num_partitions: usize },
    /// Keep partitioning from previous stage
    PreservePartitioning,
}

/// Physical scan strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScanStrategy {
    TableScan,
    IndexScan { index_name: String },
    PartitionScan { partitions: Vec<PartitionId> },
    MetadataOnlyScan,
}

/// Physical sort strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortStrategy {
    /// In-memory sort with spilling
    MemorySort { buffer_size_mb: u64 },
    /// External merge sort
    ExternalSort { memory_fraction: f64, num_passes: usize },
    /// Pre-sorted input (e.g., from index)
    Presorted,
}

/// Physical operator types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalOperator {
    // Scan operators
    TableScan {
        table_name: String,
        output_columns: Vec<usize>,
        filter: Option<Expression>,
        statistics: TableStatistics,
    },
    IndexScan {
        table_name: String,
        index_name: String,
        output_columns: Vec<usize>,
        range_conditions: Vec<Expression>,
        statistics: TableStatistics,
    },
    
    // Join operators
    HashJoin {
        algorithm: JoinAlgorithm,
        join_type: JoinType,
        left_keys: Vec<Expression>,
        right_keys: Vec<Expression>,
        estimated_left_rows: u64,
        estimated_right_rows: u64,
    },
    MergeJoin {
        join_type: JoinType,
        left_keys: Vec<Expression>,
        right_keys: Vec<Expression>,
        left_sorted: bool,
        right_sorted: bool,
    },
    NestedLoopJoin {
        join_type: JoinType,
        outer_keys: Vec<Expression>,
        inner_keys: Vec<Expression>,
        parallel: bool,
        estimated_outer_rows: u64,
    },
    BroadcastJoin {
        join_type: JoinType,
        build_side: BuildSide,
        join_keys: Vec<Expression>,
    },
    SymmetricHashJoin {
        join_type: JoinType,
        left_keys: Vec<Expression>,
        right_keys: Vec<Expression>,
        buffer_size_mb: u64,
    },
    
    // Aggregation operators
    HashAggregate {
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateSpec>,
        estimated_groups: u64,
    },
    SortAggregate {
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateSpec>,
        sort_keys: Vec<SortSpec>,
    },
    StreamAggregate {
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateSpec>,
    },
    SingleGroupByAggregate {
        aggregates: Vec<AggregateSpec>,
    },
    
    // Set operators
    Union { all: bool, num_partitions: usize },
    Intersect { all: bool },
    Except { all: bool },
    
    // Sorting and limiting
    Sort {
        sort_specs: Vec<SortSpec>,
        strategy: SortStrategy,
        estimated_rows: u64,
    },
    Limit {
        limit: usize,
        offset: Option<usize>,
    },
    TopN {
        n: usize,
        sort_specs: Vec<SortSpec>,
    },
    
    // Projection and filtering
    Projection {
        expressions: Vec<(Expression, usize)>,
    },
    Filter {
        condition: Expression,
        estimated_selectivity: f64,
    },
    
    // Exchange for distributed execution
    Exchange {
        strategy: ExchangeStrategy,
        required_ordering: Option<Vec<SortSpec>>,
    },
    
    // Window functions
    Window {
        window_specs: Vec<WindowSpec>,
    },
    
    // Write operators
    Insert {
        table_name: String,
        partition_columns: Option<Vec<String>>,
    },
    Update {
        table_name: String,
        updates: Vec<(String, Expression)>,
    },
    Delete {
        table_name: String,
    },
    
    // Materialization
    Materialize,
    
    // Empty input
    Empty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateSpec {
    pub function: AggregateFunction,
    pub arguments: Vec<Expression>,
    pub filter: Option<Expression>,
    pub distinct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
    ApproxDistinct,
    ArrayAgg,
    StringAgg,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    pub name: String,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<SortSpec>,
    pub frame: Option<WindowFrame>,
    pub function: WindowFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Lag(Expression, Option<Expression>),
    Lead(Expression, Option<Expression>),
    FirstValue(Expression),
    LastValue(Expression),
    NthValue(Expression, usize),
}

/// Physical query plan - tree of physical operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalPlan {
    pub root: PhysicalOperator,
    pub estimated_cost: PlanCost,
    pub estimated_rows: u64,
    pub estimated_output_bytes: u64,
    pub partitioning: Option<PartitioningScheme>,
    pub ordering: Option<Vec<SortSpec>>,
    pub children: Vec<Arc<PhysicalPlan>>,
}

impl PhysicalPlan {
    pub fn new(root: PhysicalOperator) -> Self {
        Self {
            root,
            estimated_cost: PlanCost {
                estimated_rows: 0.0,
                estimated_bytes: 0.0,
                cpu_cost: 0.0,
                memory_cost: 0.0,
                network_cost: 0.0,
                disk_cost: 0.0,
            },
            estimated_rows: 0,
            estimated_output_bytes: 0,
            partitioning: None,
            ordering: None,
            children: Vec::new(),
        }
    }
}

/// Cost-based query optimizer
pub struct CostBasedOptimizer {
    config: CBOConfig,
    cost_model: CostModel,
    selectivity_estimator: SelectivityEstimator,
}

#[derive(Debug, Clone)]
pub struct CBOConfig {
    pub join_ordering: JoinOrderingStrategy,
    pub enable_broadcast_join: bool,
    pub enable_hash_join: bool,
    pub enable_merge_join: bool,
    pub enable_nested_loop_join: bool,
    pub broadcast_threshold_bytes: u64,
    pub max_join_cardinality: usize,
    pub optimization_timeout_ms: u64,
    pub enable_differential_evolution: bool,
    pub population_size: usize,
    pub generations: usize,
}

impl Default for CBOConfig {
    fn default() -> Self {
        Self {
            join_ordering: JoinOrderingStrategy::Greedy,
            enable_broadcast_join: true,
            enable_hash_join: true,
            enable_merge_join: true,
            enable_nested_loop_join: true,
            broadcast_threshold_bytes: 10 * 1024 * 1024,  // 10MB
            max_join_cardinality: 20,
            optimization_timeout_ms: 30000,
            enable_differential_evolution: false,
            population_size: 50,
            generations: 100,
        }
    }
}

impl CostBasedOptimizer {
    pub fn new(config: CBOConfig) -> Self {
        Self {
            cost_model: CostModel::default(),
            selectivity_estimator: SelectivityEstimator::new(CostModel::default()),
            config,
        }
    }

    /// Convert a logical plan to an optimized physical plan
    pub fn optimize(&self, logical_plan: LogicalPlan, stats: &HashMap<String, TableStatistics>) -> Result<PhysicalPlan> {
        // Phase 1: Logical to physical conversion
        let mut physical_plan = self.logical_to_physical(logical_plan, stats)?;
        
        // Phase 2: Physical optimizations
        physical_plan = self.optimize_joins(physical_plan, stats)?;
        physical_plan = self.optimize_aggregations(physical_plan, stats)?;
        physical_plan = self.add_exchanges(physical_plan, stats)?;
        
        Ok(physical_plan)
    }

    /// Convert logical plan to physical plan
    fn logical_to_physical(&self, plan: LogicalPlan, stats: &HashMap<String, TableStatistics>) -> Result<PhysicalPlan> {
        match plan {
            LogicalPlan::Scan(scan) => {
                let table_stats = stats.get(&scan.table_name).cloned().unwrap_or_default();
                let output_columns: Vec<usize> = (0..scan.output_columns.len()).collect();
                
                let filter = scan.predicate.clone();
                let selectivity = filter.as_ref()
                    .map(|f| self.selectivity_estimator.estimate_selectivity(f, &table_stats))
                    .unwrap_or(1.0);
                
                let estimated_rows = (table_stats.row_count as f64 * selectivity) as u64;
                
                Ok(PhysicalPlan::new(PhysicalOperator::TableScan {
                    table_name: scan.table_name,
                    output_columns,
                    filter,
                    statistics: table_stats,
                }))
            }
            LogicalPlan::Filter { condition, input } => {
                let child_plan = self.logical_to_physical(*input, stats)?;
                let selectivity = self.selectivity_estimator.estimate_selectivity(
                    &condition,
                    &TableStatistics::default()
                );
                
                Ok(PhysicalPlan {
                    root: PhysicalOperator::Filter {
                        condition,
                        estimated_selectivity: selectivity,
                    },
                    ..child_plan
                })
            }
            LogicalPlan::Projection { expressions, input } => {
                let child_plan = self.logical_to_physical(*input, stats)?;
                let projections: Vec<(Expression, usize)> = expressions
                    .into_iter()
                    .enumerate()
                    .map(|(i, (e, _))| (e, i))
                    .collect();
                
                Ok(PhysicalPlan {
                    root: PhysicalOperator::Projection { expressions: projections },
                    ..child_plan
                })
            }
            LogicalPlan::Join(join) => {
                // This will be refined in the join optimization phase
                Ok(PhysicalPlan::new(PhysicalOperator::HashJoin {
                    algorithm: JoinAlgorithm::PartitionedHash {
                        bucket_count: 1024,
                    },
                    join_type: join.join_type,
                    left_keys: join.left_keys,
                    right_keys: join.right_keys,
                    estimated_left_rows: 0,
                    estimated_right_rows: 0,
                }))
            }
            LogicalPlan::Sort { sort_specs, input } => {
                let child_plan = self.logical_to_physical(*input, stats)?;
                
                Ok(PhysicalPlan {
                    root: PhysicalOperator::Sort {
                        sort_specs,
                        strategy: SortStrategy::MemorySort {
                            buffer_size_mb: 256,
                        },
                        estimated_rows: child_plan.estimated_rows,
                    },
                    ..child_plan
                })
            }
            LogicalPlan::Limit(limit) => {
                let child_plan = self.logical_to_physical(*limit.input, stats)?;
                
                let limit_expr = limit.limit;
                let limit_val = match limit_expr {
                    Some(Expression::Literal(Value::Integer(n))) => n as usize,
                    _ => usize::MAX,
                };
                
                Ok(PhysicalPlan {
                    root: PhysicalOperator::Limit {
                        limit: limit_val,
                        offset: None,
                    },
                    estimated_rows: limit_val as u64,
                    ..child_plan
                })
            }
            LogicalPlan::Aggregate(agg) => {
                let aggregates: Vec<AggregateSpec> = agg.aggregates
                    .iter()
                    .map(|a| AggregateSpec {
                        function: match a.function {
                            crate::planner::AggregateFunction::Count => AggregateFunction::Count,
                            crate::planner::AggregateFunction::Sum => AggregateFunction::Sum,
                            crate::planner::AggregateFunction::Avg => AggregateFunction::Avg,
                            crate::planner::AggregateFunction::Min => AggregateFunction::Min,
                            crate::planner::AggregateFunction::Max => AggregateFunction::Max,
                            crate::planner::AggregateFunction::CountDistinct => AggregateFunction::CountDistinct,
                            crate::planner::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
                            crate::planner::AggregateFunction::StringAgg => AggregateFunction::StringAgg,
                        },
                        arguments: a.arguments.clone(),
                        filter: a.filter.clone(),
                        distinct: false,
                    })
                    .collect();
                
                Ok(PhysicalPlan::new(PhysicalOperator::HashAggregate {
                    group_by: agg.group_by,
                    aggregates,
                    estimated_groups: 1000,  // Estimate
                }))
            }
            _ => Ok(PhysicalPlan::new(PhysicalOperator::Empty)),
        }
    }

    /// Optimize join ordering and algorithm selection
    fn optimize_joins(&self, plan: PhysicalPlan, _stats: &HashMap<String, TableStatistics>) -> Result<PhysicalPlan> {
        match plan.root {
            PhysicalOperator::HashJoin { 
                algorithm: JoinAlgorithm::PartitionedHash { .. },
                join_type,
                left_keys,
                right_keys,
                ..
            } => {
                // Decide on join algorithm based on statistics
                // For now, keep partitioned hash join
                Ok(PhysicalPlan {
                    root: PhysicalOperator::HashJoin {
                        algorithm: JoinAlgorithm::PartitionedHash {
                            bucket_count: 1024,
                        },
                        join_type,
                        left_keys,
                        right_keys,
                        estimated_left_rows: plan.estimated_rows,
                        estimated_right_rows: plan.estimated_rows,
                    },
                    ..plan
                })
            }
            _ => Ok(plan),
        }
    }

    /// Optimize aggregation strategies
    fn optimize_aggregations(&self, plan: PhysicalPlan, _stats: &HashMap<String, TableStatistics>) -> Result<PhysicalPlan> {
        match plan.root {
            PhysicalOperator::HashAggregate { group_by, .. } if group_by.is_empty() => {
                // No GROUP BY - use single group aggregation
                Ok(PhysicalPlan {
                    root: PhysicalOperator::SingleGroupByAggregate {
                        aggregates: match plan.root {
                            PhysicalOperator::HashAggregate { aggregates, .. } => aggregates,
                            _ => Vec::new(),
                        },
                    },
                    ..plan
                })
            }
            _ => Ok(plan),
        }
    }

    /// Add exchange operators for distributed execution
    fn add_exchanges(&self, plan: PhysicalPlan, _stats: &HashMap<String, TableStatistics>) -> Result<PhysicalPlan> {
        // Add necessary exchanges for distributed execution
        // This is a simplified version - production would be more sophisticated
        Ok(plan)
    }

    /// Calculate cost of a physical plan
    pub fn calculate_cost(&self, plan: &PhysicalPlan) -> PlanCost {
        match &plan.root {
            PhysicalOperator::TableScan { statistics, filter, .. } => {
                let base_cost = statistics.row_count as f64 * self.cost_model.cpu_cost_per_row;
                let filter_cost = filter.as_ref()
                    .map(|_| statistics.row_count as f64 * self.cost_model.cpu_cost_per_row * 0.5)
                    .unwrap_or(0.0);
                
                PlanCost {
                    estimated_rows: statistics.row_count as f64,
                    estimated_bytes: statistics.size_bytes as f64,
                    cpu_cost: base_cost + filter_cost,
                    memory_cost: 0.0,
                    network_cost: 0.0,
                    disk_cost: 0.0,
                }
            }
            PhysicalOperator::HashJoin { 
                estimated_left_rows,
                estimated_right_rows,
                algorithm,
                ..
            } => {
                let build_cost = *estimated_right_rows as f64 * self.cost_model.cpu_cost_per_row;
                let probe_cost = *estimated_left_rows as f64 * self.cost_model.cpu_cost_per_row;
                
                let memory_cost = match algorithm {
                    JoinAlgorithm::PartitionedHash { bucket_count } => {
                        *bucket_count as f64 * 8.0 * self.cost_model.memory_cost_per_byte
                    }
                    _ => 0.0,
                };
                
                PlanCost {
                    estimated_rows: estimated_left_rows.max(*estimated_right_rows) as f64,
                    estimated_bytes: 0.0,
                    cpu_cost: build_cost + probe_cost,
                    memory_cost,
                    network_cost: 0.0,
                    disk_cost: 0.0,
                }
            }
            PhysicalOperator::Filter { condition, estimated_selectivity } => {
                let cpu_cost = plan.children.first()
                    .map(|c| c.estimated_cost.cpu_cost)
                    .unwrap_or(0.0);
                
                PlanCost {
                    estimated_rows: plan.estimated_rows as f64 * estimated_selectivity,
                    estimated_bytes: 0.0,
                    cpu_cost,
                    memory_cost: 0.0,
                    network_cost: 0.0,
                    disk_cost: 0.0,
                }
            }
            _ => PlanCost::default(),
        }
    }
}

/// Join reordering using greedy algorithm
pub struct JoinReorderer {
    config: CBOConfig,
}

impl JoinReorderer {
    pub fn new(config: CBOConfig) -> Self {
        Self { config }
    }

    /// Find optimal join order using greedy algorithm
    pub fn find_optimal_order(&self, tables: Vec<JoinTable>) -> Vec<JoinTable> {
        if tables.len() <= 2 {
            return tables;
        }

        let mut remaining: Vec<_> = tables;
        let mut result = Vec::new();

        // Start with the smallest table
        remaining.sort_by_key(|t| t.statistics.row_count);
        
        if let Some(first) = remaining.pop() {
            result.push(first);
        }

        // Greedy join - always join with the cheapest next table
        while !remaining.is_empty() {
            if let Some(best_idx) = self.find_best_join(&result, &remaining) {
                let next = remaining.remove(best_idx);
                result.push(next);
            } else {
                // Fallback: just append remaining
                result.extend(remaining.drain(..));
                break;
            }
        }

        result
    }

    fn find_best_join(&self, current: &[JoinTable], candidates: &[JoinTable]) -> Option<usize> {
        candidates.iter()
            .enumerate()
            .min_by_key(|(_, t)| t.join_cost)
            .map(|(i, _)| i)
    }
}

/// Information about a table for join ordering
#[derive(Debug, Clone)]
pub struct JoinTable {
    pub table_name: String,
    pub alias: Option<String>,
    pub statistics: TableStatistics,
    pub join_cost: u64,
    pub join_keys: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selectivity_equality() {
        let cost_model = CostModel::default();
        let estimator = SelectivityEstimator::new(cost_model);
        
        let stats = TableStatistics {
            row_count: 10000,
            size_bytes: 1000000,
            column_stats: HashMap::new(),
        };
        
        // Without stats, equality should estimate low selectivity
        let expr = Expression::Equal(
            Box::new(Expression::ColumnReference { name: "id".into(), table_alias: None }),
            Box::new(Expression::Literal(Value::Integer(1))),
        );
        
        let sel = estimator.estimate_selectivity(&expr, &stats);
        assert!(sel > 0.0 && sel <= 1.0);
    }

    #[test]
    fn test_join_reordering() {
        let tables = vec![
            JoinTable {
                table_name: "large".into(),
                alias: None,
                statistics: TableStatistics { row_count: 1000000, ..default() },
                join_cost: 0,
                join_keys: vec![],
            },
            JoinTable {
                table_name: "small".into(),
                alias: None,
                statistics: TableStatistics { row_count: 100, ..default() },
                join_cost: 0,
                join_keys: vec![],
            },
            JoinTable {
                table_name: "medium".into(),
                alias: None,
                statistics: TableStatistics { row_count: 10000, ..default() },
                join_cost: 0,
                join_keys: vec![],
            },
        ];
        
        let rewriter = JoinReorderer::new(CBOConfig::default());
        let ordered = rewriter.find_optimal_order(tables);
        
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].table_name, "small");
    }
}

fn default() -> TableStatistics {
    TableStatistics::default()
}
