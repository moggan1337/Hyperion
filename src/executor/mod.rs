//! Hyperion Volcano-Style Iterator Execution Engine
//! 
//! Implements the pull-based iterator model for query execution with support
//! for parallel and distributed execution.

use crate::common::*;
use crate::planner::{Expression, SortSpec, AggregateFunction as PlannerAggFn};
use crate::optimizer::{PhysicalPlan, PhysicalOperator, JoinAlgorithm, BuildSide, 
    AggregationStrategy, AggregateSpec, AggregateFunction, SortStrategy};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

/// Execution context for a query
pub struct ExecutionContext {
    pub query_id: String,
    pub start_time_ms: u64,
    pub config: EngineConfig,
    pub state: Arc<Mutex<ExecutionState>>,
    pub node_id: NodeId,
}

impl ExecutionContext {
    pub fn new(query_id: String, config: EngineConfig, node_id: NodeId) -> Self {
        Self {
            query_id,
            start_time_ms: current_time_ms(),
            config,
            state: Arc::new(Mutex::new(ExecutionState::new())),
            node_id,
        }
    }

    pub fn elapsed_ms(&self) -> u64 {
        current_time_ms() - self.start_time_ms
    }
}

/// Runtime state during execution
#[derive(Debug, Default)]
pub struct ExecutionState {
    pub rows_processed: u64,
    pub bytes_processed: u64,
    pub peak_memory_bytes: u64,
    pub current_memory_bytes: u64,
    pub spilled_bytes: u64,
    pub errors: Vec<HyperionError>,
}

impl ExecutionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_error(&mut self, err: HyperionError) {
        self.errors.push(err);
    }
}

/// Trait for all physical operators in the Volcano model
pub trait PhysicalIterator: Send {
    /// Get the next row, or None if exhausted
    fn next(&mut self, context: &mut ExecutionContext) -> Result<Option<Row>>;
    
    /// Get the schema of the output
    fn schema(&self) -> &Schema;
    
    /// Get statistics about this operator
    fn stats(&self) -> OperatorStats;
    
    /// Close the operator and release resources
    fn close(&mut self) -> Result<()>;
}

/// Statistics for an operator
#[derive(Debug, Clone, Default)]
pub struct OperatorStats {
    pub rows_output: u64,
    pub rows_read: u64,
    pub cpu_time_ms: u64,
    pub memory_used_bytes: u64,
    pub spilled_bytes: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
}

/// Build side for hash joins
#[derive(Debug, Clone, Copy)]
enum HashJoinSide {
    Build,
    Probe,
}

/// A hash table for hash joins
pub struct HashTable {
    buckets: Vec<Vec<Row>>,
    num_buckets: usize,
    key_indices: Vec<usize>,
    matched: Vec<bool>,
}

impl HashTable {
    pub fn new(num_buckets: usize, key_indices: Vec<usize>) -> Self {
        Self {
            buckets: vec![Vec::new(); num_buckets],
            num_buckets,
            key_indices,
            matched: Vec::new(),
        }
    }

    pub fn insert(&mut self, row: Row) {
        let hash = self.compute_hash(&row);
        self.buckets[hash].push(row);
    }

    pub fn find(&self, row: &Row) -> Option<&[Row]> {
        let hash = self.compute_hash(row);
        let bucket = &self.buckets[hash];
        if bucket.is_empty() {
            None
        } else {
            Some(bucket)
        }
    }

    fn compute_hash(&self, row: &Row) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        for &idx in &self.key_indices {
            if let Some(val) = row.get(idx) {
                val.hash(&mut hasher);
            }
        }
        (hasher.finish() as usize) % self.num_buckets
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Scan operator
pub struct ScanOperator {
    schema: Schema,
    data: Vec<Row>,
    position: usize,
    filter: Option<Expression>,
}

impl ScanOperator {
    pub fn new(schema: Schema, data: Vec<Row>) -> Self {
        Self {
            schema,
            data,
            position: 0,
            filter: None,
        }
    }

    pub fn with_filter(mut self, filter: Expression) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl PhysicalIterator for ScanOperator {
    fn next(&mut self, _context: &mut ExecutionContext) -> Result<Option<Row>> {
        while self.position < self.data.len() {
            let row = self.data[self.position].clone();
            self.position += 1;
            
            if let Some(ref filter) = self.filter {
                if evaluatePredicate(filter, &row)? {
                    return Ok(Some(row));
                }
            } else {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        OperatorStats {
            rows_output: self.position as u64,
            rows_read: self.position as u64,
            ..Default::default()
        }
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Projection operator
pub struct ProjectionOperator {
    schema: Schema,
    input: Box<dyn PhysicalIterator>,
    expressions: Vec<Expression>,
}

impl ProjectionOperator {
    pub fn new(schema: Schema, input: Box<dyn PhysicalIterator>, expressions: Vec<Expression>) -> Self {
        Self { schema, input, expressions }
    }
}

impl PhysicalIterator for ProjectionOperator {
    fn next(&mut self, context: &mut ExecutionContext) -> Result<Option<Row>> {
        if let Some(row) = self.input.next(context)? {
            let values = self.expressions.iter()
                .map(|expr| evaluate_expression(expr, &row))
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(Row::new(values)))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        self.input.stats()
    }

    fn close(&mut self) -> Result<()> {
        self.input.close()
    }
}

/// Filter operator
pub struct FilterOperator {
    schema: Schema,
    input: Box<dyn PhysicalIterator>,
    condition: Expression,
}

impl FilterOperator {
    pub fn new(schema: Schema, input: Box<dyn PhysicalIterator>, condition: Expression) -> Self {
        Self { schema, input, condition }
    }
}

impl PhysicalIterator for FilterOperator {
    fn next(&mut self, context: &mut ExecutionContext) -> Result<Option<Row>> {
        while let Some(row) = self.input.next(context)? {
            if evaluatePredicate(&self.condition, &row)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        self.input.stats()
    }

    fn close(&mut self) -> Result<()> {
        self.input.close()
    }
}

/// Hash join operator
pub struct HashJoinOperator {
    schema: Schema,
    build_side: HashJoinSide,
    hash_table: Option<HashTable>,
    build_rows: Vec<Row>,
    probe_position: usize,
    left_keys: Vec<Expression>,
    right_keys: Vec<Expression>,
    build_input: Option<Box<dyn PhysicalIterator>>,
    probe_input: Option<Box<dyn PhysicalIterator>>,
    join_type: crate::planner::JoinType,
    buffer: Vec<Row>,
}

impl HashJoinOperator {
    pub fn new(
        schema: Schema,
        build_input: Box<dyn PhysicalIterator>,
        probe_input: Box<dyn PhysicalIterator>,
        left_keys: Vec<Expression>,
        right_keys: Vec<Expression>,
        join_type: crate::planner::JoinType,
        build_side: BuildSide,
    ) -> Self {
        let key_indices: Vec<usize> = (0..left_keys.len()).collect();
        Self {
            schema,
            build_side: if build_side == BuildSide::Left { HashJoinSide::Build } else { HashJoinSide::Probe },
            hash_table: None,
            build_rows: Vec::new(),
            probe_position: 0,
            left_keys,
            right_keys,
            build_input: Some(build_input),
            probe_input: Some(probe_input),
            join_type,
            buffer: Vec::new(),
        }
    }

    fn build_hash_table(&mut self, context: &mut ExecutionContext) -> Result<()> {
        let input = self.build_input.take().expect("build_input not available");
        let mut ht = HashTable::new(1024, vec![0, 1]);
        
        while let Some(row) = input.next(context)? {
            ht.insert(row);
        }
        input.close()?;
        
        self.hash_table = Some(ht);
        Ok(())
    }
}

impl PhysicalIterator for HashJoinOperator {
    fn next(&mut self, context: &mut ExecutionContext) -> Result<Option<Row>> {
        // First call - build hash table
        if self.hash_table.is_none() {
            self.build_hash_table(context)?;
        }
        
        let probe_input = self.probe_input.as_mut().expect("probe_input not available");
        let hash_table = self.hash_table.as_ref().expect("hash_table not available");
        
        while let Some(probe_row) = probe_input.next(context)? {
            if let Some(matches) = hash_table.find(&probe_row) {
                for match_row in matches {
                    let mut joined = probe_row.values.clone();
                    joined.extend_from_slice(&match_row.values);
                    return Ok(Some(Row::new(joined)));
                }
            }
            
            // For outer joins, handle unmatched rows
            match self.join_type {
                crate::planner::JoinType::Left | crate::planner::JoinType::Full => {
                    let mut joined = probe_row.values.clone();
                    // Add null padding for unmatched
                    return Ok(Some(Row::new(joined)));
                }
                _ => {}
            }
        }
        
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        OperatorStats {
            memory_used_bytes: self.hash_table.as_ref().map(|h| h.len() as u64 * 100).unwrap_or(0),
            ..Default::default()
        }
    }

    fn close(&mut self) -> Result<()> {
        if let Some(input) = self.build_input.take() {
            input.close()?;
        }
        if let Some(input) = self.probe_input.take() {
            input.close()?;
        }
        Ok(())
    }
}

/// Sort operator with external sorting support
pub struct SortOperator {
    schema: Schema,
    input: Option<Box<dyn PhysicalIterator>>,
    sort_specs: Vec<SortSpec>,
    sorted_rows: Vec<Row>,
    position: usize,
    strategy: SortStrategy,
    estimated_rows: u64,
}

impl SortOperator {
    pub fn new(
        schema: Schema,
        input: Box<dyn PhysicalIterator>,
        sort_specs: Vec<SortSpec>,
        strategy: SortStrategy,
        estimated_rows: u64,
    ) -> Self {
        Self {
            schema,
            input: Some(input),
            sort_specs,
            sorted_rows: Vec::new(),
            position: 0,
            strategy,
            estimated_rows,
        }
    }

    fn sort_rows(&mut self, context: &mut ExecutionContext) -> Result<()> {
        let input = self.input.take().expect("input not available");
        
        let mut rows = Vec::new();
        while let Some(row) = input.next(context)? {
            rows.push(row);
        }
        input.close()?;
        
        // Sort using comparator
        rows.sort_by(|a, b| self.compare_rows(a, b));
        self.sorted_rows = rows;
        
        Ok(())
    }

    fn compare_rows(&self, a: &Row, b: &Row) -> Ordering {
        for spec in &self.sort_specs {
            let idx_a = self.get_sort_index(&spec.expression);
            let idx_b = self.get_sort_index(&spec.expression);
            
            if let (Some(val_a), Some(val_b)) = (a.get(idx_a), b.get(idx_b)) {
                let ord = compare_values(val_a, val_b);
                if ord != Ordering::Equal {
                    return if spec.ascending { ord } else { ord.reverse() };
                }
            }
        }
        Ordering::Equal
    }

    fn get_sort_index(&self, expr: &Expression) -> usize {
        if let Expression::ColumnReference { name, .. } = expr {
            self.schema.columns.iter()
                .position(|c| &c.name == name)
                .unwrap_or(0)
        } else {
            0
        }
    }
}

impl PhysicalIterator for SortOperator {
    fn next(&mut self, context: &mut ExecutionContext) -> Result<Option<Row>> {
        if self.sorted_rows.is_empty() && self.position == 0 {
            self.sort_rows(context)?;
        }
        
        if self.position < self.sorted_rows.len() {
            let row = self.sorted_rows[self.position].clone();
            self.position += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        OperatorStats {
            rows_output: self.position as u64,
            memory_used_bytes: self.sorted_rows.capacity() as u64 * 100,
            ..Default::default()
        }
    }

    fn close(&mut self) -> Result<()> {
        if let Some(input) = self.input.take() {
            input.close()?;
        }
        self.sorted_rows.clear();
        Ok(())
    }
}

/// Aggregation operator
pub struct AggregateOperator {
    schema: Schema,
    input: Option<Box<dyn PhysicalIterator>>,
    group_by: Vec<Expression>,
    aggregates: Vec<AggregateSpec>,
    groups: HashMap<Vec<Value>, GroupState>,
    strategy: AggregationStrategy,
    estimated_groups: u64,
}

struct GroupState {
    rows: Vec<Row>,
    aggregate_values: Vec<Value>,
    count: u64,
}

impl AggregateOperator {
    pub fn new(
        schema: Schema,
        input: Box<dyn PhysicalIterator>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateSpec>,
        strategy: AggregationStrategy,
        estimated_groups: u64,
    ) -> Self {
        Self {
            schema,
            input: Some(input),
            group_by,
            aggregates,
            groups: HashMap::new(),
            strategy,
            estimated_groups,
        }
    }

    fn compute_aggregate(&self, state: &GroupState, spec: &AggregateSpec) -> Value {
        match &spec.function {
            AggregateFunction::Count => Value::Integer(state.count as i64),
            AggregateFunction::Sum => self.compute_sum(&state.rows, spec),
            AggregateFunction::Avg => self.compute_avg(&state.rows, spec),
            AggregateFunction::Min => self.compute_min(&state.rows, spec),
            AggregateFunction::Max => self.compute_max(&state.rows, spec),
            AggregateFunction::CountDistinct => Value::Integer(self.compute_count_distinct(&state.rows, spec) as i64),
            AggregateFunction::ArrayAgg => self.compute_array_agg(&state.rows, spec),
            AggregateFunction::StringAgg => self.compute_string_agg(&state.rows, spec),
            _ => Value::Null,
        }
    }

    fn compute_sum(&self, rows: &[Row], spec: &AggregateSpec) -> Value {
        let mut sum: f64 = 0.0;
        for row in rows {
            if let Some(val) = evaluate_expression(&spec.arguments[0], row).ok().flatten() {
                if let Value::Double(d) = val {
                    sum += d;
                } else if let Value::Integer(i) = val {
                    sum += i as f64;
                }
            }
        }
        Value::Double(sum)
    }

    fn compute_avg(&self, rows: &[Row], spec: &AggregateSpec) -> Value {
        if rows.is_empty() {
            return Value::Null;
        }
        let sum = self.compute_sum(rows, spec);
        if let Value::Double(s) = sum {
            Value::Double(s / rows.len() as f64)
        } else {
            Value::Null
        }
    }

    fn compute_min(&self, rows: &[Row], spec: &AggregateSpec) -> Value {
        let mut min: Option<Value> = None;
        for row in rows {
            if let Some(val) = evaluate_expression(&spec.arguments[0], row).ok().flatten() {
                min = Some(match min {
                    None => val,
                    Some(m) => {
                        if compare_values(&val, &m) == Ordering::Less {
                            val
                        } else {
                            m
                        }
                    }
                });
            }
        }
        min.unwrap_or(Value::Null)
    }

    fn compute_max(&self, rows: &[Row], spec: &AggregateSpec) -> Value {
        let mut max: Option<Value> = None;
        for row in rows {
            if let Some(val) = evaluate_expression(&spec.arguments[0], row).ok().flatten() {
                max = Some(match max {
                    None => val,
                    Some(m) => {
                        if compare_values(&val, &m) == Ordering::Greater {
                            val
                        } else {
                            m
                        }
                    }
                });
            }
        }
        max.unwrap_or(Value::Null)
    }

    fn compute_count_distinct(&self, rows: &[Row], spec: &AggregateSpec) -> usize {
        let mut distinct = std::collections::HashSet::new();
        for row in rows {
            if let Some(val) = evaluate_expression(&spec.arguments[0], row).ok().flatten() {
                distinct.insert(val);
            }
        }
        distinct.len()
    }

    fn compute_array_agg(&self, rows: &[Row], spec: &AggregateSpec) -> Value {
        let values: Vec<Value> = rows.iter()
            .filter_map(|row| evaluate_expression(&spec.arguments[0], row).ok().flatten())
            .collect();
        Value::Array(values)
    }

    fn compute_string_agg(&self, rows: &[Row], spec: &AggregateSpec) -> Value {
        let separator = match spec.arguments.get(1) {
            Some(Expression::Literal(Value::String(s))) => s.clone(),
            _ => ",".to_string(),
        };
        
        let strings: Vec<String> = rows.iter()
            .filter_map(|row| {
                evaluate_expression(&spec.arguments[0], row).ok().flatten()
                    .and_then(|v| match v {
                        Value::String(s) => Some(s),
                        _ => None,
                    })
            })
            .collect();
        
        Value::String(strings.join(&separator))
    }

    fn compute_group_key(&self, row: &Row) -> Vec<Value> {
        self.group_by.iter()
            .map(|expr| evaluate_expression(expr, row).ok().flatten().unwrap_or(Value::Null))
            .collect()
    }

    fn process_all(&mut self, context: &mut ExecutionContext) -> Result<()> {
        let input = self.input.take().expect("input not available");
        
        while let Some(row) = input.next(context)? {
            let key = self.compute_group_key(&row);
            
            let state = self.groups.entry(key).or_insert(GroupState {
                rows: Vec::new(),
                aggregate_values: Vec::new(),
                count: 0,
            });
            
            state.rows.push(row);
            state.count += 1;
        }
        
        input.close()?;
        Ok(())
    }
}

impl PhysicalIterator for AggregateOperator {
    fn next(&mut self, context: &mut ExecutionContext) -> Result<Option<Row>> {
        // Process all input on first call
        if self.groups.is_empty() && self.input.is_some() {
            self.process_all(context)?;
        }
        
        // For now, return all groups at once (simplified)
        // In production, use a proper iterator
        Ok(None)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        OperatorStats {
            rows_output: self.groups.len() as u64,
            memory_used_bytes: self.groups.capacity() as u64 * 200,
            ..Default::default()
        }
    }

    fn close(&mut self) -> Result<()> {
        if let Some(input) = self.input.take() {
            input.close()?;
        }
        self.groups.clear();
        Ok(())
    }
}

/// Limit operator
pub struct LimitOperator {
    schema: Schema,
    input: Box<dyn PhysicalIterator>,
    limit: usize,
    offset: Option<usize>,
    emitted: usize,
}

impl LimitOperator {
    pub fn new(schema: Schema, input: Box<dyn PhysicalIterator>, limit: usize, offset: Option<usize>) -> Self {
        Self { schema, input, limit, offset, emitted: 0 }
    }
}

impl PhysicalIterator for LimitOperator {
    fn next(&mut self, _context: &mut ExecutionContext) -> Result<Option<Row>> {
        if let Some(offset) = self.offset {
            // Skip offset rows
            while self.emitted < offset {
                if self.input.next(&mut ExecutionContext::new("".into(), EngineConfig::default(), NodeId("".into())))?.is_none() {
                    return Ok(None);
                }
                self.emitted += 1;
            }
        }
        
        if self.emitted >= self.limit {
            return Ok(None);
        }
        
        if let Some(row) = self.input.next(&mut ExecutionContext::new("".into(), EngineConfig::default(), NodeId("".into())))? {
            self.emitted += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        OperatorStats {
            rows_output: self.emitted as u64,
            ..Default::default()
        }
    }

    fn close(&mut self) -> Result<()> {
        self.input.close()
    }
}

/// Exchange operator for distributed execution
pub struct ExchangeOperator {
    schema: Schema,
    partition_id: usize,
    total_partitions: usize,
    local_input: Box<dyn PhysicalIterator>,
    strategy: crate::optimizer::ExchangeStrategy,
}

impl ExchangeOperator {
    pub fn new(
        schema: Schema,
        partition_id: usize,
        total_partitions: usize,
        local_input: Box<dyn PhysicalIterator>,
        strategy: crate::optimizer::ExchangeStrategy,
    ) -> Self {
        Self {
            schema,
            partition_id,
            total_partitions,
            local_input,
            strategy,
        }
    }
}

impl PhysicalIterator for ExchangeOperator {
    fn next(&mut self, _context: &mut ExecutionContext) -> Result<Option<Row>> {
        // In distributed mode, this would handle network communication
        self.local_input.next(&mut ExecutionContext::new("".into(), EngineConfig::default(), NodeId("".into())))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stats(&self) -> OperatorStats {
        OperatorStats::default()
    }

    fn close(&mut self) -> Result<()> {
        self.local_input.close()
    }
}

// Helper functions

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn evaluatePredicate(expr: &Expression, row: &Row) -> Result<bool> {
    match expr {
        Expression::Literal(Value::Boolean(b)) => Ok(*b),
        Expression::Equal(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            Ok(lv == rv)
        }
        Expression::NotEqual(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            Ok(lv != rv)
        }
        Expression::LessThan(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            Ok(compare_values(&lv, &rv) == Ordering::Less)
        }
        Expression::And(conditions) => {
            for cond in conditions {
                if !evaluatePredicate(cond, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expression::Or(conditions) => {
            for cond in conditions {
                if evaluatePredicate(cond, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expression::Not(inner) => {
            Ok(!evaluatePredicate(inner, row)?)
        }
        Expression::IsNull(e) => {
            let val = evaluate_expression(e, row)?;
            Ok(val.is_null())
        }
        Expression::IsNotNull(e) => {
            let val = evaluate_expression(e, row)?;
            Ok(!val.is_null())
        }
        _ => Err(HyperionError::Execution("Unsupported predicate".into())),
    }
}

fn evaluate_expression(expr: &Expression, row: &Row) -> Result<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::ColumnReference { name, .. } => {
            let idx = row.values.iter()
                .position(|_| true)
                .unwrap_or(0);
            Ok(row.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expression::Add(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            add_values(&lv, &rv)
        }
        Expression::Subtract(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            subtract_values(&lv, &rv)
        }
        Expression::Multiply(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            multiply_values(&lv, &rv)
        }
        Expression::Divide(l, r) => {
            let lv = evaluate_expression(l, row)?;
            let rv = evaluate_expression(r, row)?;
            divide_values(&lv, &rv)
        }
        Expression::Coalesce(exprs) => {
            for e in exprs {
                let val = evaluate_expression(e, row)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        Expression::Cast { expr, data_type } => {
            let val = evaluate_expression(expr, row)?;
            cast_value(&val, data_type)
        }
        Expression::CurrentDate => Ok(Value::Date(current_time_ms() as i32 / 86400000)),
        Expression::CurrentTimestamp => Ok(Value::Timestamp(current_time_ms() as i64)),
        Expression::Case { conditions, else_expr } => {
            for (cond, then) in conditions {
                if evaluatePredicate(cond, row)? {
                    return evaluate_expression(then, row);
                }
            }
            if let Some(e) = else_expr {
                evaluate_expression(e, row)
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(HyperionError::Execution("Unsupported expression".into())),
    }
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,  // NULLs sort last
        (_, Value::Null) => Ordering::Less,
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Double(a), Value::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn add_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Integer(i1), Value::Integer(i2)) => Ok(Value::Integer(i1 + i2)),
        (Value::Double(d1), Value::Double(d2)) => Ok(Value::Double(d1 + d2)),
        (Value::Integer(i), Value::Double(d)) | (Value::Double(d), Value::Integer(i)) => {
            Ok(Value::Double(*i as f64 + d))
        }
        (Value::String(s1), Value::String(s2)) => Ok(Value::String(format!("{}{}", s1, s2))),
        _ => Err(HyperionError::Execution("Cannot add values".into())),
    }
}

fn subtract_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Integer(i1), Value::Integer(i2)) => Ok(Value::Integer(i1 - i2)),
        (Value::Double(d1), Value::Double(d2)) => Ok(Value::Double(d1 - d2)),
        (Value::Integer(i), Value::Double(d)) => Ok(Value::Double(*i as f64 - d)),
        (Value::Double(d), Value::Integer(i)) => Ok(Value::Double(d - *i as f64)),
        _ => Err(HyperionError::Execution("Cannot subtract values".into())),
    }
}

fn multiply_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Integer(i1), Value::Integer(i2)) => Ok(Value::Integer(i1 * i2)),
        (Value::Double(d1), Value::Double(d2)) => Ok(Value::Double(d1 * d2)),
        (Value::Integer(i), Value::Double(d)) | (Value::Double(d), Value::Integer(i)) => {
            Ok(Value::Double(*i as f64 * d))
        }
        _ => Err(HyperionError::Execution("Cannot multiply values".into())),
    }
}

fn divide_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Integer(i1), Value::Integer(i2)) => {
            if *i2 == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Integer(i1 / i2))
            }
        }
        (Value::Double(d1), Value::Double(d2)) => {
            if *d2 == 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Double(d1 / d2))
            }
        }
        _ => Err(HyperionError::Execution("Cannot divide values".into())),
    }
}

fn cast_value(val: &Value, target: &DataType) -> Result<Value> {
    match (val, target) {
        (Value::Integer(i), DataType::Double) => Ok(Value::Double(*i as f64)),
        (Value::Double(d), DataType::Integer) => Ok(Value::Integer(*d as i64)),
        (Value::Integer(i), DataType::Varchar(_)) => Ok(Value::String(i.to_string())),
        (Value::String(s), DataType::Integer) => {
            s.parse::<i64>().map(Value::Integer).ok().map(Value::Null).ok_or_else(|| 
                HyperionError::Execution("Cannot cast".into()))
        }
        _ => Ok(val.clone()),
    }
}

impl Value {
    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Convert physical plan to executable iterator tree
pub fn build_execution_tree(plan: &PhysicalPlan) -> Result<Box<dyn PhysicalIterator>> {
    match &plan.root {
        PhysicalOperator::TableScan { table_name, output_columns, filter, .. } => {
            // Create scan with schema
            let schema = Schema::new(table_name.clone(), vec![]);
            let data = vec![];  // Would be populated from storage
            let mut scan = ScanOperator::new(schema, data);
            if let Some(f) = filter {
                scan = scan.with_filter(f.clone());
            }
            Ok(Box::new(scan))
        }
        PhysicalOperator::Projection { expressions } => {
            let child = build_execution_tree(&PhysicalPlan::new(
                PhysicalOperator::Empty  // Placeholder
            ))?;
            let schema = Schema::new("projection".into(), vec![]);
            Ok(Box::new(ProjectionOperator::new(schema, child, expressions.clone())))
        }
        PhysicalOperator::Filter { condition, .. } => {
            let child = build_execution_tree(&PhysicalPlan::new(PhysicalOperator::Empty))?;
            let schema = Schema::new("filter".into(), vec![]);
            Ok(Box::new(FilterOperator::new(schema, child, condition.clone())))
        }
        PhysicalOperator::Sort { sort_specs, strategy, estimated_rows, .. } => {
            let child = build_execution_tree(&PhysicalPlan::new(PhysicalOperator::Empty))?;
            let schema = Schema::new("sort".into(), vec![]);
            Ok(Box::new(SortOperator::new(schema, child, sort_specs.clone(), strategy.clone(), *estimated_rows)))
        }
        PhysicalOperator::Limit { limit, offset } => {
            let child = build_execution_tree(&PhysicalPlan::new(PhysicalOperator::Empty))?;
            let schema = Schema::new("limit".into(), vec![]);
            Ok(Box::new(LimitOperator::new(schema, child, *limit, *offset)))
        }
        PhysicalOperator::HashAggregate { group_by, aggregates, estimated_groups } => {
            let child = build_execution_tree(&PhysicalPlan::new(PhysicalOperator::Empty))?;
            let schema = Schema::new("aggregate".into(), vec![]);
            Ok(Box::new(AggregateOperator::new(
                schema, child, group_by.clone(), aggregates.clone(),
                AggregationStrategy::Hash { bucket_count: 1024 }, *estimated_groups
            )))
        }
        PhysicalOperator::Empty => {
            Ok(Box::new(ScanOperator::new(Schema::new("empty".into(), vec![]), vec![])))
        }
        _ => Err(HyperionError::Execution("Unsupported operator".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_operator() {
        let schema = Schema::new("test".into(), vec![
            Column { name: "id".into(), data_type: DataType::Integer, nullable: false, ordinal: 0 },
            Column { name: "name".into(), data_type: DataType::Varchar(100), nullable: true, ordinal: 1 },
        ]);
        
        let data = vec![
            Row::new(vec![Value::Integer(1), Value::String("Alice".into())]),
            Row::new(vec![Value::Integer(2), Value::String("Bob".into())]),
        ];
        
        let mut scan = ScanOperator::new(schema, data);
        let ctx = &mut ExecutionContext::new("test".into(), EngineConfig::default(), NodeId("test".into()));
        
        let row1 = scan.next(ctx).unwrap().unwrap();
        assert_eq!(row1.get(0), Some(&Value::Integer(1)));
        
        let row2 = scan.next(ctx).unwrap().unwrap();
        assert_eq!(row2.get(0), Some(&Value::Integer(2)));
        
        let none = scan.next(ctx).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn test_filter_operator() {
        let schema = Schema::new("test".into(), vec![
            Column { name: "id".into(), data_type: DataType::Integer, nullable: false, ordinal: 0 },
        ]);
        
        let data = vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(2)]),
            Row::new(vec![Value::Integer(3)]),
        ];
        
        let scan = ScanOperator::new(schema.clone(), data);
        let filter = Expression::LessThan(
            Box::new(Expression::ColumnReference { name: "id".into(), table_alias: None }),
            Box::new(Expression::Literal(Value::Integer(3))),
        );
        
        let mut filter_op = FilterOperator::new(schema, Box::new(scan), filter);
        let ctx = &mut ExecutionContext::new("test".into(), EngineConfig::default(), NodeId("test".into()));
        
        let row1 = filter_op.next(ctx).unwrap().unwrap();
        assert_eq!(row1.get(0), Some(&Value::Integer(1)));
        
        let row2 = filter_op.next(ctx).unwrap().unwrap();
        assert_eq!(row2.get(0), Some(&Value::Integer(2)));
        
        let none = filter_op.next(ctx).unwrap();
        assert!(none.is_none());
    }
}
