//! Hyperion Query Planner
//! 
//! Converts logical query plans into physical execution plans.

use crate::common::*;
use std::collections::HashSet;

/// SQL expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    // Literals and constants
    Literal(Value),
    Parameter(usize),
    
    // Column references
    ColumnReference { name: String, table_alias: Option<String> },
    
    // Arithmetic operations
    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    
    // Comparison operations
    Equal(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    In { expr: Box<Expression>, values: Vec<Expression> },
    Between { expr: Box<Expression>, low: Box<Expression>, high: Box<Expression> },
    Like { expr: Box<Expression>, pattern: Box<Expression> },
    
    // Logical operations
    And(Vec<Expression>),
    Or(Vec<Expression>),
    Not(Box<Expression>),
    
    // Aggregate functions
    CountAll,
    Count { expr: Box<Expression>, distinct: bool },
    Sum { expr: Box<Expression>, distinct: bool },
    Avg { expr: Box<Expression>, distinct: bool },
    Min { expr: Box<Expression> },
    Max { expr: Box<Expression> },
    ArrayAgg { expr: Box<Expression>, distinct: bool },
    
    // Window functions
    RowNumber,
    Rank,
    DenseRank,
    Lag { expr: Box<Expression>, offset: Option<usize>, default: Option<Box<Expression>> },
    Lead { expr: Box<Expression>, offset: Option<usize>, default: Option<Box<Expression>> },
    FirstValue { expr: Box<Expression> },
    LastValue { expr: Box<Expression> },
    
    // Scalar functions
    Cast { expr: Box<Expression>, data_type: DataType },
    Coalesce(Vec<Expression>),
    NullIf(Box<Expression>, Box<Expression>),
    Concat(Vec<Expression>),
    Substring { expr: Box<Expression>, start: Box<Expression>, length: Option<Box<Expression>> },
    Upper(Box<Expression>),
    Lower(Box<Expression>),
    Trim(Box<Expression>),
    Length(Box<Expression>),
    Abs(Box<Expression>),
    Round { expr: Box<Expression>, digits: Option<Box<Expression>> },
    Floor(Box<Expression>),
    Ceil(Box<Expression>),
    CurrentDate,
    CurrentTimestamp,
    
    // Conditional
    Case { conditions: Vec<(Expression, Expression)>, else_expr: Option<Box<Expression>> },
}

impl Expression {
    /// Get all column references in this expression
    pub fn get_column_refs(&self) -> Vec<(Option<String>, String)> {
        match self {
            Expression::ColumnReference { name, table_alias } => {
                vec![(table_alias.clone(), name.clone())]
            }
            Expression::Add(l, r) | Expression::Subtract(l, r) 
            | Expression::Multiply(l, r) | Expression::Divide(l, r)
            | Expression::Modulo(l, r) | Expression::Equal(l, r)
            | Expression::NotEqual(l, r) | Expression::LessThan(l, r)
            | Expression::LessThanOrEqual(l, r) | Expression::GreaterThan(l, r)
            | Expression::GreaterThanOrEqual(l, r) => {
                let mut refs = l.get_column_refs();
                refs.extend(r.get_column_refs());
                refs
            }
            Expression::IsNull(e) | Expression::IsNotNull(e) 
            | Expression::Not(e) | Expression::Upper(e) | Expression::Lower(e)
            | Expression::Trim(e) | Expression::Length(e) | Expression::Abs(e)
            | Expression::Floor(e) | Expression::Ceil(e) => e.get_column_refs(),
            Expression::In { expr, values } | Expression::Concat(values) => {
                let mut refs = expr.get_column_refs();
                for v in values {
                    refs.extend(v.get_column_refs());
                }
                refs
            }
            Expression::Between { expr, low, high } => {
                let mut refs = expr.get_column_refs();
                refs.extend(low.get_column_refs());
                refs.extend(high.get_column_refs());
                refs
            }
            Expression::Like { expr, pattern } => {
                let mut refs = expr.get_column_refs();
                refs.extend(pattern.get_column_refs());
                refs
            }
            Expression::And(exprs) | Expression::Or(exprs) => {
                let mut refs = Vec::new();
                for e in exprs {
                    refs.extend(e.get_column_refs());
                }
                refs
            }
            Expression::Count(e) | Expression::Sum { expr: e, .. }
            | Expression::Avg { expr: e, .. } | Expression::Min(e) 
            | Expression::Max(e) | Expression::ArrayAgg { expr: e, .. }
            | Expression::Lag { expr: e, .. } | Expression::Lead { expr: e, .. }
            | Expression::FirstValue(e) | Expression::LastValue(e)
            | Expression::CurrentDate | Expression::CurrentTimestamp => {
                e.get_column_refs()
            }
            Expression::Case { conditions, else_expr } => {
                let mut refs = Vec::new();
                for (cond, then) in conditions {
                    refs.extend(cond.get_column_refs());
                    refs.extend(then.get_column_refs());
                }
                if let Some(e) = else_expr {
                    refs.extend(e.get_column_refs());
                }
                refs
            }
            Expression::Coalesce(exprs) | Expression::Substring { expr: _, length: _ } => {
                let mut refs = Vec::new();
                for e in exprs {
                    refs.extend(e.get_column_refs());
                }
                refs
            }
            _ => Vec::new(),
        }
    }

    /// Check if expression contains an aggregate function
    pub fn contains_aggregate(&self) -> bool {
        match self {
            Expression::CountAll => true,
            Expression::Count { .. } | Expression::Sum { .. } | Expression::Avg { .. }
            | Expression::Min(_) | Expression::Max(_) | Expression::ArrayAgg { .. } => true,
            Expression::And(exprs) | Expression::Or(exprs) => {
                exprs.iter().any(|e| e.contains_aggregate())
            }
            Expression::Add(l, r) | Expression::Subtract(l, r)
            | Expression::Multiply(l, r) | Expression::Divide(l, r)
            | Expression::Modulo(l, r) => {
                l.contains_aggregate() || r.contains_aggregate()
            }
            Expression::Equal(l, r) | Expression::NotEqual(l, r)
            | Expression::LessThan(l, r) | Expression::LessThanOrEqual(l, r)
            | Expression::GreaterThan(l, r) | Expression::GreaterThanOrEqual(l, r) => {
                l.contains_aggregate() || r.contains_aggregate()
            }
            Expression::Case { conditions, else_expr } => {
                conditions.iter().any(|(c, t)| c.contains_aggregate() || t.contains_aggregate())
                    || else_expr.as_ref().map_or(false, |e| e.contains_aggregate())
            }
            _ => false,
        }
    }
}

/// Sort specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    pub expression: Expression,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// Window frame specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFrameType {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

/// Window specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<SortSpec>,
    pub frame: Option<WindowFrame>,
}

/// Logical scan operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalScan {
    pub table_name: String,
    pub table_alias: Option<String>,
    pub output_columns: Vec<String>,
    pub predicate: Option<Expression>,
    pub partition_filter: Option<Expression>,
}

/// Logical join operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalJoin {
    pub join_type: JoinType,
    pub left_keys: Vec<Expression>,
    pub right_keys: Vec<Expression>,
    pub condition: Option<Expression>,
    pub output_schema: Vec<(String, DataType)>,
}

/// Logical aggregation operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalAggregate {
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<AggregateInfo>,
    pub having: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateInfo {
    pub function: AggregateFunction,
    pub arguments: Vec<Expression>,
    pub alias: String,
    pub filter: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
    ArrayAgg,
    StringAgg,
}

/// Logical set operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SetOperation {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalSetOperation {
    pub operation: SetOperation,
    pub all: bool,
}

/// Logical limit operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalLimit {
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

/// Base trait for all logical operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    Scan(LogicalScan),
    Projection {
        expressions: Vec<(Expression, String)>,
        input: Box<LogicalPlan>,
    },
    Filter {
        condition: Expression,
        input: Box<LogicalPlan>,
    },
    Join(LogicalJoin),
    Aggregate(Box<LogicalAggregate>),
    Window {
        window_specs: Vec<(String, WindowSpec)>,
        input: Box<LogicalPlan>,
    },
    Sort {
        sort_specs: Vec<SortSpec>,
        input: Box<LogicalPlan>,
    },
    Limit(Box<LogicalLimit>),
    SetOperation(Box<LogicalSetOperation>),
    TableWriter {
        table_name: String,
        write_mode: WriteMode,
        input: Box<LogicalPlan>,
    },
    Explain {
        plan: Box<LogicalPlan>,
        analyze: bool,
    },
    Values(Vec<Vec<Expression>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteMode {
    Insert,
    InsertOverwrite,
    Update,
    Delete,
    Merge,
}

/// Query optimizer
pub struct QueryOptimizer {
    config: OptimizerConfig,
}

#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    pub enable_predicate_pushdown: bool,
    pub enable_column_pruning: bool,
    pub enable_join_reordering: bool,
    pub enable_constant_folding: bool,
    pub enable_dedup: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_predicate_pushdown: true,
            enable_column_pruning: true,
            enable_join_reordering: true,
            enable_constant_folding: true,
            enable_dedup: true,
        }
    }
}

impl QueryOptimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        Self { config }
    }

    /// Optimize a logical plan
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut current = plan;
        
        // Phase 1: Logical optimizations
        if self.config.enable_column_pruning {
            current = self.column_pruning(current)?;
        }
        
        if self.config.enable_constant_folding {
            current = self.constant_folding(current)?;
        }
        
        if self.config.enable_predicate_pushdown {
            current = self.predicate_pushdown(current)?;
        }
        
        if self.config.enable_dedup {
            current = self.remove_redundant_operations(current)?;
        }
        
        Ok(current)
    }

    /// Remove unused columns from projections
    fn column_pruning(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Simplified column pruning - collect all required columns
        let required_cols = self.collect_required_columns(&plan, &HashSet::new());
        
        self.apply_column_pruning(plan, &required_cols)
    }

    fn collect_required_columns(&self, plan: &LogicalPlan, _aggregates: &HashSet<String>) -> HashSet<String> {
        let mut cols = HashSet::new();
        
        match plan {
            LogicalPlan::Projection { expressions, input } => {
                for (expr, _) in expressions {
                    for (_, col) in expr.get_column_refs() {
                        cols.insert(col);
                    }
                }
                cols.extend(self.collect_required_columns(input, &HashSet::new()));
            }
            LogicalPlan::Filter { condition, input } => {
                for (_, col) in condition.get_column_refs() {
                    cols.insert(col);
                }
                cols.extend(self.collect_required_columns(input, &HashSet::new()));
            }
            LogicalPlan::Scan(scan) => {
                for col in &scan.output_columns {
                    cols.insert(col.clone());
                }
            }
            _ => {}
        }
        
        cols
    }

    fn apply_column_pruning(&self, plan: LogicalPlan, _required: &HashSet<String>) -> Result<LogicalPlan> {
        // Simplified - just return the plan as-is for now
        Ok(plan)
    }

    /// Fold constant expressions
    fn constant_folding(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { expressions, input } => {
                let folded_exprs: Vec<_> = expressions
                    .into_iter()
                    .map(|(e, alias)| (self.fold_constants(e), alias))
                    .collect();
                Ok(LogicalPlan::Projection {
                    expressions: folded_exprs,
                    input: Box::new(self.constant_folding(*input)?),
                })
            }
            LogicalPlan::Filter { condition, input } => {
                Ok(LogicalPlan::Filter {
                    condition: self.fold_constants(condition),
                    input: Box::new(self.constant_folding(*input)?),
                })
            }
            _ => Ok(plan),
        }
    }

    fn fold_constants(&self, expr: Expression) -> Expression {
        // Simplified constant folding
        match expr {
            Expression::Add(l, r) => {
                let l = self.fold_constants(*l);
                let r = self.fold_constants(*r);
                match (&l, &r) {
                    (Expression::Literal(Value::Integer(a)), Expression::Literal(Value::Integer(b))) => {
                        Expression::Literal(Value::Integer(a + b))
                    }
                    _ => Expression::Add(Box::new(l), Box::new(r)),
                }
            }
            Expression::And(exprs) => {
                let folded: Vec<_> = exprs.into_iter().map(|e| self.fold_constants(e)).collect();
                // Short-circuit evaluation
                if folded.iter().any(|e| matches!(e, Expression::Literal(Value::Boolean(false)))) {
                    Expression::Literal(Value::Boolean(false))
                } else if folded.iter().all(|e| matches!(e, Expression::Literal(Value::Boolean(true)))) {
                    Expression::Literal(Value::Boolean(true))
                } else {
                    Expression::And(folded)
                }
            }
            _ => expr,
        }
    }

    /// Push predicates down through the plan
    fn predicate_pushdown(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { condition, input } => {
                let pushed = self.push_down_predicates(*input, condition)?;
                Ok(LogicalPlan::Filter {
                    condition: self.extract_top_predicates(pushed.1),
                    input: Box::new(pushed.0),
                })
            }
            _ => Ok(plan),
        }
    }

    fn push_down_predicates(&self, plan: LogicalPlan, predicate: Expression) -> Result<(LogicalPlan, Expression)> {
        match plan {
            LogicalPlan::Scan(mut scan) => {
                if let Some(existing) = scan.predicate.take() {
                    scan.predicate = Some(Expression::And(vec![existing, predicate.clone()]));
                } else {
                    scan.predicate = Some(predicate.clone());
                }
                Ok((LogicalPlan::Scan(scan), Expression::Literal(Value::Boolean(true))))
            }
            LogicalPlan::Filter { condition, input } => {
                Ok((LogicalPlan::Filter {
                    condition,
                    input,
                }, predicate))
            }
            LogicalPlan::Projection { expressions, input } => {
                // Only push down predicates that only reference projected columns
                let projected_cols: HashSet<_> = expressions.iter().map(|(_, n)| n.clone()).collect();
                let (new_input, remaining) = self.push_down_predicates(*input, predicate)?;
                Ok((LogicalPlan::Projection {
                    expressions,
                    input: Box::new(new_input),
                }, remaining))
            }
            _ => Ok((plan, predicate)),
        }
    }

    fn extract_top_predicates(&self, expr: Expression) -> Expression {
        expr
    }

    /// Remove redundant operations
    fn remove_redundant_operations(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { condition: Expression::Literal(Value::Boolean(true)), input } => {
                self.remove_redundant_operations(*input)
            }
            LogicalPlan::Sort { sort_specs, input } if sort_specs.is_empty() => {
                self.remove_redundant_operations(*input)
            }
            LogicalPlan::Limit(limit) if limit.limit.is_none() && limit.offset.is_none() => {
                Ok(*limit.input)
            }
            _ => Ok(plan),
        }
    }
}

/// SQL parser - simplified
pub struct SqlParser;

impl SqlParser {
    pub fn parse(sql: &str) -> Result<LogicalPlan> {
        // This is a simplified parser - in production, use a proper SQL parser
        // like sqlparser-rs
        Err(HyperionError::Parse(format!(
            "Use a proper SQL parser for: {}", sql
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_refs() {
        let expr = Expression::Add(
            Box::new(Expression::ColumnReference { name: "a".into(), table_alias: None }),
            Box::new(Expression::ColumnReference { name: "b".into(), table_alias: Some("t".into()) }),
        );
        let refs = expr.get_column_refs();
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn test_constant_folding() {
        let optimizer = QueryOptimizer::new(OptimizerConfig::default());
        let expr = Expression::Add(
            Box::new(Expression::Literal(Value::Integer(1))),
            Box::new(Expression::Literal(Value::Integer(2))),
        );
        let folded = optimizer.fold_constants(expr);
        assert!(matches!(folded, Expression::Literal(Value::Integer(3))));
    }
}
