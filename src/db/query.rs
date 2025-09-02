use super::parser::{ASTNode, WhereCondition};
use super::schema::Row;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Identifier(pub String);

impl From<String> for Identifier {
    fn from(s: String) -> Self {
        Identifier(s)
    }
}

impl From<&str> for Identifier {
    fn from(s: &str) -> Self {
        Identifier(s.to_string())
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub query_type: QueryType,
    pub table: Identifier,
    pub projection: Option<Vec<Identifier>>,
    pub condition: Option<WhereCondition>,
    pub assignments: Option<Vec<(Identifier, String)>>,
    pub insert_data: Option<(Vec<Identifier>, Vec<String>)>,
    pub estimated_cost: f64,
    pub execution_steps: Vec<ExecutionStep>,
}

#[derive(Debug, Clone)]
pub enum ExecutionStep {
    TableScan {
        table: String,
        estimated_rows: usize,
    },
    FilterRows {
        condition: WhereCondition,
        estimated_selectivity: f64,
    },
    ProjectColumns {
        columns: Vec<String>,
    },
    InsertRow {
        table: String,
        columns: Vec<String>,
        values: Vec<String>,
    },
    UpdateRows {
        table: String,
        assignments: Vec<(String, String)>,
        condition: Option<WhereCondition>,
    },
    DeleteRows {
        table: String,
        condition: Option<WhereCondition>,
    },
}

#[derive(Debug)]
pub struct QueryStatistics {
    pub total_queries: u64,
    pub select_queries: u64,
    pub insert_queries: u64,
    pub update_queries: u64,
    pub delete_queries: u64,
    pub failed_queries: u64,
    pub average_execution_time: f64,
}

impl Default for QueryStatistics {
    fn default() -> Self {
        QueryStatistics {
            total_queries: 0,
            select_queries: 0,
            insert_queries: 0,
            update_queries: 0,
            delete_queries: 0,
            failed_queries: 0,
            average_execution_time: 0.0,
        }
    }
}

#[derive(Debug)]
pub struct QueryOptimizer {
    pub enable_optimizations: bool,
    pub statistics: QueryStatistics,
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        QueryOptimizer {
            enable_optimizations: true,
            statistics: QueryStatistics::default(),
        }
    }
}

impl QueryOptimizer {
    pub fn new() -> Self {
        QueryOptimizer::default()
    }

    pub fn optimize_plan(&self, plan: &mut QueryPlan) {
        if !self.enable_optimizations {
            return;
        }

        // Simple optimization rules
        self.optimize_where_clause(plan);
        self.optimize_projection(plan);
        self.estimate_cost(plan);
    }

    fn optimize_where_clause(&self, plan: &mut QueryPlan) {
        // Future: Add WHERE clause optimization logic
        // For now, just ensure the condition is properly structured
        if let Some(ref condition) = plan.condition {
            // Add index usage hints or condition reordering here
            plan.execution_steps.push(ExecutionStep::FilterRows {
                condition: condition.clone(),
                estimated_selectivity: self.estimate_selectivity(condition),
            });
        }
    }

    fn optimize_projection(&self, plan: &mut QueryPlan) {
        // Optimize column projection
        if let Some(ref projection) = plan.projection {
            if projection.len() == 1 && projection[0].0 == "*" {
                // SELECT * - no optimization needed
                return;
            }
            
            plan.execution_steps.push(ExecutionStep::ProjectColumns {
                columns: projection.iter().map(|id| id.0.clone()).collect(),
            });
        }
    }

    fn estimate_selectivity(&self, _condition: &WhereCondition) -> f64 {
        // Simple selectivity estimation
        // In a real database, this would use statistics
        match _condition.operator.as_str() {
            "=" => 0.1,    // Equality is usually selective
            ">" | "<" => 0.3,  // Range queries are less selective
            ">=" | "<=" => 0.4,
            "!=" | "<>" => 0.9, // Not equal is usually not very selective
            _ => 0.5,
        }
    }

    fn estimate_cost(&self, plan: &mut QueryPlan) {
        let mut cost = 0.0;

        for step in &plan.execution_steps {
            cost += match step {
                ExecutionStep::TableScan { estimated_rows, .. } => {
                    *estimated_rows as f64 * 0.1 // Base cost per row scan
                }
                ExecutionStep::FilterRows { estimated_selectivity, .. } => {
                    100.0 * (1.0 - estimated_selectivity) // Cost increases with lower selectivity
                }
                ExecutionStep::ProjectColumns { columns } => {
                    columns.len() as f64 * 0.5 // Cost per column projection
                }
                ExecutionStep::InsertRow { .. } => 50.0, // Fixed cost for insert
                ExecutionStep::UpdateRows { .. } => 75.0, // Fixed cost for update
                ExecutionStep::DeleteRows { .. } => 25.0, // Fixed cost for delete
            };
        }

        plan.estimated_cost = cost;
    }

    pub fn update_statistics(&mut self, query_type: &QueryType, execution_time: f64, success: bool) {
        self.statistics.total_queries += 1;
        
        if !success {
            self.statistics.failed_queries += 1;
            return;
        }

        match query_type {
            QueryType::Select => self.statistics.select_queries += 1,
            QueryType::Insert => self.statistics.insert_queries += 1,
            QueryType::Update => self.statistics.update_queries += 1,
            QueryType::Delete => self.statistics.delete_queries += 1,
        }

        // Update average execution time
        let total_successful = self.statistics.total_queries - self.statistics.failed_queries;
        self.statistics.average_execution_time = 
            (self.statistics.average_execution_time * (total_successful - 1) as f64 + execution_time) 
            / total_successful as f64;
    }
}

pub struct QueryPlanner {
    pub optimizer: QueryOptimizer,
}

impl QueryPlanner {
    pub fn new() -> Self {
        QueryPlanner {
            optimizer: QueryOptimizer::new(),
        }
    }

    pub fn plan(&mut self, ast: &ASTNode) -> Result<QueryPlan, PlanningError> {
        let mut plan = match ast {
            ASTNode::SelectStatement { projection, table, condition } => {
                let mut steps = vec![
                    ExecutionStep::TableScan {
                        table: table.0.clone(),
                        estimated_rows: 1000, // Default estimate
                    }
                ];

                QueryPlan {
                    query_type: QueryType::Select,
                    table: table.clone(),
                    projection: Some(projection.clone()),
                    condition: condition.clone(),
                    assignments: None,
                    insert_data: None,
                    estimated_cost: 0.0,
                    execution_steps: steps,
                }
            }
            ASTNode::InsertStatement { table, columns, values } => {
                let steps = vec![
                    ExecutionStep::InsertRow {
                        table: table.0.clone(),
                        columns: columns.iter().map(|id| id.0.clone()).collect(),
                        values: values.clone(),
                    }
                ];

                QueryPlan {
                    query_type: QueryType::Insert,
                    table: table.clone(),
                    projection: None,
                    condition: None,
                    assignments: None,
                    insert_data: Some((columns.clone(), values.clone())),
                    estimated_cost: 0.0,
                    execution_steps: steps,
                }
            }
            ASTNode::UpdateStatement { table, assignments, condition } => {
                let steps = vec![
                    ExecutionStep::UpdateRows {
                        table: table.0.clone(),
                        assignments: assignments.iter()
                            .map(|(id, val)| (id.0.clone(), val.clone()))
                            .collect(),
                        condition: condition.clone(),
                    }
                ];

                QueryPlan {
                    query_type: QueryType::Update,
                    table: table.clone(),
                    projection: None,
                    condition: condition.clone(),
                    assignments: Some(assignments.clone()),
                    insert_data: None,
                    estimated_cost: 0.0,
                    execution_steps: steps,
                }
            }
            ASTNode::DeleteStatement { table, condition } => {
                let steps = vec![
                    ExecutionStep::DeleteRows {
                        table: table.0.clone(),
                        condition: condition.clone(),
                    }
                ];

                QueryPlan {
                    query_type: QueryType::Delete,
                    table: table.clone(),
                    projection: None,
                    condition: condition.clone(),
                    assignments: None,
                    insert_data: None,
                    estimated_cost: 0.0,
                    execution_steps: steps,
                }
            }
            ASTNode::Identifier(_) => {
                return Err(PlanningError::InvalidQuery("Standalone identifier not supported".to_string()));
            }
        };

        // Apply optimizations
        self.optimizer.optimize_plan(&mut plan);

        Ok(plan)
    }

    pub fn validate_plan(&self, plan: &QueryPlan, table_exists: bool, columns: &[String]) -> Result<(), PlanningError> {
        // Validate table exists
        if !table_exists {
            return Err(PlanningError::TableNotFound(plan.table.0.clone()));
        }

        // Validate columns exist for SELECT queries
        if let Some(ref projection) = plan.projection {
            for column in projection {
                if column.0 != "*" && !columns.contains(&column.0) {
                    return Err(PlanningError::ColumnNotFound(column.0.clone()));
                }
            }
        }

        // Validate WHERE clause columns
        if let Some(ref condition) = plan.condition {
            if !columns.contains(&condition.column) {
                return Err(PlanningError::ColumnNotFound(condition.column.clone()));
            }
        }

        // Validate UPDATE assignments
        if let Some(ref assignments) = plan.assignments {
            for (column, _) in assignments {
                if !columns.contains(&column.0) {
                    return Err(PlanningError::ColumnNotFound(column.0.clone()));
                }
            }
        }

        // Validate INSERT columns
        if let Some((ref insert_columns, ref values)) = plan.insert_data {
            if !insert_columns.is_empty() {
                for column in insert_columns {
                    if !columns.contains(&column.0) {
                        return Err(PlanningError::ColumnNotFound(column.0.clone()));
                    }
                }
                
                if insert_columns.len() != values.len() {
                    return Err(PlanningError::InvalidQuery(
                        "Column count doesn't match value count".to_string()
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn get_statistics(&self) -> &QueryStatistics {
        &self.optimizer.statistics
    }

    pub fn reset_statistics(&mut self) {
        self.optimizer.statistics = QueryStatistics::default();
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum PlanningError {
    TableNotFound(String),
    ColumnNotFound(String),
    InvalidQuery(String),
    OptimizationFailed(String),
}

impl std::fmt::Display for PlanningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanningError::TableNotFound(table) => write!(f, "Table '{}' not found", table),
            PlanningError::ColumnNotFound(column) => write!(f, "Column '{}' not found", column),
            PlanningError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            PlanningError::OptimizationFailed(msg) => write!(f, "Optimization failed: {}", msg),
        }
    }
}

impl std::error::Error for PlanningError {}

// Helper functions for query analysis
pub fn analyze_query_complexity(plan: &QueryPlan) -> QueryComplexity {
    let mut complexity_score = 0;

    // Base complexity for query type
    complexity_score += match plan.query_type {
        QueryType::Select => 1,
        QueryType::Insert => 2,
        QueryType::Update => 3,
        QueryType::Delete => 2,
    };

    // Add complexity for WHERE clause
    if plan.condition.is_some() {
        complexity_score += 2;
    }

    // Add complexity for projections
    if let Some(ref projection) = plan.projection {
        if projection.len() > 5 {
            complexity_score += 1;
        }
    }

    match complexity_score {
        1..=2 => QueryComplexity::Simple,
        3..=5 => QueryComplexity::Medium,
        _ => QueryComplexity::Complex,
    }
}

#[derive(Debug, PartialEq)]
pub enum QueryComplexity {
    Simple,
    Medium,
    Complex,
}

// Query cache for future optimization
pub struct QueryCache {
    cache: HashMap<String, QueryPlan>,
    max_size: usize,
}

impl QueryCache {
    pub fn new(max_size: usize) -> Self {
        QueryCache {
            cache: HashMap::new(),
            max_size,
        }
    }

    pub fn get(&self, query_hash: &str) -> Option<&QueryPlan> {
        self.cache.get(query_hash)
    }

    pub fn put(&mut self, query_hash: String, plan: QueryPlan) {
        if self.cache.len() >= self.max_size {
            // Simple eviction: remove first entry (FIFO)
            if let Some(first_key) = self.cache.keys().next().cloned() {
                self.cache.remove(&first_key);
            }
        }
        self.cache.insert(query_hash, plan);
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}
