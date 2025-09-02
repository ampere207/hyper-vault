use std::collections::HashMap;
use super::{parser::{ASTNode, WhereCondition}, query::Identifier, schema::Row, storage_engine::FileSystem};

pub struct QueryExecutor<'a> {
    filesystem: &'a mut FileSystem,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(filesystem: &'a mut FileSystem) -> Self {
        QueryExecutor { filesystem }
    }

    pub fn execute(&mut self, query: ASTNode) -> Result<Vec<Row>, ExecutionError> {
        match query {
            ASTNode::SelectStatement { projection, table, condition } => {
                Ok(self.execute_select(projection, table, condition)?)
            }
            ASTNode::DeleteStatement { table, condition } => {
                self.execute_delete(table, condition)?;
                Ok(vec![])
            }
            ASTNode::InsertStatement { table, columns, values } => {
                self.execute_insert(table, columns, values)?;
                Ok(vec![])
            }
            ASTNode::UpdateStatement { table, assignments, condition } => {
                self.execute_update(table, assignments, condition)?;
                Ok(vec![])
            }
            ASTNode::Identifier(_) => {
                Err(ExecutionError::InvalidQuery)
            }
        }
    }

    fn execute_select(
        &self,
        projection: Vec<Identifier>,
        table: Identifier,
        condition: Option<WhereCondition>,
    ) -> Result<Vec<Row>, ExecutionError> {
        let table = self
            .filesystem
            .storage_engine
            .tables
            .get(&table.0)
            .ok_or(ExecutionError::TableNotFound)?;

        let mut result = Vec::new();
        for row in table.rows.values() {
            // Apply WHERE condition if present
            if let Some(ref cond) = condition {
                if !cond.evaluate(row) {
                    continue;
                }
            }

            let mut row_data = HashMap::new();
            
            // Handle SELECT * or specific columns
            if projection.len() == 1 && projection[0].0 == "*" {
                // Select all columns
                for (key, value) in &row.data {
                    row_data.insert(key.clone(), value.clone());
                }
            } else {
                // Select specific columns
                for column in &projection {
                    row_data.insert(
                        column.0.clone(),
                        row.data.get(&column.0).cloned().unwrap_or_default(),
                    );
                }
            }
            
            result.push(Row { data: row_data });
        }

        Ok(result)
    }

    fn execute_insert(
        &mut self,
        table: Identifier,
        columns: Vec<Identifier>,
        values: Vec<String>,
    ) -> Result<(), ExecutionError> {
        let mut row_data = HashMap::new();
        
        if columns.is_empty() {
            // If no columns specified, assume values are in table column order
            if let Some(table_info) = self.filesystem.storage_engine.tables.get(&table.0) {
                for (i, column) in table_info.columns.iter().enumerate() {
                    if let Some(value) = values.get(i) {
                        row_data.insert(column.clone(), value.clone());
                    }
                }
            }
        } else {
            // Map columns to values
            for (i, column) in columns.iter().enumerate() {
                if let Some(value) = values.get(i) {
                    row_data.insert(column.0.clone(), value.clone());
                }
            }
        }

        let row = Row { data: row_data };
        self.filesystem.insert_row(&table.0, row)
            .map_err(|_| ExecutionError::InsertFailed)?;
        
        Ok(())
    }

    fn execute_update(
        &mut self,
        table: Identifier,
        assignments: Vec<(Identifier, String)>,
        condition: Option<WhereCondition>,
    ) -> Result<(), ExecutionError> {
        let mut updates = HashMap::new();
        for (column, value) in assignments {
            updates.insert(column.0, value);
        }

        let condition_fn = move |row: &Row| -> bool {
            if let Some(ref cond) = condition {
                cond.evaluate(row)
            } else {
                true // Update all rows if no condition
            }
        };

        self.filesystem.update_rows(&table.0, updates, condition_fn)
            .map_err(|_| ExecutionError::UpdateFailed)?;

        Ok(())
    }

    fn execute_delete(
        &mut self,
        table: Identifier,
        condition: Option<WhereCondition>,
    ) -> Result<(), ExecutionError> {
        let condition_fn = move |row: &Row| -> bool {
            if let Some(ref cond) = condition {
                cond.evaluate(row)
            } else {
                false // Don't delete all rows if no condition for safety
            }
        };

        self.filesystem.delete_rows(&table.0, condition_fn);
        Ok(())
    }
}

#[derive(Debug)]
pub enum ExecutionError {
    TableNotFound,
    InsertFailed,
    UpdateFailed,
    InvalidQuery,
}
