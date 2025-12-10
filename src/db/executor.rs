use std::collections::HashMap;
use super::advanced_sql::{apply_limit_offset, execute_aggregate, execute_group_by, execute_join, execute_order_by};
use super::concurrency::LockType;
use super::parser::{AggregateFunc, ASTNode, OrderByItem, ProjectionItem, WhereCondition};
use super::query::Identifier;
use super::schema::Row;
use super::storage_engine::FileSystem;
use super::wal::WalEntryType;

pub struct QueryExecutor<'a> {
    filesystem: &'a mut FileSystem,
    pub tx_id: Option<u64>,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(filesystem: &'a mut FileSystem) -> Self {
        QueryExecutor {
            filesystem,
            tx_id: None,
        }
    }

    pub fn with_transaction(filesystem: &'a mut FileSystem, tx_id: u64) -> Self {
        QueryExecutor {
            filesystem,
            tx_id: Some(tx_id),
        }
    }

    pub fn execute(&mut self, query: ASTNode) -> Result<Vec<Row>, ExecutionError> {
        match query {
            ASTNode::BeginTransaction => {
                let tx_id = self.filesystem.storage_engine.transaction_manager.begin_transaction();
                self.tx_id = Some(tx_id);
                Ok(vec![])
            }
            ASTNode::CommitTransaction => {
                if let Some(tx_id) = self.tx_id {
                    self.commit_transaction(tx_id)?;
                    self.tx_id = None;
                } else {
                    return Err(ExecutionError::NoActiveTransaction);
                }
                Ok(vec![])
            }
            ASTNode::RollbackTransaction => {
                if let Some(tx_id) = self.tx_id {
                    self.rollback_transaction(tx_id)?;
                    self.tx_id = None;
                } else {
                    return Err(ExecutionError::NoActiveTransaction);
                }
                Ok(vec![])
            }
            ASTNode::SelectStatement {
                projection,
                table,
                joins,
                condition,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                let tx_id = self.tx_id.unwrap_or(0);
                Ok(self.execute_select(
                    projection,
                    table,
                    joins,
                    condition,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                    tx_id,
                )?)
            }
            ASTNode::DeleteStatement { table, condition } => {
                let tx_id = self.tx_id.unwrap_or(0);
                self.execute_delete(table, condition, tx_id)?;
                Ok(vec![])
            }
            ASTNode::InsertStatement { table, columns, values } => {
                let tx_id = self.tx_id.unwrap_or(0);
                self.execute_insert(table, columns, values, tx_id)?;
                Ok(vec![])
            }
            ASTNode::UpdateStatement {
                table,
                assignments,
                condition,
            } => {
                let tx_id = self.tx_id.unwrap_or(0);
                self.execute_update(table, assignments, condition, tx_id)?;
                Ok(vec![])
            }
            ASTNode::Identifier(_) => Err(ExecutionError::InvalidQuery),
        }
    }

    fn execute_select(
        &self,
        projection: Vec<ProjectionItem>,
        table: Identifier,
        joins: Vec<super::parser::JoinClause>,
        condition: Option<WhereCondition>,
        group_by: Option<Vec<Identifier>>,
        having: Option<WhereCondition>,
        order_by: Option<Vec<OrderByItem>>,
        limit: Option<usize>,
        offset: Option<usize>,
        tx_id: u64,
    ) -> Result<Vec<Row>, ExecutionError> {
        let table_name = table.0.clone();

        if tx_id > 0 {
            self.filesystem
                .storage_engine
                .lock_manager
                .acquire_lock(&table_name, tx_id, LockType::Shared)
                .map_err(|e| ExecutionError::LockError(e))?;
        }

        let table_data = self
            .filesystem
            .storage_engine
            .tables
            .get(&table_name)
            .ok_or(ExecutionError::TableNotFound)?;

        let mut result: Vec<Row> = table_data.rows.values().cloned().collect();

        for join_clause in &joins {
            let right_table_name = join_clause.table.0.clone();
            
            if tx_id > 0 {
                self.filesystem
                    .storage_engine
                    .lock_manager
                    .acquire_lock(&right_table_name, tx_id, LockType::Shared)
                    .map_err(|e| ExecutionError::LockError(e))?;
            }

            let right_table = self
                .filesystem
                .storage_engine
                .tables
                .get(&right_table_name)
                .ok_or(ExecutionError::TableNotFound)?;

            let right_rows: Vec<Row> = right_table.rows.values().cloned().collect();
            result = execute_join(&result, &right_rows, &join_clause.join_type, &join_clause.condition);
        }

        if let Some(ref cond) = condition {
            result.retain(|row| cond.evaluate(row));
        }

        if let Some(ref group_cols) = group_by {
            let group_columns: Vec<String> = group_cols.iter().map(|id| id.0.clone()).collect();
            let aggregates: Vec<(AggregateFunc, Option<String>)> = projection
                .iter()
                .filter_map(|item| match item {
                    ProjectionItem::Aggregate { func, column } => {
                        Some((func.clone(), column.as_ref().map(|c| c.0.clone())))
                    }
                    _ => None,
                })
                .collect();

            if !aggregates.is_empty() {
                result = execute_group_by(&result, &group_columns, &aggregates);
            }

            if let Some(ref having_cond) = having {
                result.retain(|row| having_cond.evaluate(row));
            }
        } else {
            let has_aggregates = projection.iter().any(|item| matches!(item, ProjectionItem::Aggregate { .. }));
            if has_aggregates && result.len() == 1 {
                let mut new_result = Vec::new();
                let mut row_data = HashMap::new();
                for item in &projection {
                    match item {
                        ProjectionItem::Aggregate { func, column } => {
                            if let Some(value) = execute_aggregate(&result, func, column.as_ref().map(|c| c.0.as_str())) {
                                let agg_name = match func {
                                    AggregateFunc::Count => "COUNT",
                                    AggregateFunc::Sum => "SUM",
                                    AggregateFunc::Avg => "AVG",
                                    AggregateFunc::Max => "MAX",
                                    AggregateFunc::Min => "MIN",
                                };
                                let col_name = column.as_ref().map(|c| c.0.as_str()).unwrap_or("*");
                                row_data.insert(format!("{}({})", agg_name, col_name), value);
                            }
                        }
                        ProjectionItem::Column(id) => {
                            if let Some(value) = result[0].data.get(&id.0) {
                                row_data.insert(id.0.clone(), value.clone());
                            }
                        }
                        ProjectionItem::All => {
                            row_data = result[0].data.clone();
                        }
                    }
                }
                new_result.push(Row { data: row_data });
                result = new_result;
            } else {
                let mut new_result = Vec::new();
                for row in &result {
                    let mut row_data = HashMap::new();
                    for item in &projection {
                        match item {
                            ProjectionItem::Column(id) => {
                                if let Some(value) = row.data.get(&id.0) {
                                    row_data.insert(id.0.clone(), value.clone());
                                }
                            }
                            ProjectionItem::All => {
                                row_data = row.data.clone();
                                break;
                            }
                            ProjectionItem::Aggregate { .. } => {}
                        }
                    }
                    new_result.push(Row { data: row_data });
                }
                result = new_result;
            }
        }

        if let Some(ref order_items) = order_by {
            execute_order_by(&mut result, order_items);
        }

        result = apply_limit_offset(&result, limit, offset);

        Ok(result)
    }

    fn execute_insert(
        &mut self,
        table: Identifier,
        columns: Vec<Identifier>,
        values: Vec<String>,
        tx_id: u64,
    ) -> Result<(), ExecutionError> {
        let table_name = table.0.clone();

        if tx_id > 0 {
            self.filesystem
                .storage_engine
                .lock_manager
                .acquire_lock(&table_name, tx_id, LockType::Exclusive)
                .map_err(|e| ExecutionError::LockError(e))?;

            if let Some(tx) = self.filesystem.storage_engine.transaction_manager.get_transaction_mut(tx_id) {
                tx.add_write(table_name.clone());
            }
        }

        let mut row_data = HashMap::new();

        if columns.is_empty() {
            if let Some(table_info) = self.filesystem.storage_engine.tables.get(&table_name) {
                for (i, column) in table_info.columns.iter().enumerate() {
                    if let Some(value) = values.get(i) {
                        row_data.insert(column.clone(), value.clone());
                    }
                }
            }
        } else {
            for (i, column) in columns.iter().enumerate() {
                if let Some(value) = values.get(i) {
                    row_data.insert(column.0.clone(), value.clone());
                }
            }
        }

        let row = Row { data: row_data.clone() };
        let row_id = self
            .filesystem
            .storage_engine
            .tables
            .get(&table_name)
            .map(|t| t.rows.len())
            .unwrap_or(0);

        if tx_id > 0 {
            let wal_entry = self
                .filesystem
                .wal_manager
                .create_entry(tx_id, WalEntryType::Insert {
                    table: table_name.clone(),
                    row_id,
                    data: row.clone(),
                })
                .map_err(|e| ExecutionError::WalError(e.to_string()))?;

            self.filesystem.wal_manager.append_entry(wal_entry.clone()).map_err(|e| ExecutionError::WalError(e.to_string()))?;

            if let Some(tx) = self.filesystem.storage_engine.transaction_manager.get_transaction_mut(tx_id) {
                tx.add_wal_entry(wal_entry);
            }
        }

        self.filesystem
            .insert_row(&table_name, row)
            .map_err(|_| ExecutionError::InsertFailed)?;

        Ok(())
    }

    fn execute_update(
        &mut self,
        table: Identifier,
        assignments: Vec<(Identifier, String)>,
        condition: Option<WhereCondition>,
        tx_id: u64,
    ) -> Result<(), ExecutionError> {
        let table_name = table.0.clone();

        if tx_id > 0 {
            self.filesystem
                .storage_engine
                .lock_manager
                .acquire_lock(&table_name, tx_id, LockType::Exclusive)
                .map_err(|e| ExecutionError::LockError(e))?;

            if let Some(tx) = self.filesystem.storage_engine.transaction_manager.get_transaction_mut(tx_id) {
                tx.add_write(table_name.clone());
            }
        }

        let mut updates = HashMap::new();
        for (column, value) in assignments {
            updates.insert(column.0, value);
        }

        let table_data = self
            .filesystem
            .storage_engine
            .tables
            .get(&table_name)
            .ok_or(ExecutionError::TableNotFound)?;

        let rows_to_update: Vec<(usize, Row, Row)> = table_data
            .rows
            .iter()
            .filter_map(|(id, row)| {
                let should_update = condition.as_ref().map(|c| c.evaluate(row)).unwrap_or(true);
                if should_update {
                    let mut new_row = row.clone();
                    for (col, val) in &updates {
                        new_row.data.insert(col.clone(), val.clone());
                    }
                    Some((*id, row.clone(), new_row))
                } else {
                    None
                }
            })
            .collect();

        for (row_id, old_row, new_row) in &rows_to_update {
            if tx_id > 0 {
                let wal_entry = self
                    .filesystem
                    .wal_manager
                    .create_entry(tx_id, WalEntryType::Update {
                        table: table_name.clone(),
                        row_id: *row_id,
                        old_data: old_row.clone(),
                        new_data: new_row.clone(),
                    })
                    .map_err(|e| ExecutionError::WalError(e.to_string()))?;

                self.filesystem.wal_manager.append_entry(wal_entry.clone()).map_err(|e| ExecutionError::WalError(e.to_string()))?;

                if let Some(tx) = self.filesystem.storage_engine.transaction_manager.get_transaction_mut(tx_id) {
                    tx.add_wal_entry(wal_entry);
                }
            }
        }

        let condition_fn = move |row: &Row| -> bool {
            condition.as_ref().map(|c| c.evaluate(row)).unwrap_or(true)
        };

        self.filesystem
            .update_rows(&table_name, updates, condition_fn)
            .map_err(|_| ExecutionError::UpdateFailed)?;

        Ok(())
    }

    fn execute_delete(
        &mut self,
        table: Identifier,
        condition: Option<WhereCondition>,
        tx_id: u64,
    ) -> Result<(), ExecutionError> {
        let table_name = table.0.clone();

        if tx_id > 0 {
            self.filesystem
                .storage_engine
                .lock_manager
                .acquire_lock(&table_name, tx_id, LockType::Exclusive)
                .map_err(|e| ExecutionError::LockError(e))?;

            if let Some(tx) = self.filesystem.storage_engine.transaction_manager.get_transaction_mut(tx_id) {
                tx.add_write(table_name.clone());
            }
        }

        let table_data = self
            .filesystem
            .storage_engine
            .tables
            .get(&table_name)
            .ok_or(ExecutionError::TableNotFound)?;

        let rows_to_delete: Vec<(usize, Row)> = table_data
            .rows
            .iter()
            .filter_map(|(id, row)| {
                let should_delete = condition.as_ref().map(|c| c.evaluate(row)).unwrap_or(false);
                if should_delete {
                    Some((*id, row.clone()))
                } else {
                    None
                }
            })
            .collect();

        for (row_id, row_data) in &rows_to_delete {
            if tx_id > 0 {
                let wal_entry = self
                    .filesystem
                    .wal_manager
                    .create_entry(tx_id, WalEntryType::Delete {
                        table: table_name.clone(),
                        row_id: *row_id,
                        data: row_data.clone(),
                    })
                    .map_err(|e| ExecutionError::WalError(e.to_string()))?;

                self.filesystem.wal_manager.append_entry(wal_entry.clone()).map_err(|e| ExecutionError::WalError(e.to_string()))?;

                if let Some(tx) = self.filesystem.storage_engine.transaction_manager.get_transaction_mut(tx_id) {
                    tx.add_wal_entry(wal_entry);
                }
            }
        }

        let condition_fn = move |row: &Row| -> bool {
            condition.as_ref().map(|c| c.evaluate(row)).unwrap_or(false)
        };

        self.filesystem.delete_rows(&table_name, condition_fn);

        Ok(())
    }

    fn commit_transaction(&mut self, tx_id: u64) -> Result<(), ExecutionError> {
        let commit_entry = self
            .filesystem
            .wal_manager
            .create_entry(tx_id, WalEntryType::Commit { tx_id })
            .map_err(|e| ExecutionError::WalError(e.to_string()))?;

        self.filesystem.wal_manager.append_entry(commit_entry).map_err(|e| ExecutionError::WalError(e.to_string()))?;

        self.filesystem
            .storage_engine
            .transaction_manager
            .commit_transaction(tx_id)
            .map_err(|e| ExecutionError::TransactionError(e))?;

        let tables_written: Vec<String> = self
            .filesystem
            .storage_engine
            .transaction_manager
            .get_transaction(tx_id)
            .map(|tx| tx.write_set.iter().cloned().collect())
            .unwrap_or_default();

        for table in tables_written {
            self.filesystem
                .storage_engine
                .lock_manager
                .release_lock(&table, tx_id)
                .map_err(|e| ExecutionError::LockError(e))?;
        }

        if self.filesystem.wal_manager.should_checkpoint() {
            self.filesystem.wal_manager.checkpoint().map_err(|e| ExecutionError::WalError(e.to_string()))?;
            self.filesystem.save_to_file().map_err(|_| ExecutionError::SaveFailed)?;
        }

        self.filesystem
            .storage_engine
            .transaction_manager
            .remove_transaction(tx_id);

        Ok(())
    }

    fn rollback_transaction(&mut self, tx_id: u64) -> Result<(), ExecutionError> {
        let abort_entry = self
            .filesystem
            .wal_manager
            .create_entry(tx_id, WalEntryType::Abort { tx_id })
            .map_err(|e| ExecutionError::WalError(e.to_string()))?;

        self.filesystem.wal_manager.append_entry(abort_entry).map_err(|e| ExecutionError::WalError(e.to_string()))?;

        let tables_written: Vec<String> = self
            .filesystem
            .storage_engine
            .transaction_manager
            .get_transaction(tx_id)
            .map(|tx| tx.write_set.iter().cloned().collect())
            .unwrap_or_default();

        for table in tables_written {
            self.filesystem
                .storage_engine
                .lock_manager
                .release_lock(&table, tx_id)
                .map_err(|e| ExecutionError::LockError(e))?;
        }

        self.filesystem
            .storage_engine
            .transaction_manager
            .rollback_transaction(tx_id)
            .map_err(|e| ExecutionError::TransactionError(e))?;

        self.filesystem
            .storage_engine
            .transaction_manager
            .remove_transaction(tx_id);

        Ok(())
    }
}

#[derive(Debug)]
pub enum ExecutionError {
    TableNotFound,
    InsertFailed,
    UpdateFailed,
    InvalidQuery,
    NoActiveTransaction,
    TransactionError(String),
    LockError(String),
    WalError(String),
    SaveFailed,
}

