use super::query::{QueryStatistics, PlanningError};
use super::schema::{Row, Table};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StorageEngine {
    pub tables: HashMap<String, Table>,
    pub metadata: StorageMetadata,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StorageMetadata {
    pub version: String,
    pub created_at: u64,
    pub last_modified: u64,
    pub total_operations: u64,
    pub total_tables_created: u64,
    pub total_rows_inserted: u64,
    pub total_rows_updated: u64,
    pub total_rows_deleted: u64,
}

impl Default for StorageMetadata {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        StorageMetadata {
            version: "1.0.0".to_string(),
            created_at: now,
            last_modified: now,
            total_operations: 0,
            total_tables_created: 0,
            total_rows_inserted: 0,
            total_rows_updated: 0,
            total_rows_deleted: 0,
        }
    }
}

impl StorageMetadata {
    fn update_timestamp(&mut self) {
        self.last_modified = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.total_operations += 1;
    }
}

impl StorageEngine {
    pub fn new() -> Self {
        StorageEngine {
            tables: HashMap::new(),
            metadata: StorageMetadata::default(),
        }
    }

    /// Get table statistics for query optimization
    pub fn get_table_stats(&self, table_name: &str) -> Option<TableStatistics> {
        if let Some(table) = self.tables.get(table_name) {
            let mut column_stats = HashMap::new();
            
            // Calculate basic column statistics
            for column in &table.columns {
                let mut unique_values = std::collections::HashSet::new();
                let mut total_values = 0;
                
                for row in table.rows.values() {
                    if let Some(value) = row.data.get(column) {
                        unique_values.insert(value.clone());
                        total_values += 1;
                    }
                }
                
                column_stats.insert(column.clone(), ColumnStatistics {
                    unique_values: unique_values.len(),
                    total_values,
                    selectivity: if total_values > 0 { 
                        unique_values.len() as f64 / total_values as f64 
                    } else { 
                        1.0 
                    },
                });
            }
            
            Some(TableStatistics {
                row_count: table.rows.len(),
                column_stats,
                last_updated: self.metadata.last_modified,
            })
        } else {
            None
        }
    }

    /// Validate table schema
    pub fn validate_table_schema(&self, table_name: &str, columns: &[String]) -> Result<(), StorageError> {
        if let Some(table) = self.tables.get(table_name) {
            for column in columns {
                if !table.columns.contains(column) {
                    return Err(StorageError::ColumnNotFound {
                        table: table_name.to_string(),
                        column: column.clone(),
                    });
                }
            }
            Ok(())
        } else {
            Err(StorageError::TableNotFound(table_name.to_string()))
        }
    }

    /// Create a new table with enhanced validation
    pub fn create_table(&mut self, name: &str, columns: Vec<String>, primary_key: Option<&str>) -> Result<(), StorageError> {
        // Validate table name
        if name.trim().is_empty() {
            return Err(StorageError::InvalidTableName(name.to_string()));
        }
        
        if self.tables.contains_key(name) {
            return Err(StorageError::TableAlreadyExists(name.to_string()));
        }

        // Validate columns
        if columns.is_empty() {
            return Err(StorageError::InvalidSchema("Table must have at least one column".to_string()));
        }

        // Check for duplicate column names
        let mut unique_columns = std::collections::HashSet::new();
        for column in &columns {
            if !unique_columns.insert(column.clone()) {
                return Err(StorageError::InvalidSchema(format!("Duplicate column name: {}", column)));
            }
        }

        // Validate primary key
        if let Some(pk) = primary_key {
            if !columns.contains(&pk.to_string()) {
                return Err(StorageError::InvalidSchema(
                    format!("Primary key '{}' must be one of the table columns", pk)
                ));
            }
        }

        self.tables.insert(
            name.to_string(),
            Table {
                columns,
                rows: HashMap::new(),
                primary_key: primary_key.map(String::from),
            },
        );

        self.metadata.update_timestamp();
        self.metadata.total_tables_created += 1;
        Ok(())
    }

    /// Insert a row with enhanced validation
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), StorageError> {
        // Get immutable reference first for validation
        let table = self.tables.get(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        // Validate row data against table schema
        self.validate_row_data(&row, table)?;

        // Now get mutable reference for insertion
        let table = self.tables.get_mut(table_name).unwrap();

        // Validate primary key uniqueness
        if let Some(pk) = &table.primary_key {
            if let Some(pk_value) = row.data.get(pk) {
                // Check for existing primary key
                for existing_row in table.rows.values() {
                    if let Some(existing_pk_value) = existing_row.data.get(pk) {
                        if existing_pk_value == pk_value {
                            return Err(StorageError::PrimaryKeyViolation {
                                table: table_name.to_string(),
                                key: pk.clone(),
                                value: pk_value.clone(),
                            });
                        }
                    }
                }
            } else {
                return Err(StorageError::MissingPrimaryKey {
                    table: table_name.to_string(),
                    key: pk.clone(),
                });
            }
        }

        let row_id = table.rows.len();
        table.rows.insert(row_id, row);
        
        self.metadata.update_timestamp();
        self.metadata.total_rows_inserted += 1;
        Ok(())
    }

    /// Update rows with enhanced error handling
    pub fn update_rows<F>(
        &mut self,
        table_name: &str,
        updates: HashMap<String, String>,
        condition: F,
    ) -> Result<usize, StorageError>
    where
        F: Fn(&Row) -> bool,
    {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        // Validate update columns exist
        for column in updates.keys() {
            if !table.columns.contains(column) {
                return Err(StorageError::ColumnNotFound {
                    table: table_name.to_string(),
                    column: column.clone(),
                });
            }
        }

        // Check primary key constraints for updates
        if let Some(pk) = &table.primary_key {
            if let Some(new_pk_value) = updates.get(pk) {
                // Check if the new primary key value would create a duplicate
                for row in table.rows.values() {
                    if !condition(row) { // Skip rows that won't be updated
                        if let Some(existing_pk_value) = row.data.get(pk) {
                            if existing_pk_value == new_pk_value {
                                return Err(StorageError::PrimaryKeyViolation {
                                    table: table_name.to_string(),
                                    key: pk.clone(),
                                    value: new_pk_value.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        let mut updated_count = 0;
        for row in table.rows.values_mut() {
            if condition(row) {
                for (column, value) in &updates {
                    row.data.insert(column.clone(), value.clone());
                }
                updated_count += 1;
            }
        }

        if updated_count > 0 {
            self.metadata.update_timestamp();
            self.metadata.total_rows_updated += updated_count as u64;
        }

        Ok(updated_count)
    }

    /// Delete rows with count tracking
    pub fn delete_rows<F>(&mut self, table_name: &str, condition: F) -> Result<usize, StorageError>
    where
        F: Fn(&Row) -> bool,
    {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        let initial_count = table.rows.len();
        table.rows.retain(|_, row| !condition(row));
        let deleted_count = initial_count - table.rows.len();

        if deleted_count > 0 {
            self.metadata.update_timestamp();
            self.metadata.total_rows_deleted += deleted_count as u64;
        }

        Ok(deleted_count)
    }

    /// Drop a table
    pub fn drop_table(&mut self, table_name: &str) -> Result<(), StorageError> {
        if self.tables.remove(table_name).is_some() {
            self.metadata.update_timestamp();
            Ok(())
        } else {
            Err(StorageError::TableNotFound(table_name.to_string()))
        }
    }

    /// Get all table names
    pub fn get_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get table info
    pub fn get_table_info(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }

    /// Validate row data against table schema
    fn validate_row_data(&self, row: &Row, table: &Table) -> Result<(), StorageError> {
        // Check for unknown columns
        for column in row.data.keys() {
            if !table.columns.contains(column) {
                return Err(StorageError::ColumnNotFound {
                    table: "unknown".to_string(), // We don't have table name here
                    column: column.clone(),
                });
            }
        }
        Ok(())
    }

    /// Serialize storage engine
    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), std::io::Error> {
        buffer.clear();
        match bincode::serialize(self) {
            Ok(data) => {
                buffer.extend(data);
                Ok(())
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Serialization failed: {}", e),
            )),
        }
    }

    /// Deserialize storage engine
    pub fn deserialize(buffer: &[u8]) -> Result<Self, std::io::Error> {
        match bincode::deserialize(buffer) {
            Ok(engine) => Ok(engine),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Deserialization failed: {}", e),
            )),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileSystem {
    pub storage_engine: StorageEngine,
    file_path: String,
}

impl FileSystem {
    pub fn new(file_path: &str) -> Self {
        let storage_engine = if Path::new(file_path).exists() {
            FileSystem::load_from_file(file_path).unwrap_or_else(|_| StorageEngine::new())
        } else {
            StorageEngine::new()
        };

        FileSystem {
            storage_engine,
            file_path: file_path.to_string(),
        }
    }

    /// Create table with file persistence
    pub fn create_table(&mut self, name: &str, columns: Vec<String>, primary_key: Option<&str>) {
        if let Err(e) = self.storage_engine.create_table(name, columns, primary_key) {
            eprintln!("Failed to create table: {}", e);
            return;
        }
        if let Err(e) = self.save_to_file() {
            eprintln!("Failed to save after table creation: {}", e);
        }
    }

    /// Insert row with file persistence
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), std::io::Error> {
        match self.storage_engine.insert_row(table_name, row) {
            Ok(_) => self.save_to_file(),
            Err(e) => Err(Error::new(ErrorKind::InvalidInput, e.to_string())),
        }
    }

    /// Update rows with file persistence
    pub fn update_rows<F>(
        &mut self,
        table_name: &str,
        updates: HashMap<String, String>,
        condition: F,
    ) -> Result<Vec<Row>, String>
    where
        F: Fn(&Row) -> bool,
    {
        match self.storage_engine.update_rows(table_name, updates.clone(), condition) {
            Ok(count) => {
                if let Err(e) = self.save_to_file() {
                    return Err(format!("Failed to save after update: {}", e));
                }
                // Return a dummy row to maintain compatibility
                let mut result_row_data = HashMap::new();
                for (key, value) in updates {
                    result_row_data.insert(key, value);
                }
                Ok(vec![Row { data: result_row_data }])
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// Delete rows with file persistence
    pub fn delete_rows<F>(&mut self, table_name: &str, condition: F)
    where
        F: Fn(&Row) -> bool,
    {
        match self.storage_engine.delete_rows(table_name, condition) {
            Ok(count) => {
                if count > 0 {
                    if let Err(e) = self.save_to_file() {
                        eprintln!("Failed to save after delete: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to delete rows: {}", e);
            }
        }
    }

    /// Fetch rows for SELECT queries
    pub fn fetch_rows(
        &self,
        table: &Table,
        projection: Vec<super::query::Identifier>,
    ) -> Result<Vec<Row>, String> {
        let mut result = Vec::new();
        for row in table.rows.values() {
            let mut row_data = HashMap::new();
            for column in &projection {
                row_data.insert(
                    column.0.clone(),
                    row.data.get(&column.0).cloned().unwrap_or_default(),
                );
            }
            result.push(Row { data: row_data });
        }
        Ok(result)
    }

    /// Get storage statistics
    pub fn get_statistics(&self) -> &StorageMetadata {
        &self.storage_engine.metadata
    }

    /// Save storage engine to file
    fn save_to_file(&self) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)?;
        let mut buffer = Vec::new();
        self.storage_engine.serialize(&mut buffer)?;
        file.write_all(&buffer)?;
        Ok(())
    }

    /// Load storage engine from file
    fn load_from_file(file_path: &str) -> Result<StorageEngine, std::io::Error> {
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        StorageEngine::deserialize(&buffer)
    }
}

// Enhanced error types
#[derive(Debug)]
pub enum StorageError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound { table: String, column: String },
    InvalidTableName(String),
    InvalidSchema(String),
    PrimaryKeyViolation { table: String, key: String, value: String },
    MissingPrimaryKey { table: String, key: String },
    IoError(std::io::Error),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::TableNotFound(table) => write!(f, "Table '{}' not found", table),
            StorageError::TableAlreadyExists(table) => write!(f, "Table '{}' already exists", table),
            StorageError::ColumnNotFound { table, column } => {
                write!(f, "Column '{}' not found in table '{}'", column, table)
            }
            StorageError::InvalidTableName(name) => write!(f, "Invalid table name: '{}'", name),
            StorageError::InvalidSchema(msg) => write!(f, "Invalid schema: {}", msg),
            StorageError::PrimaryKeyViolation { table, key, value } => {
                write!(f, "Primary key violation in table '{}': duplicate value '{}' for key '{}'", table, value, key)
            }
            StorageError::MissingPrimaryKey { table, key } => {
                write!(f, "Missing primary key '{}' in table '{}'", key, table)
            }
            StorageError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        StorageError::IoError(error)
    }
}

// Statistics structures
#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: usize,
    pub column_stats: HashMap<String, ColumnStatistics>,
    pub last_updated: u64,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub unique_values: usize,
    pub total_values: usize,
    pub selectivity: f64,
}
