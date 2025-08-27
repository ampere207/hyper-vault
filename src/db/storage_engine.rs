use std::collections::HashMap;
use super::schema::{Table, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct StorageEngine{
    tables: HashMap<String, Table>
}

impl StorageEngine{
    pub fn new() -> Self{
        StorageEngine { tables: HashMap::new() }
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<String>){
        self.tables.insert(
            name.to_string(),
            Table { columns, rows: HashMap::new() }
        );
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row){
        if let Some(table) = self.tables.get_mut(table_name){
            let row_id = table.rows.len();
            table.rows.insert(row_id, row);
        }
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), std::io::Error>{
        buffer.clear();
        buffer.extend(bincode::serialize(self).unwrap());
        Ok(())
    }

    pub fn deserialize(&self, buffer: &[u8]) -> Result<(), std::io::Error>{
        Ok(bincode::deserialize(buffer).unwrap())
    }
}