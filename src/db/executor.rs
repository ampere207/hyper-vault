use crate::db::{query::QueryPlan, storage_engine::{self, StorageEngine}};

pub struct ExecutionEngine{
    storage_engine: StorageEngine
}

impl ExecutionEngine{
    pub fn new(storage_engine: StorageEngine) -> Self{
        ExecutionEngine { storage_engine }
    }


    pub fn execute(&self, query_plan: QueryPlan){
        
    }
}