use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockType {
    Shared,
    Exclusive,
}

#[derive(Debug)]
pub struct TableLock {
    #[allow(dead_code)]
    rwlock: Arc<RwLock<()>>,
    waiters: Arc<RwLock<VecDeque<(u64, LockType)>>>,
    holders: Arc<RwLock<HashMap<u64, LockType>>>,
}

impl TableLock {
    pub fn new() -> Self {
        TableLock {
            rwlock: Arc::new(RwLock::new(())),
            waiters: Arc::new(RwLock::new(VecDeque::new())),
            holders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn try_acquire_read(&self, tx_id: u64) -> bool {
        let mut holders = self.holders.write();
        
        if let Some(lock_type) = holders.get(&tx_id) {
            return *lock_type == LockType::Shared || *lock_type == LockType::Exclusive;
        }

        let has_exclusive = holders.values().any(|&lt| lt == LockType::Exclusive);
        if !has_exclusive {
            holders.insert(tx_id, LockType::Shared);
            return true;
        }
        false
    }

    pub fn try_acquire_write(&self, tx_id: u64) -> bool {
        let mut holders = self.holders.write();
        
        if let Some(lock_type) = holders.get(&tx_id) {
            if *lock_type == LockType::Exclusive {
                return true;
            }
        }

        if holders.is_empty() {
            holders.insert(tx_id, LockType::Exclusive);
            return true;
        }
        false
    }

    pub fn release(&self, tx_id: u64) {
        let mut holders = self.holders.write();
        holders.remove(&tx_id);
    }

    pub fn add_waiter(&self, tx_id: u64, lock_type: LockType) {
        let mut waiters = self.waiters.write();
        waiters.push_back((tx_id, lock_type));
    }

    #[allow(dead_code)]
    pub fn get_waiters(&self) -> Vec<(u64, LockType)> {
        self.waiters.read().iter().cloned().collect()
    }

    #[allow(dead_code)]
    pub fn get_holders(&self) -> Vec<u64> {
        self.holders.read().keys().cloned().collect()
    }
}

impl Default for TableLock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct LockManager {
    table_locks: Arc<RwLock<HashMap<String, Arc<TableLock>>>>,
    lock_graph: Arc<RwLock<HashMap<u64, HashSet<u64>>>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            table_locks: Arc::new(RwLock::new(HashMap::new())),
            lock_graph: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get_or_create_lock(&self, table: &str) -> Arc<TableLock> {
        let mut locks = self.table_locks.write();
        locks
            .entry(table.to_string())
            .or_insert_with(|| Arc::new(TableLock::new()))
            .clone()
    }

    pub fn acquire_lock(
        &self,
        table: &str,
        tx_id: u64,
        lock_type: LockType,
    ) -> Result<(), String> {
        let table_lock = self.get_or_create_lock(table);

        let acquired = match lock_type {
            LockType::Shared => table_lock.try_acquire_read(tx_id),
            LockType::Exclusive => table_lock.try_acquire_write(tx_id),
        };

        if acquired {
            return Ok(());
        }

        table_lock.add_waiter(tx_id, lock_type);
        
        if let Some(cycle) = self.detect_deadlock(tx_id) {
            let victim = self.resolve_deadlock(&cycle);
            if victim == tx_id {
                return Err(format!("Transaction {} aborted due to deadlock", tx_id));
            }
        }

        let start = Instant::now();
        let timeout = Duration::from_secs(5);

        loop {
            let acquired = match lock_type {
                LockType::Shared => table_lock.try_acquire_read(tx_id),
                LockType::Exclusive => table_lock.try_acquire_write(tx_id),
            };

            if acquired {
                let mut waiters = table_lock.waiters.write();
                waiters.retain(|&(id, _)| id != tx_id);
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(format!("Timeout acquiring lock for transaction {}", tx_id));
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn release_lock(&self, table: &str, tx_id: u64) -> Result<(), String> {
        let table_lock = self.get_or_create_lock(table);
        table_lock.release(tx_id);

        let mut graph = self.lock_graph.write();
        graph.remove(&tx_id);
        graph.values_mut().for_each(|waiters| {
            waiters.remove(&tx_id);
        });

        Ok(())
    }

    #[allow(dead_code)]
    pub fn release_all_locks(&self, tx_id: u64) {
        let locks = self.table_locks.read();
        for table_lock in locks.values() {
            table_lock.release(tx_id);
        }

        let mut graph = self.lock_graph.write();
        graph.remove(&tx_id);
        graph.values_mut().for_each(|waiters| {
            waiters.remove(&tx_id);
        });
    }

    fn detect_deadlock(&self, tx_id: u64) -> Option<Vec<u64>> {
        let graph = self.lock_graph.read();
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        if self.has_cycle_dfs(tx_id, &graph, &mut visited, &mut rec_stack, &mut path) {
            Some(path)
        } else {
            None
        }
    }

    fn has_cycle_dfs(
        &self,
        node: u64,
        graph: &HashMap<u64, HashSet<u64>>,
        visited: &mut HashSet<u64>,
        rec_stack: &mut HashSet<u64>,
        path: &mut Vec<u64>,
    ) -> bool {
        visited.insert(node);
        rec_stack.insert(node);
        path.push(node);

        if let Some(neighbors) = graph.get(&node) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if self.has_cycle_dfs(neighbor, graph, visited, rec_stack, path) {
                        return true;
                    }
                } else if rec_stack.contains(&neighbor) {
                    path.push(neighbor);
                    return true;
                }
            }
        }

        rec_stack.remove(&node);
        path.pop();
        false
    }

    fn resolve_deadlock(&self, cycle: &[u64]) -> u64 {
        *cycle.iter().max().unwrap()
    }

    #[allow(dead_code)]
    pub fn build_wait_for_graph(&self) {
        let mut graph = self.lock_graph.write();
        graph.clear();

        let locks = self.table_locks.read();
        for (_table_name, table_lock) in locks.iter() {
            let waiters = table_lock.get_waiters();
            let holders = table_lock.get_holders();

            for (waiter_id, _) in waiters {
                for holder_id in &holders {
                    graph.entry(waiter_id).or_insert_with(HashSet::new).insert(*holder_id);
                }
            }
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

