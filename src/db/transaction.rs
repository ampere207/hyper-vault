use super::wal::WalEntry;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub struct Transaction {
    #[allow(dead_code)]
    pub id: u64,
    pub state: TransactionState,
    #[allow(dead_code)]
    pub read_set: HashSet<String>,
    pub write_set: HashSet<String>,
    pub wal_entries: Vec<WalEntry>,
}

impl Transaction {
    pub fn new(id: u64) -> Self {
        Transaction {
            id,
            state: TransactionState::Active,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            wal_entries: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn add_read(&mut self, table: String) {
        self.read_set.insert(table);
    }

    pub fn add_write(&mut self, table: String) {
        self.write_set.insert(table);
    }

    pub fn add_wal_entry(&mut self, entry: WalEntry) {
        self.wal_entries.push(entry);
    }

    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }

    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }

    #[allow(dead_code)]
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }
}

#[derive(Debug)]
pub struct TransactionManager {
    active_transactions: HashMap<u64, Transaction>,
    next_transaction_id: u64,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            next_transaction_id: 1,
        }
    }

    pub fn begin_transaction(&mut self) -> u64 {
        let tx_id = self.next_transaction_id;
        self.next_transaction_id += 1;
        self.active_transactions.insert(tx_id, Transaction::new(tx_id));
        tx_id
    }

    pub fn commit_transaction(&mut self, tx_id: u64) -> Result<(), String> {
        if let Some(tx) = self.active_transactions.get_mut(&tx_id) {
            if tx.state != TransactionState::Active {
                return Err(format!("Transaction {} is not active", tx_id));
            }
            tx.commit();
            Ok(())
        } else {
            Err(format!("Transaction {} not found", tx_id))
        }
    }

    pub fn rollback_transaction(&mut self, tx_id: u64) -> Result<(), String> {
        if let Some(tx) = self.active_transactions.get_mut(&tx_id) {
            if tx.state != TransactionState::Active {
                return Err(format!("Transaction {} is not active", tx_id));
            }
            tx.abort();
            Ok(())
        } else {
            Err(format!("Transaction {} not found", tx_id))
        }
    }

    pub fn get_transaction(&self, tx_id: u64) -> Option<&Transaction> {
        self.active_transactions.get(&tx_id)
    }

    pub fn get_transaction_mut(&mut self, tx_id: u64) -> Option<&mut Transaction> {
        self.active_transactions.get_mut(&tx_id)
    }

    pub fn remove_transaction(&mut self, tx_id: u64) {
        self.active_transactions.remove(&tx_id);
    }

    #[allow(dead_code)]
    pub fn is_active(&self, tx_id: u64) -> bool {
        self.active_transactions
            .get(&tx_id)
            .map(|tx| tx.is_active())
            .unwrap_or(false)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

