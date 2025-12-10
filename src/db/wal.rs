use super::schema::Row;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntryType {
    Insert {
        table: String,
        row_id: usize,
        data: Row,
    },
    Update {
        table: String,
        row_id: usize,
        old_data: Row,
        new_data: Row,
    },
    Delete {
        table: String,
        row_id: usize,
        data: Row,
    },
    Commit {
        tx_id: u64,
    },
    Abort {
        tx_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub tx_id: u64,
    pub sequence: u64,
    pub entry_type: WalEntryType,
    pub timestamp: u64,
}

#[derive(Debug)]
pub struct WalManager {
    wal_file_path: String,
    sequence: u64,
    checkpoint_interval: u64,
    operation_count: u64,
}

impl WalManager {
    pub fn new(wal_file_path: String) -> Self {
        let sequence = Self::get_last_sequence(&wal_file_path).unwrap_or(0);
        WalManager {
            wal_file_path,
            sequence,
            checkpoint_interval: 100,
            operation_count: 0,
        }
    }

    fn get_last_sequence(wal_file_path: &str) -> Result<u64, std::io::Error> {
        if !Path::new(wal_file_path).exists() {
            return Ok(0);
        }

        let mut file = File::open(wal_file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok(0);
        }

        let mut max_sequence = 0;
        let mut offset = 0;

        while offset < buffer.len() {
            match Self::read_entry_at_offset(&buffer, offset) {
                Ok((entry, entry_size)) => {
                    max_sequence = max_sequence.max(entry.sequence);
                    offset += entry_size;
                }
                Err(_) => {
                    break;
                }
            }
        }

        Ok(max_sequence)
    }

    fn read_entry_at_offset(buffer: &[u8], offset: usize) -> Result<(WalEntry, usize), std::io::Error> {
        if offset + 8 > buffer.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "End of WAL file"));
        }

        let size_bytes: [u8; 8] = buffer[offset..offset + 8]
            .try_into()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid size bytes"))?;
        let size = u64::from_le_bytes(size_bytes) as usize;

        if offset + 8 + size > buffer.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Incomplete entry"));
        }

        let entry_data = &buffer[offset + 8..offset + 8 + size];
        let entry = bincode::deserialize::<WalEntry>(entry_data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Deserialization error: {}", e)))?;

        Ok((entry, 8 + size))
    }

    pub fn append_entry(&mut self, entry: WalEntry) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_file_path)?;

        let serialized = bincode::serialize(&entry)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Serialization error: {}", e)))?;

        let size = serialized.len() as u64;
        file.write_all(&size.to_le_bytes())?;
        file.write_all(&serialized)?;
        file.sync_all()?;

        self.sequence = entry.sequence;
        self.operation_count += 1;

        Ok(())
    }

    pub fn create_entry(
        &mut self,
        tx_id: u64,
        entry_type: WalEntryType,
    ) -> Result<WalEntry, std::io::Error> {
        self.sequence += 1;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entry = WalEntry {
            tx_id,
            sequence: self.sequence,
            entry_type,
            timestamp,
        };

        Ok(entry)
    }

    pub fn replay_wal(&self) -> Result<Vec<WalEntry>, std::io::Error> {
        if !Path::new(&self.wal_file_path).exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.wal_file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            match Self::read_entry_at_offset(&buffer, offset) {
                Ok((entry, entry_size)) => {
                    entries.push(entry);
                    offset += entry_size.max(1);
                }
                Err(_) => {
                    break;
                }
            }
        }

        Ok(entries)
    }

    pub fn checkpoint(&mut self) -> Result<(), std::io::Error> {
        if !Path::new(&self.wal_file_path).exists() {
            return Ok(());
        }

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.wal_file_path)?;
        file.sync_all()?;

        self.operation_count = 0;
        Ok(())
    }

    pub fn should_checkpoint(&self) -> bool {
        self.operation_count >= self.checkpoint_interval
    }

    pub fn recover(&self) -> Result<Vec<WalEntry>, std::io::Error> {
        self.replay_wal()
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) -> Result<(), std::io::Error> {
        if Path::new(&self.wal_file_path).exists() {
            std::fs::remove_file(&self.wal_file_path)?;
        }
        self.sequence = 0;
        self.operation_count = 0;
        Ok(())
    }
}

