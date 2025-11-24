use serde::{Deserialize, Serialize};

const B_TREE_ORDER: usize = 4; // Minimum degree (t). Each node has at most 2t-1 keys

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BPlusTree {
    root: Option<Box<BPlusNode>>,
    order: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BPlusNode {
    Internal {
        keys: Vec<String>,
        children: Vec<Box<BPlusNode>>,
    },
    Leaf {
        keys: Vec<String>,
        values: Vec<usize>, // Row IDs
        next: Option<usize>, // For range scans (not used in current implementation)
    },
}

impl BPlusTree {
    pub fn new() -> Self {
        BPlusTree {
            root: None,
            order: B_TREE_ORDER,
        }
    }

    pub fn insert(&mut self, key: String, row_id: usize) {
        if self.root.is_none() {
            self.root = Some(Box::new(BPlusNode::Leaf {
                keys: vec![key],
                values: vec![row_id],
                next: None,
            }));
            return;
        }

        let root = self.root.take().unwrap();
        match self.insert_recursive(root, key, row_id) {
            InsertResult::NoSplit(node) => {
                self.root = Some(node);
            }
            InsertResult::Split(left, middle_key, right) => {
                self.root = Some(Box::new(BPlusNode::Internal {
                    keys: vec![middle_key],
                    children: vec![left, right],
                }));
            }
        }
    }

    pub fn search(&self, key: &str) -> Option<usize> {
        self.search_recursive(self.root.as_ref(), key)
    }

    pub fn delete(&mut self, key: &str) -> bool {
        if let Some(root) = self.root.take() {
            match self.delete_recursive(root, key) {
                DeleteResult::NoChange(node) => {
                    self.root = Some(node);
                    false
                }
                DeleteResult::Deleted(node) => {
                    self.root = Some(node);
                    true
                }
                DeleteResult::Underflow(node) => {
                    // Root underflow - check if we can merge
                    if let BPlusNode::Internal { keys, children } = *node {
                        if keys.is_empty() && children.len() == 1 {
                            self.root = Some(children.into_iter().next().unwrap());
                        } else {
                            self.root = Some(Box::new(BPlusNode::Internal { keys, children }));
                        }
                    } else {
                        self.root = Some(node);
                    }
                    true
                }
            }
        } else {
            false
        }
    }

    fn insert_recursive(&mut self, node: Box<BPlusNode>, key: String, row_id: usize) -> InsertResult {
        match *node {
            BPlusNode::Leaf { mut keys, mut values, next } => {
                let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
                
                if pos < keys.len() && keys[pos] == key {
                    // Key exists - update value
                    values[pos] = row_id;
                    InsertResult::NoSplit(Box::new(BPlusNode::Leaf { keys, values, next }))
                } else {
                    keys.insert(pos, key.clone());
                    values.insert(pos, row_id);
                    
                    if keys.len() > 2 * self.order - 1 {
                        // Split leaf
                        let mid = keys.len() / 2;
                        let right_keys = keys.split_off(mid);
                        let right_values = values.split_off(mid);
                        let middle_key = right_keys[0].clone();
                        
                        let left = Box::new(BPlusNode::Leaf {
                            keys,
                            values,
                            next: None,
                        });
                        let right = Box::new(BPlusNode::Leaf {
                            keys: right_keys,
                            values: right_values,
                            next,
                        });
                        
                        InsertResult::Split(left, middle_key, right)
                    } else {
                        InsertResult::NoSplit(Box::new(BPlusNode::Leaf { keys, values, next }))
                    }
                }
            }
            BPlusNode::Internal { mut keys, mut children } => {
                let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
                let child_idx = if pos < keys.len() && keys[pos] == key {
                    pos + 1
                } else {
                    pos
                };
                
                let child = children.remove(child_idx);
                match self.insert_recursive(child, key, row_id) {
                    InsertResult::NoSplit(new_child) => {
                        children.insert(child_idx, new_child);
                        InsertResult::NoSplit(Box::new(BPlusNode::Internal { keys, children }))
                    }
                    InsertResult::Split(left, middle_key, right) => {
                        children.insert(child_idx, left);
                        children.insert(child_idx + 1, right);
                        
                        let insert_pos = keys.binary_search(&middle_key).unwrap_or_else(|e| e);
                        keys.insert(insert_pos, middle_key);
                        
                        if keys.len() > 2 * self.order - 1 {
                            // Split internal node
                            let mid = keys.len() / 2;
                            let middle_key = keys.remove(mid);
                            let right_keys = keys.split_off(mid);
                            let right_children = children.split_off(mid + 1);
                            
                            let left = Box::new(BPlusNode::Internal { keys, children });
                            let right = Box::new(BPlusNode::Internal {
                                keys: right_keys,
                                children: right_children,
                            });
                            
                            InsertResult::Split(left, middle_key, right)
                        } else {
                            InsertResult::NoSplit(Box::new(BPlusNode::Internal { keys, children }))
                        }
                    }
                }
            }
        }
    }

    fn search_recursive(&self, node: Option<&Box<BPlusNode>>, key: &str) -> Option<usize> {
        match node {
            Some(n) => match n.as_ref() {
                BPlusNode::Leaf { keys, values, .. } => {
                    keys.binary_search(&key.to_string())
                        .ok()
                        .and_then(|idx| values.get(idx).copied())
                }
                BPlusNode::Internal { keys, children } => {
                    let pos = keys.binary_search(&key.to_string()).unwrap_or_else(|e| e);
                    let child_idx = if pos < keys.len() && keys[pos] == key {
                        pos + 1
                    } else {
                        pos
                    };
                    self.search_recursive(children.get(child_idx), key)
                }
            },
            None => None,
        }
    }

    fn delete_recursive(&mut self, node: Box<BPlusNode>, key: &str) -> DeleteResult {
        match *node {
            BPlusNode::Leaf { mut keys, mut values, next } => {
                if let Ok(pos) = keys.binary_search(&key.to_string()) {
                    keys.remove(pos);
                    values.remove(pos);
                    DeleteResult::Deleted(Box::new(BPlusNode::Leaf { keys, values, next }))
                } else {
                    DeleteResult::NoChange(Box::new(BPlusNode::Leaf { keys, values, next }))
                }
            }
            BPlusNode::Internal { keys, mut children } => {
                let pos = keys.binary_search(&key.to_string()).unwrap_or_else(|e| e);
                let child_idx = if pos < keys.len() && keys[pos] == key {
                    pos + 1
                } else {
                    pos
                };
                
                if child_idx < children.len() {
                    let child = children.remove(child_idx);
                    match self.delete_recursive(child, key) {
                        DeleteResult::NoChange(new_child) => {
                            children.insert(child_idx, new_child);
                            DeleteResult::NoChange(Box::new(BPlusNode::Internal { keys, children }))
                        }
                        DeleteResult::Deleted(new_child) => {
                            children.insert(child_idx, new_child);
                            DeleteResult::Deleted(Box::new(BPlusNode::Internal { keys, children }))
                        }
                        DeleteResult::Underflow(new_child) => {
                            // Handle underflow - simplified for now
                            children.insert(child_idx, new_child);
                            DeleteResult::Deleted(Box::new(BPlusNode::Internal { keys, children }))
                        }
                    }
                } else {
                    DeleteResult::NoChange(Box::new(BPlusNode::Internal { keys, children }))
                }
            }
        }
    }
}

enum InsertResult {
    NoSplit(Box<BPlusNode>),
    Split(Box<BPlusNode>, String, Box<BPlusNode>),
}

enum DeleteResult {
    NoChange(Box<BPlusNode>),
    Deleted(Box<BPlusNode>),
    #[allow(dead_code)]
    Underflow(Box<BPlusNode>),
}

