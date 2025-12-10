use db::{
    encryption::EncryptionKey,
    executor::{QueryExecutor, ExecutionError}, 
    parser::Parser, 
    query::{QueryPlanner, QueryComplexity, analyze_query_complexity},
    schema::Row,
    storage_engine::FileSystem,
    wal::WalManager,
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Instant;

mod db;

fn main() {
    println!("🚀 Welcome to HyperVault Database!");
    println!("=====================================");
    println!("Version 1.2.0 - Production-Ready SQL Database");
    println!("Features: ACID Transactions | Multi-threaded Concurrency | WAL Crash Recovery | Advanced SQL");
    println!("Type 'help' for available commands or 'exit' to quit.");
    println!();

    // Initialize the database with password prompting and recovery
    let mut filesystem = initialize_database_with_encryption("database.db");
    
    // Recover from WAL if crash detected
    recover_from_wal(&mut filesystem);
    
    let mut query_planner = QueryPlanner::new();
    
    // Create sample data if it doesn't exist
    initialize_sample_data(&mut filesystem);

    // Display startup information
    display_startup_info(&filesystem);

    // Start the CLI loop
    run_cli(&mut filesystem, &mut query_planner);
}

fn recover_from_wal(filesystem: &mut FileSystem) {
    use db::wal::WalEntryType;
    use std::path::Path;
    
    let wal_path = format!("{}.wal", filesystem.file_path);
    
    if Path::new(&wal_path).exists() {
        println!("⚠️  Crash detected. Recovering from WAL...");
        
        match filesystem.wal_manager.recover() {
            Ok(entries) => {
                if !entries.is_empty() {
                    println!("📋 Found {} WAL entries to replay", entries.len());
                    
                    let mut committed_txs = std::collections::HashSet::new();
                    let mut aborted_txs = std::collections::HashSet::new();
                    
                    for entry in &entries {
                        match &entry.entry_type {
                            WalEntryType::Commit { tx_id } => {
                                committed_txs.insert(*tx_id);
                            }
                            WalEntryType::Abort { tx_id } => {
                                aborted_txs.insert(*tx_id);
                            }
                            _ => {}
                        }
                    }
                    
                    for entry in entries {
                        if aborted_txs.contains(&entry.tx_id) {
                            continue;
                        }
                        
                        if committed_txs.contains(&entry.tx_id) || entry.tx_id == 0 {
                            match &entry.entry_type {
                                WalEntryType::Insert { table, row_id: _, data } => {
                                    let _ = filesystem.storage_engine.insert_row(table, data.clone());
                                }
                                WalEntryType::Update { table, row_id, new_data, .. } => {
                                    if let Some(table_data) = filesystem.storage_engine.tables.get_mut(table) {
                                        if let Some(row) = table_data.rows.get_mut(row_id) {
                                            *row = new_data.clone();
                                        }
                                    }
                                }
                                WalEntryType::Delete { table, row_id, .. } => {
                                    if let Some(table_data) = filesystem.storage_engine.tables.get_mut(table) {
                                        table_data.rows.remove(row_id);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    
                    println!("✅ Recovery complete!");
                }
                
                filesystem.wal_manager.checkpoint().unwrap_or_default();
                filesystem.save_to_file().unwrap_or_default();
            }
            Err(e) => {
                eprintln!("❌ Recovery failed: {}", e);
            }
        }
    }
}

fn initialize_database_with_encryption(file_path: &str) -> FileSystem {
    let db_exists = std::path::Path::new(file_path).exists();
    let is_encrypted = if db_exists {
        FileSystem::is_encrypted(file_path)
    } else {
        false
    };

    if db_exists && is_encrypted {
        // Database exists and is encrypted - prompt for password
        loop {
            println!("🔐 Encrypted database detected.");
            println!("Enter password to unlock, or type 'reset' to reset the password.");
            let password = prompt_password("Password (or 'reset'): ");
            
            // Check if user wants to reset password
            if password.to_lowercase() == "reset" {
                return reset_password_on_startup(file_path);
            }
            
            match FileSystem::try_load_with_password(file_path, &password) {
                Ok((storage_engine, cached_key)) => {
                    println!("✅ Database unlocked successfully!");
                    println!();
                    let wal_path = format!("{}.wal", file_path);
                    let wal_manager = WalManager::new(wal_path);
                    return FileSystem {
                        storage_engine,
                        file_path: file_path.to_string(),
                        encryption_password: Some(password),
                        cached_encryption_key: Some(cached_key),
                        wal_manager,
                    };
                }
                Err(e) => {
                    eprintln!("❌ Incorrect password or decryption failed: {}", e);
                    println!("Please try again or type 'reset' to reset the password.");
                }
            }
        }
    } else if !db_exists {
        // New database - prompt to set password
        println!("🔐 New database detected.");
        println!("Would you like to enable encryption? (y/n, default: y)");
        print!("> ");
        io::stdout().flush().unwrap();
        
        let mut response = String::new();
        io::stdin().read_line(&mut response).unwrap();
        let response = response.trim().to_lowercase();
        
        if response.is_empty() || response == "y" || response == "yes" {
            let password = prompt_password("Enter encryption password: ");
            let confirm = prompt_password("Confirm password: ");
            
            if password != confirm {
                eprintln!("❌ Passwords do not match. Starting without encryption.");
                println!();
                return FileSystem::new(file_path);
            }
            
            println!("✅ Encryption enabled!");
            println!();
            
            let mut fs = FileSystem::new(file_path);
            fs.set_password(&password);
            // Pre-derive and cache the key for first save (performance optimization)
            if let Ok(key) = EncryptionKey::from_password_cached(&password) {
                fs.cached_encryption_key = Some(key);
            }
            return fs;
        } else {
            println!("ℹ️  Starting database without encryption.");
            println!();
            return FileSystem::new(file_path);
        }
    } else {
        // Database exists but is unencrypted
        return FileSystem::new(file_path);
    }
}

fn reset_password_on_startup(file_path: &str) -> FileSystem {
    println!();
    println!("🔐 Reset Database Password");
    println!("==========================");
    
    // First, we need to unlock the database with the old password
    loop {
        let old_password = prompt_password("Enter current password: ");
        match FileSystem::try_load_with_password(file_path, &old_password) {
            Ok((storage_engine, _)) => {
                // Successfully unlocked - now we can reset
                println!("✅ Password verified. Now set a new password.");
                println!();
                
                let new_password = prompt_password("Enter new password: ");
                let confirm_password = prompt_password("Confirm new password: ");
                
                if new_password != confirm_password {
                    eprintln!("❌ Passwords do not match. Exiting.");
                    std::process::exit(1);
                }
                
                if new_password.is_empty() {
                    eprintln!("❌ Password cannot be empty. Exiting.");
                    std::process::exit(1);
                }
                
                // Create FileSystem with old password, then reset it
                let wal_path = format!("{}.wal", file_path);
                let wal_manager = WalManager::new(wal_path);
                let mut fs = FileSystem {
                    storage_engine,
                    file_path: file_path.to_string(),
                    encryption_password: Some(old_password.clone()),
                    cached_encryption_key: None,
                    wal_manager,
                };
                
                // Reset to new password
                if let Err(e) = fs.reset_password(&old_password, &new_password) {
                    eprintln!("❌ Failed to reset password: {}", e);
                    std::process::exit(1);
                }
                
                println!("✅ Password reset successfully! Database has been re-encrypted.");
                println!();
                return fs;
            }
            Err(e) => {
                eprintln!("❌ Incorrect password: {}", e);
                println!("Please try again.");
            }
        }
    }
}

fn reset_password(filesystem: &mut FileSystem) {
    if filesystem.encryption_password.is_none() {
        println!("❌ No password is currently set. Use 'set password <password>' to set one.");
        return;
    }
    
    println!("🔐 Reset Encryption Password");
    println!("============================");
    
    let old_password = prompt_password("Enter current password: ");
    
    // Verify old password
    if let Some(ref current_password) = filesystem.encryption_password {
        if current_password != &old_password {
            println!("❌ Incorrect current password.");
            return;
        }
    }
    
    let new_password = prompt_password("Enter new password: ");
    let confirm_password = prompt_password("Confirm new password: ");
    
    if new_password != confirm_password {
        println!("❌ Passwords do not match. Password reset cancelled.");
        return;
    }
    
    if new_password.is_empty() {
        println!("❌ Password cannot be empty. Password reset cancelled.");
        return;
    }
    
    match filesystem.reset_password(&old_password, &new_password) {
        Ok(_) => {
            println!("✅ Password reset successfully! Database has been re-encrypted with the new password.");
        }
        Err(e) => {
            eprintln!("❌ Failed to reset password: {}", e);
        }
    }
}

fn prompt_password(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().unwrap();
    
    // On Windows, we can't easily hide input, so we'll just read it normally
    // On Unix systems, we could use termion or similar, but for simplicity
    // we'll just read the password (user should be aware it's visible)
    let mut password = String::new();
    io::stdin().read_line(&mut password).unwrap();
    password.trim().to_string()
}

fn initialize_sample_data(filesystem: &mut FileSystem) {
    // Check if users table already exists, if not create it
    if !filesystem.storage_engine.tables.contains_key("users") {
        println!("📦 Initializing sample 'users' table...");
        filesystem.create_table(
            "users",
            vec![
                "id".to_string(),
                "name".to_string(),
                "email".to_string(),
                "age".to_string(),
            ],
            Some("id"),
        );

        // Insert sample data
        let sample_users = vec![
            ("1", "Anthony Etienne", "anthony.etienne@gmail.com", "25"),
            ("2", "Jane Doe", "jane.doe@example.com", "30"),
            ("3", "Bob Smith", "bob.smith@example.com", "28"),
            ("4", "Alice Johnson", "alice.johnson@example.com", "35"),
        ];

        for (id, name, email, age) in sample_users {
            let _ = filesystem.insert_row(
                "users",
                Row {
                    data: HashMap::from([
                        ("id".to_string(), id.to_string()),
                        ("name".to_string(), name.to_string()),
                        ("email".to_string(), email.to_string()),
                        ("age".to_string(), age.to_string()),
                    ]),
                },
            );
        }

        println!("✅ Sample data initialized successfully!");
        println!();
    }
}

fn display_startup_info(filesystem: &FileSystem) {
    let stats = filesystem.get_statistics();
    println!("📊 Database Statistics:");
    println!("   Version: {}", stats.version);
    println!("   Tables: {}", filesystem.storage_engine.tables.len());
    println!("   Total Operations: {}", stats.total_operations);
    if stats.total_operations > 0 {
        println!("   Last Modified: {}", format_timestamp(stats.last_modified));
    }
    println!();
}

fn format_timestamp(timestamp: u64) -> String {
    // Simple timestamp formatting - in a real application you'd use a proper date library
    format!("Unix timestamp: {}", timestamp)
}

fn format_duration(duration: std::time::Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos < 1_000 {
        format!("{} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.2} μs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.3} s", nanos as f64 / 1_000_000_000.0)
    }
}

fn run_cli(filesystem: &mut FileSystem, query_planner: &mut QueryPlanner) {
    let mut current_tx_id: Option<u64> = None;
    
    loop {
        // Display prompt
        print!("hypervault> ");
        io::stdout().flush().unwrap();

        // Read user input
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let input = input.trim();
                
                // Handle special commands
                match input.to_lowercase().as_str() {
                    "" => continue, // Skip empty input
                    "exit" | "quit" | "q" => {
                        println!("👋 Goodbye! Thanks for using HyperVault Database!");
                        display_session_summary(query_planner);
                        break;
                    }
                    "help" | "h" => {
                        display_help();
                        continue;
                    }
                    "show tables" => {
                        show_tables(filesystem);
                        continue;
                    }
                    "show all" | "show data" => {
                        show_all_data(filesystem);
                        continue;
                    }
                    "show stats" | "stats" => {
                        show_database_statistics(filesystem, query_planner);
                        continue;
                    }
                    "clear" | "cls" => {
                        // Clear screen (works on most terminals)
                        print!("\x1B[2J\x1B[1;1H");
                        io::stdout().flush().unwrap();
                        continue;
                    }
                    _ => {
                        // Check for set password command
                        if input.to_lowercase().starts_with("set password ") {
                            let password = input[13..].trim();
                            if !password.is_empty() {
                                filesystem.set_password(password);
                                println!("✅ Encryption password set. Database will be encrypted on next save.");
                                continue;
                            } else {
                                println!("❌ Password cannot be empty.");
                                continue;
                            }
                        }
                        if input.to_lowercase() == "clear password" {
                            filesystem.clear_password();
                            println!("✅ Encryption password cleared. Database will be saved unencrypted.");
                            continue;
                        }
                        if input.to_lowercase() == "reset password" || input.to_lowercase() == "change password" {
                            reset_password(filesystem);
                            continue;
                        }
                    }
                }

                // Process SQL command
                current_tx_id = execute_sql_command(filesystem, query_planner, input, current_tx_id);
            }
            Err(error) => {
                eprintln!("❌ Error reading input: {}", error);
            }
        }
        
        println!(); // Add spacing between commands
    }
}

fn execute_sql_command(filesystem: &mut FileSystem, query_planner: &mut QueryPlanner, input: &str, current_tx_id: Option<u64>) -> Option<u64> {
    println!("🔍 Executing: {}", input);
    
    let total_start = Instant::now();
    
    // Parse the SQL command
    let parse_start = Instant::now();
    match Parser::parse(input) {
        Ok(ast) => {
            let parse_time = parse_start.elapsed();
            
            // Handle transaction commands directly
            match &ast {
                db::parser::ASTNode::BeginTransaction => {
                    let mut executor = QueryExecutor::new(filesystem);
                    match executor.execute(ast) {
                        Ok(_) => {
                            println!("✅ Transaction started");
                            return executor.tx_id;
                        }
                        Err(e) => {
                            eprintln!("❌ Error: {}", format_execution_error(&e));
                            return current_tx_id;
                        }
                    }
                }
                db::parser::ASTNode::CommitTransaction => {
                    if current_tx_id.is_none() {
                        eprintln!("❌ No active transaction to commit");
                        return None;
                    }
                    let mut executor = QueryExecutor::with_transaction(filesystem, current_tx_id.unwrap());
                    match executor.execute(ast) {
                        Ok(_) => {
                            println!("✅ Transaction committed");
                            return None;
                        }
                        Err(e) => {
                            eprintln!("❌ Commit failed: {}", format_execution_error(&e));
                            return current_tx_id;
                        }
                    }
                }
                db::parser::ASTNode::RollbackTransaction => {
                    if current_tx_id.is_none() {
                        eprintln!("❌ No active transaction to rollback");
                        return None;
                    }
                    let mut executor = QueryExecutor::with_transaction(filesystem, current_tx_id.unwrap());
                    match executor.execute(ast) {
                        Ok(_) => {
                            println!("✅ Transaction rolled back");
                            return None;
                        }
                        Err(e) => {
                            eprintln!("❌ Rollback failed: {}", format_execution_error(&e));
                            return current_tx_id;
                        }
                    }
                }
                _ => {}
            }
            
            // Get table row count for better cost estimation
            let table_name = match &ast {
                db::parser::ASTNode::SelectStatement { table, .. } |
                db::parser::ASTNode::InsertStatement { table, .. } |
                db::parser::ASTNode::UpdateStatement { table, .. } |
                db::parser::ASTNode::DeleteStatement { table, .. } => Some(table.0.clone()),
                _ => None,
            };
            let table_row_count = table_name.as_ref()
                .and_then(|name| filesystem.storage_engine.tables.get(name))
                .map(|table| table.rows.len());
            
            // Create and validate query plan (skip for transaction commands)
            let plan_start = Instant::now();
            match query_planner.plan(&ast, table_row_count) {
                Ok(plan) => {
                    let plan_time = plan_start.elapsed();
                    
                    // Analyze query complexity
                    let complexity = analyze_query_complexity(&plan);
                    println!("📈 Query complexity: {:?}", complexity);
                    
                    // Display query plan for complex queries
                    if matches!(complexity, QueryComplexity::Complex) {
                        println!("📋 Query plan:");
                        display_query_plan(&plan);
                    }
                    
                    // Validate plan if table exists
                    if let Some(table) = filesystem.storage_engine.tables.get(&plan.table.0) {
                        if let Err(e) = query_planner.validate_plan(&plan, true, &table.columns) {
                            eprintln!("❌ Query validation failed: {}", e);
                            return current_tx_id;
                        }
                    }
                    
                    // Execute the query
                    let exec_start = Instant::now();
                    let mut execution_engine = if let Some(tx_id) = current_tx_id {
                        QueryExecutor::with_transaction(filesystem, tx_id)
                    } else {
                        QueryExecutor::new(filesystem)
                    };
                    
                    match execution_engine.execute(ast) {
                        Ok(result) => {
                            let exec_time = exec_start.elapsed();
                            let total_time = total_start.elapsed();
                            
                            if !result.is_empty() {
                                println!("📊 Query Results:");
                                display_results(&result);
                            }
                            
                            // Display timing information
                            println!("⏱️  Timing:");
                            println!("   Parse:   {}", format_duration(parse_time));
                            println!("   Plan:    {}", format_duration(plan_time));
                            println!("   Execute: {}", format_duration(exec_time));
                            println!("   Total:   {}", format_duration(total_time));
                            
                            // Update statistics
                            query_planner.optimizer.update_statistics(&plan.query_type, total_time.as_secs_f64(), true);
                            
                            // Auto-commit if not in transaction
                            if current_tx_id.is_none() && execution_engine.tx_id.is_some() {
                                let tx_id = execution_engine.tx_id.unwrap();
                                let mut auto_executor = QueryExecutor::with_transaction(filesystem, tx_id);
                                let _ = auto_executor.execute(db::parser::ASTNode::CommitTransaction);
                                return None;
                            }
                            
                            return execution_engine.tx_id;
                        }
                        Err(err) => {
                            eprintln!("❌ Execution Error: {}", format_execution_error(&err));
                            return current_tx_id;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Query Planning Error: {}", e);
                    return current_tx_id;
                }
            }
        }
        Err(err) => {
            eprintln!("❌ Parse Error: {}", err);
            println!("💡 Tip: Check your SQL syntax. Type 'help' for examples.");
            return current_tx_id;
        }
    }
}

fn format_execution_error(error: &ExecutionError) -> String {
    match error {
        ExecutionError::TableNotFound => "Table not found".to_string(),
        ExecutionError::InsertFailed => "Insert operation failed".to_string(),
        ExecutionError::UpdateFailed => "Update operation failed".to_string(),
        ExecutionError::InvalidQuery => "Invalid query structure".to_string(),
        ExecutionError::NoActiveTransaction => "No active transaction".to_string(),
        ExecutionError::TransactionError(msg) => format!("Transaction error: {}", msg),
        ExecutionError::LockError(msg) => format!("Lock error: {}", msg),
        ExecutionError::WalError(msg) => format!("WAL error: {}", msg),
        ExecutionError::SaveFailed => "Failed to save database".to_string(),
    }
}

fn display_query_plan(plan: &db::query::QueryPlan) {
    println!("   Table: {}", plan.table.0);
    println!("   Estimated Cost: {:.2}", plan.estimated_cost);
    println!("   Execution Steps:");
    for (i, step) in plan.execution_steps.iter().enumerate() {
        println!("     {}. {:?}", i + 1, step);
    }
}

fn display_results(results: &[Row]) {
    if results.is_empty() {
        println!("   No rows returned.");
        return;
    }

    // Get all unique column names from the results
    let mut columns: Vec<String> = Vec::new();
    for row in results {
        for key in row.data.keys() {
            if !columns.contains(key) {
                columns.push(key.clone());
            }
        }
    }
    columns.sort();

    if columns.is_empty() {
        println!("   No data to display.");
        return;
    }

    // Calculate column widths for formatting
    let mut col_widths: HashMap<String, usize> = HashMap::new();
    for col in &columns {
        col_widths.insert(col.clone(), col.len().max(12)); // Minimum width of 12
    }

    for row in results {
        for col in &columns {
            if let Some(value) = row.data.get(col) {
                let current_width = col_widths.get(col).unwrap_or(&12);
                col_widths.insert(col.clone(), (*current_width).max(value.len()));
            }
        }
    }

    // Print header
    print!("   ");
    for col in &columns {
        let width = col_widths.get(col).unwrap_or(&12);
        print!("| {:width$} ", col, width = width);
    }
    println!("|");

    // Print separator
    print!("   ");
    for col in &columns {
        let width = col_widths.get(col).unwrap_or(&12);
        print!("|{}", "-".repeat(width + 2));
    }
    println!("|");

    // Print rows
    for row in results {
        print!("   ");
        for col in &columns {
            let width = col_widths.get(col).unwrap_or(&12);
            let null_str = "NULL".to_string();
            let value = row.data.get(col).unwrap_or(&null_str);
            print!("| {:width$} ", value, width = width);
        }
        println!("|");
    }

    println!("   ({} rows)", results.len());
}

fn display_help() {
    println!("📚 HyperVault Database - Enhanced Help");
    println!("======================================");
    println!();
    println!("🔧 Special Commands:");
    println!("   help, h              - Show this help message");
    println!("   show tables          - List all tables in the database");
    println!("   show all, show data  - Display all data from all tables");
    println!("   show stats, stats    - Show database and query statistics");
    println!("   set password <pwd>   - Set encryption password for database");
    println!("   reset password       - Reset/change encryption password");
    println!("   clear password       - Remove encryption password");
    println!("   clear, cls           - Clear the screen");
    println!("   exit, quit, q        - Exit the database");
    println!();
    println!("📝 SQL Commands:");
    println!("   SELECT * FROM users");
    println!("   SELECT id, name FROM users WHERE age > '25'");
    println!("   SELECT COUNT(*) FROM users");
    println!("   SELECT * FROM users ORDER BY age DESC LIMIT 10");
    println!("   SELECT * FROM table1 JOIN table2 ON table1.id = table2.id");
    println!("   INSERT INTO users (id, name, email, age) VALUES ('5', 'John Doe', 'john@example.com', '32')");
    println!("   UPDATE users SET age = '26' WHERE id = '1'");
    println!("   DELETE FROM users WHERE age > '35'");
    println!();
    println!("🔄 Transaction Commands:");
    println!("   BEGIN TRANSACTION");
    println!("   COMMIT TRANSACTION");
    println!("   ROLLBACK TRANSACTION");
    println!();
    println!("🎯 Advanced Features:");
    println!("   - Query optimization and planning");
    println!("   - Query complexity analysis");
    println!("   - Performance statistics tracking");
    println!("   - Enhanced error messages");
    println!();
    println!("💡 Tips:");
    println!("   - Use single quotes for string values: 'value'");
    println!("   - Supported operators: =, >, <, >=, <=, !=, <>");
    println!("   - Use * to select all columns: SELECT * FROM table");
    println!("   - Commands are case-insensitive");
    println!("   - Complex queries show execution plans");
    println!();
    println!("🎯 Quick Examples:");
    println!("   hypervault> SELECT * FROM users WHERE age >= '30'");
    println!("   hypervault> show stats");
    println!("   hypervault> show all");
    println!();
}

fn show_tables(filesystem: &mut FileSystem) {
    println!("📋 Available Tables:");
    println!("===================");
    
    if filesystem.storage_engine.tables.is_empty() {
        println!("   No tables found in the database.");
        return;
    }

    // Collect table names first to avoid borrow conflicts
    let table_names: Vec<String> = filesystem.storage_engine.tables.keys().cloned().collect();
    
    for table_name in &table_names {
        let table = filesystem.storage_engine.tables.get(table_name).unwrap();
        println!("   🗂️  Table: {}", table_name);
        println!("      Columns: {}", table.columns.join(", "));
        if let Some(pk) = &table.primary_key {
            println!("      Primary Key: {}", pk);
        }
        println!("      Rows: {}", table.rows.len());
        
        // Show table statistics if available
        if let Some(stats) = filesystem.get_table_stats(table_name) {
            println!("      Statistics:");
            println!("        Row Count: {}", stats.row_count);
            for (column, col_stats) in &stats.column_stats {
                println!("        {}: {} unique values (selectivity: {:.2})", 
                    column, col_stats.unique_values, col_stats.selectivity);
            }
        }
        println!();
    }
}

fn show_all_data(filesystem: &FileSystem) {
    println!("🗄️  All Database Content:");
    println!("=========================");
    
    if filesystem.storage_engine.tables.is_empty() {
        println!("   No tables found in the database.");
        return;
    }

    let mut total_rows = 0;
    
    for (table_name, table) in &filesystem.storage_engine.tables {
        println!("📋 Table: {}", table_name);
        println!("   Columns: {}", table.columns.join(", "));
        if let Some(pk) = &table.primary_key {
            println!("   Primary Key: {}", pk);
        }
        println!();

        if table.rows.is_empty() {
            println!("   No data in this table.");
            println!();
            continue;
        }

        // Convert table rows to Vec<Row> for display_results function
        let rows: Vec<Row> = table.rows.values().cloned().collect();
        display_results(&rows);
        total_rows += rows.len();
        
        println!();
        println!("   {}", "─".repeat(60));
        println!();
    }

    println!("📊 Database Summary:");
    println!("   Total Tables: {}", filesystem.storage_engine.tables.len());
    println!("   Total Rows: {}", total_rows);
}

fn show_database_statistics(filesystem: &mut FileSystem, query_planner: &QueryPlanner) {
    println!("📊 Database Statistics:");
    println!("======================");
    
    let storage_stats = filesystem.get_statistics();
    println!("🗄️  Storage Statistics:");
    println!("   Version: {}", storage_stats.version);
    println!("   Total Operations: {}", storage_stats.total_operations);
    println!("   Tables Created: {}", storage_stats.total_tables_created);
    println!("   Rows Inserted: {}", storage_stats.total_rows_inserted);
    println!("   Rows Updated: {}", storage_stats.total_rows_updated);
    println!("   Rows Deleted: {}", storage_stats.total_rows_deleted);
    println!("   Last Modified: {}", format_timestamp(storage_stats.last_modified));
    println!();
    
    let query_stats = query_planner.get_statistics();
    println!("🔍 Query Statistics:");
    println!("   Total Queries: {}", query_stats.total_queries);
    println!("   SELECT Queries: {}", query_stats.select_queries);
    println!("   INSERT Queries: {}", query_stats.insert_queries);
    println!("   UPDATE Queries: {}", query_stats.update_queries);
    println!("   DELETE Queries: {}", query_stats.delete_queries);
    println!("   Failed Queries: {}", query_stats.failed_queries);
    if query_stats.total_queries > 0 {
        println!("   Success Rate: {:.1}%", 
            ((query_stats.total_queries - query_stats.failed_queries) as f64 / query_stats.total_queries as f64) * 100.0);
        println!("   Average Execution Time: {:.3}s", query_stats.average_execution_time);
    }
    println!();
    
    println!("📋 Table Details:");
    // Collect table names first to avoid borrow conflicts
    let table_names: Vec<String> = filesystem.storage_engine.tables.keys().cloned().collect();
    
    for table_name in &table_names {
        if let Some(stats) = filesystem.get_table_stats(table_name) {
            println!("   {} ({} rows):", table_name, stats.row_count);
            for (column, col_stats) in &stats.column_stats {
                println!("     {}: {} unique/{} total (selectivity: {:.3})", 
                    column, col_stats.unique_values, col_stats.total_values, col_stats.selectivity);
            }
        }
    }
}

fn display_session_summary(query_planner: &QueryPlanner) {
    let stats = query_planner.get_statistics();
    if stats.total_queries > 0 {
        println!();
        println!("📈 Session Summary:");
        println!("   Queries Executed: {}", stats.total_queries);
        println!("   Success Rate: {:.1}%", 
            ((stats.total_queries - stats.failed_queries) as f64 / stats.total_queries as f64) * 100.0);
        if stats.average_execution_time > 0.0 {
            println!("   Average Query Time: {:.3}s", stats.average_execution_time);
        }
    }
}
