use db::{
    executor::{QueryExecutor, ExecutionError}, 
    lexer::Tokenizer, 
    parser::Parser, 
    query::{QueryPlanner, QueryComplexity, analyze_query_complexity},
    schema::Row,
    storage_engine::{FileSystem, StorageError},
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Instant;

mod db;

fn main() {
    println!("🚀 Welcome to HyperVault Database!");
    println!("=====================================");
    println!("Enhanced SQL Database with Query Optimization");
    println!("Type 'help' for available commands or 'exit' to quit.");
    println!();

    // Initialize the database and query planner
    let mut filesystem = FileSystem::new("database.db");
    let mut query_planner = QueryPlanner::new();
    
    // Create sample data if it doesn't exist
    initialize_sample_data(&mut filesystem);

    // Display startup information
    display_startup_info(&filesystem);

    // Start the CLI loop
    run_cli(&mut filesystem, &mut query_planner);
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

fn run_cli(filesystem: &mut FileSystem, query_planner: &mut QueryPlanner) {
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
                    _ => {}
                }

                // Process SQL command
                execute_sql_command(filesystem, query_planner, input);
            }
            Err(error) => {
                eprintln!("❌ Error reading input: {}", error);
            }
        }
        
        println!(); // Add spacing between commands
    }
}

fn execute_sql_command(filesystem: &mut FileSystem, query_planner: &mut QueryPlanner, input: &str) {
    println!("🔍 Executing: {}", input);
    
    let start_time = Instant::now();
    let mut success = true;
    
    // Parse the SQL command
    match Parser::parse(input) {
        Ok(ast) => {
            println!("✅ Query parsed successfully");
            
            // Create and validate query plan
            match query_planner.plan(&ast) {
                Ok(mut plan) => {
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
                            success = false;
                            return;
                        }
                    }
                    
                    // Execute the query
                    let mut execution_engine = QueryExecutor::new(filesystem);
                    match execution_engine.execute(ast) {
                        Ok(result) => {
                            println!("📊 Query Results:");
                            display_results(&result);
                            
                            // Update statistics
                            let execution_time = start_time.elapsed().as_secs_f64();
                            query_planner.optimizer.update_statistics(&plan.query_type, execution_time, true);
                        }
                        Err(err) => {
                            eprintln!("❌ Execution Error: {}", format_execution_error(&err));
                            success = false;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Query Planning Error: {}", e);
                    success = false;
                }
            }
        }
        Err(err) => {
            eprintln!("❌ Parse Error: {}", err);
            println!("💡 Tip: Check your SQL syntax. Type 'help' for examples.");
            success = false;
        }
    }
    
    // Update statistics for failed queries
    if !success {
        let execution_time = start_time.elapsed().as_secs_f64();
        // We can't determine query type for failed parses, so we'll skip statistics update
    }
}

fn format_execution_error(error: &ExecutionError) -> String {
    match error {
        ExecutionError::TableNotFound => "Table not found".to_string(),
        ExecutionError::InsertFailed => "Insert operation failed".to_string(),
        ExecutionError::UpdateFailed => "Update operation failed".to_string(),
        ExecutionError::InvalidQuery => "Invalid query structure".to_string(),
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
    println!("   clear, cls           - Clear the screen");
    println!("   exit, quit, q        - Exit the database");
    println!();
    println!("📝 SQL Commands:");
    println!("   SELECT * FROM users");
    println!("   SELECT id, name FROM users WHERE age > '25'");
    println!("   SELECT * FROM users WHERE name = 'Anthony Etienne'");
    println!("   INSERT INTO users (id, name, email, age) VALUES ('5', 'John Doe', 'john@example.com', '32')");
    println!("   UPDATE users SET age = '26' WHERE id = '1'");
    println!("   UPDATE users SET email = 'new.email@example.com' WHERE name = 'Jane Doe'");
    println!("   DELETE FROM users WHERE age > '35'");
    println!("   DELETE FROM users WHERE id = '4'");
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

fn show_tables(filesystem: &FileSystem) {
    println!("📋 Available Tables:");
    println!("===================");
    
    if filesystem.storage_engine.tables.is_empty() {
        println!("   No tables found in the database.");
        return;
    }

    for (table_name, table) in &filesystem.storage_engine.tables {
        println!("   🗂️  Table: {}", table_name);
        println!("      Columns: {}", table.columns.join(", "));
        if let Some(pk) = &table.primary_key {
            println!("      Primary Key: {}", pk);
        }
        println!("      Rows: {}", table.rows.len());
        
        // Show table statistics if available
        if let Some(stats) = filesystem.storage_engine.get_table_stats(table_name) {
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

fn show_database_statistics(filesystem: &FileSystem, query_planner: &QueryPlanner) {
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
    for (table_name, table) in &filesystem.storage_engine.tables {
        if let Some(stats) = filesystem.storage_engine.get_table_stats(table_name) {
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
