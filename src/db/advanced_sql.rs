use super::parser::{AggregateFunc, JoinCondition, JoinType, OrderByItem};
use super::schema::Row;
use std::collections::HashMap;

pub fn execute_join(
    left: &[Row],
    right: &[Row],
    join_type: &JoinType,
    condition: &JoinCondition,
) -> Vec<Row> {
    let mut results = Vec::new();

    for left_row in left {
        let mut matched = false;

        for right_row in right {
            let left_value = get_join_value(left_row, &condition.left_table, &condition.left_column);
            let right_value = get_join_value(right_row, &condition.right_table, &condition.right_column);

            if left_value == right_value {
                matched = true;
                let mut combined_row = left_row.data.clone();
                for (key, value) in &right_row.data {
                    let prefixed_key = if condition.right_table.is_some() {
                        format!("{}.{}", condition.right_table.as_ref().unwrap(), key)
                    } else {
                        key.clone()
                    };
                    combined_row.insert(prefixed_key, value.clone());
                }
                results.push(Row { data: combined_row });
            }
        }

        match join_type {
            JoinType::Left if !matched => {
                let mut combined_row = left_row.data.clone();
                for key in right.first().map(|r| r.data.keys()).unwrap_or_default() {
                    let prefixed_key = if condition.right_table.is_some() {
                        format!("{}.{}", condition.right_table.as_ref().unwrap(), key)
                    } else {
                        key.clone()
                    };
                    combined_row.insert(prefixed_key, "NULL".to_string());
                }
                results.push(Row { data: combined_row });
            }
            JoinType::Right => {
                for right_row in right {
                    let left_value = get_join_value(left_row, &condition.left_table, &condition.left_column);
                    let right_value = get_join_value(right_row, &condition.right_table, &condition.right_column);
                    
                    if left_value != right_value {
                        let mut combined_row = HashMap::new();
                        for key in left_row.data.keys() {
                            let prefixed_key = if condition.left_table.is_some() {
                                format!("{}.{}", condition.left_table.as_ref().unwrap(), key)
                            } else {
                                key.clone()
                            };
                            combined_row.insert(prefixed_key, "NULL".to_string());
                        }
                        for (key, value) in &right_row.data {
                            let prefixed_key = if condition.right_table.is_some() {
                                format!("{}.{}", condition.right_table.as_ref().unwrap(), key)
                            } else {
                                key.clone()
                            };
                            combined_row.insert(prefixed_key, value.clone());
                        }
                        results.push(Row { data: combined_row });
                    }
                }
            }
            _ => {}
        }
    }

    results
}

fn get_join_value(row: &Row, table: &Option<String>, column: &str) -> String {
    if let Some(table_name) = table {
        row.data
            .get(&format!("{}.{}", table_name, column))
            .or_else(|| row.data.get(column))
            .cloned()
            .unwrap_or_default()
    } else {
        row.data.get(column).cloned().unwrap_or_default()
    }
}

pub fn execute_aggregate(rows: &[Row], func: &AggregateFunc, column: Option<&str>) -> Option<String> {
    if rows.is_empty() {
        return None;
    }

    match func {
        AggregateFunc::Count => {
            if column.is_none() || column == Some("*") {
                Some(rows.len().to_string())
            } else {
                let count = rows
                    .iter()
                    .filter(|row| {
                        column.and_then(|col| row.data.get(col)).is_some()
                    })
                    .count();
                Some(count.to_string())
            }
        }
        AggregateFunc::Sum => {
            let column = column?;
            let sum: f64 = rows
                .iter()
                .filter_map(|row| row.data.get(column)?.parse::<f64>().ok())
                .sum();
            Some(sum.to_string())
        }
        AggregateFunc::Avg => {
            let column = column?;
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|row| row.data.get(column)?.parse::<f64>().ok())
                .collect();
            if values.is_empty() {
                None
            } else {
                let avg = values.iter().sum::<f64>() / values.len() as f64;
                Some(avg.to_string())
            }
        }
        AggregateFunc::Max => {
            let column = column?;
            rows.iter()
                .filter_map(|row| {
                    row.data.get(column)?.parse::<f64>().ok()
                })
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .map(|v| v.to_string())
        }
        AggregateFunc::Min => {
            let column = column?;
            rows.iter()
                .filter_map(|row| {
                    row.data.get(column)?.parse::<f64>().ok()
                })
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .map(|v| v.to_string())
        }
    }
}

pub fn execute_group_by(
    rows: &[Row],
    group_columns: &[String],
    aggregates: &[(AggregateFunc, Option<String>)],
) -> Vec<Row> {
    let mut groups: HashMap<Vec<String>, Vec<&Row>> = HashMap::new();

    for row in rows {
        let key: Vec<String> = group_columns
            .iter()
            .map(|col| row.data.get(col).cloned().unwrap_or_default())
            .collect();
        groups.entry(key).or_insert_with(Vec::new).push(row);
    }

    let mut results = Vec::new();
    for (group_key, group_rows) in groups {
        let mut result_row = HashMap::new();
        
        for (i, col) in group_columns.iter().enumerate() {
            result_row.insert(col.clone(), group_key[i].clone());
        }

        for (func, column) in aggregates {
            let group_rows_slice: Vec<Row> = group_rows.iter().map(|r| (*r).clone()).collect();
            if let Some(value) = execute_aggregate(&group_rows_slice, func, column.as_deref()) {
                let agg_name = match func {
                    AggregateFunc::Count => "COUNT",
                    AggregateFunc::Sum => "SUM",
                    AggregateFunc::Avg => "AVG",
                    AggregateFunc::Max => "MAX",
                    AggregateFunc::Min => "MIN",
                };
                let col_name = column.as_ref().map(|c| c.as_str()).unwrap_or("*");
                result_row.insert(format!("{}({})", agg_name, col_name), value);
            }
        }

        results.push(Row { data: result_row });
    }

    results
}

pub fn execute_order_by(rows: &mut [Row], order_items: &[OrderByItem]) {
    rows.sort_by(|a, b| {
        for item in order_items {
            let a_val = a.data.get(&item.column.0).cloned().unwrap_or_default();
            let b_val = b.data.get(&item.column.0).cloned().unwrap_or_default();

            let comparison = if let (Ok(a_num), Ok(b_num)) = (a_val.parse::<f64>(), b_val.parse::<f64>()) {
                a_num.partial_cmp(&b_num).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                a_val.cmp(&b_val)
            };

            let result = if item.ascending {
                comparison
            } else {
                comparison.reverse()
            };

            if result != std::cmp::Ordering::Equal {
                return result;
            }
        }
        std::cmp::Ordering::Equal
    });
}

pub fn apply_limit_offset(rows: &[Row], limit: Option<usize>, offset: Option<usize>) -> Vec<Row> {
    let start = offset.unwrap_or(0);
    let end = limit.map(|l| start + l).unwrap_or(rows.len());
    
    rows.iter()
        .skip(start)
        .take(end - start)
        .cloned()
        .collect()
}

