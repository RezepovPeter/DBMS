use crate::{ Schema, Condition };
use crate::{ MyVec, MyHashMap };
use crate::db_api::{ lock_table, unlock_table, increment_pk_sequence, is_locked };
use crate::utils::{ cartesian_product, read_all_table_data, find_not_full_csv };
use std::fs::OpenOptions;
use std::io::{ BufRead, Write, BufReader };

//Execute Functions
fn execute_insert(table: &str, values_list: MyVec<&str>, schema: &Schema) {
    if !is_locked(table, schema) {
        lock_table(table, schema);

        for value in values_list.iter() {
            let cleaned_value = value
                .replace("'", "")
                .replace("(", "")
                .replace(")", "")
                .replace(" ", "");

            let not_full_csv_index = find_not_full_csv(schema, table);
            let path = format!("{}/{}/{}.csv", schema.name, table, not_full_csv_index);

            let mut not_full_csv = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&path)
                .expect("failed to open CSV file for writing");

            increment_pk_sequence(schema.name.as_str(), table);
            writeln!(not_full_csv, "{}", cleaned_value).expect("failed to write data to CSV");
        }
        unlock_table(table, schema);
    }
}

fn execute_delete(table: &str, parsed_conditions: MyVec<MyVec<Condition>>, schema: &Schema) {
    if !is_locked(table, schema) {
        if let Some(head) = schema.structure.get(table) {
            let mut file_index = 0;
            lock_table(table, schema);
            loop {
                file_index += 1;
                let mut remaining_lines: MyVec<String> = MyVec::new();
                remaining_lines.push(head.join(",")); // Add header to remaining lines
                let path = format!("{}/{}/{}.csv", schema.name, table, file_index);
                let mut table_file = match OpenOptions::new().read(true).open(&path) {
                    Ok(file) => file,
                    Err(_) => {
                        break;
                    } // Break the loop if the file is not found
                };
                let reader = BufReader::new(table_file);
                let mut lines = reader.lines();
                lines.next();
                for line in lines {
                    let line = line.unwrap();
                    let line: MyVec<&str> = line.split(",").collect();
                    let mut data_for_condition: MyHashMap<String, String> = MyHashMap::new();
                    for (field, value) in head.iter().zip(line.iter()) {
                        data_for_condition.insert(
                            format!("{}.{}", table, field),
                            value.to_string()
                        );
                    }
                    if !execute_conditions(&parsed_conditions, &data_for_condition) {
                        remaining_lines.push(line.join(",")); // Save the line if it is not deleted
                    }
                }
                table_file = OpenOptions::new()
                    .write(true)
                    .truncate(true) // Clear the file
                    .open(&path)
                    .expect("Failed to open file for truncation");

                // Write remaining lines
                for line in remaining_lines.iter() {
                    writeln!(table_file, "{}", line).expect("Failed to write line");
                }
            }
            unlock_table(table, schema);
        } else {
            println!("No such table in DB");
        }
    }
}

fn execute_select(
    tables: MyVec<&str>,
    columns: MyVec<&str>,
    conditions: Option<MyVec<MyVec<Condition>>>,
    schema: &Schema
) {
    if !is_locked(tables[0], schema) && !is_locked(tables[1], schema) {
        let mut table_columns: MyHashMap<String, MyVec<String>> = MyHashMap::new();

        for table in tables.iter() {
            table_columns.insert(table.to_string(), MyVec::new());
        }
        for column in columns.iter() {
            if let Some((table, column_name)) = column.split_once(".") {
                if let Some(columns_vector) = table_columns.get_mut(&table.to_string()) {
                    columns_vector.push(column_name.to_string());
                }
            }
        }

        let tables: MyVec<String> = tables
            .iter()
            .map(|&s| s.to_string())
            .collect();
        let mut table_data: MyVec<MyVec<MyHashMap<String, String>>> = MyVec::new();

        for table in tables.iter() {
            let data = read_all_table_data(table, schema);
            table_data.push(data);
        }

        let mut joined_data = table_data[0].clone();
        for i in 1..table_data.len() {
            joined_data = cartesian_product(&joined_data, &table_data[i]);
        }

        let filtered_data = if let Some(conds) = conditions {
            joined_data
                .iter()
                .cloned()
                .filter(|row| execute_conditions(&conds, row))
                .collect()
        } else {
            joined_data
        };

        for row in filtered_data.iter() {
            let mut selected_row = MyVec::new();
            for (table, cols) in table_columns.iter() {
                for col in cols.iter() {
                    let key = format!("{}.{}", table, col);
                    if let Some(value) = row.get(&key) {
                        selected_row.push(value.as_str());
                    }
                }
            }
            println!("{}", selected_row.join(", "));
        }
    }
}

fn execute_conditions(
    conditions: &MyVec<MyVec<Condition>>,
    data: &MyHashMap<String, String>
) -> bool {
    for and_group in conditions.iter() {
        let mut group_result = true;
        for condition in and_group.iter() {
            if let Some(data_value) = data.get(&condition.field) {
                if data_value != &condition.value {
                    group_result = false;
                    break;
                }
            } else {
                group_result = false;
                break;
            }
        }
        if group_result {
            return true;
        }
    }
    return false;
}

//Parser functions
pub fn parse_insert(input: String, schema: &Schema) {
    let parts: MyVec<&str> = input.split_whitespace().collect();
    let table = parts[2];

    if let Some(values_index) = parts.iter().position(|&x| x == "VALUES") {
        let values_part = &parts[values_index + 1..].join(" ");
        let values = values_part.trim_start_matches('(').trim_end_matches(')').trim();
        let values_list: MyVec<&str> = values.split("), (").collect();
        execute_insert(table, values_list, schema);
    } else {
        println!("'VALUES' not found");
    }
}

pub fn parse_delete(query: String, schema: &Schema) {
    let parts: MyVec<&str> = query.split(" ").collect();
    let table = parts[2];

    if let Some(parsed_conditions) = parse_where(&query) {
        execute_delete(table, parsed_conditions, schema);
    } else {
        println!("No WHERE clause found");
    }
}

pub fn parse_select(query: String, schema: &Schema) {
    let parts: MyVec<&str> = query.split(" ").collect();
    let parsed_conditions = parse_where(&query);
    let select_index = parts
        .iter()
        .position(|&x| x == "SELECT")
        .expect("No SELECT found");
    let from_index = parts
        .iter()
        .position(|&x| x == "FROM")
        .expect("No FROM found");
    let where_index: Option<usize> = parts.iter().position(|&x| x == "WHERE");

    let columns_part = parts[select_index + 1..from_index].join(" ");
    let columns: MyVec<&str> = columns_part
        .split(',')
        .map(|col| col.trim())
        .collect();

    let tables_part = if let Some(where_idx) = where_index {
        parts[from_index + 1..where_idx].join(" ")
    } else {
        parts[from_index + 1..].join(" ")
    };
    let tables: MyVec<&str> = tables_part
        .split(',')
        .map(|table| table.trim())
        .collect();

    execute_select(tables, columns, parsed_conditions, schema);
}

fn parse_where(querry: &String) -> Option<MyVec<MyVec<Condition>>> {
    if let Some(where_index) = querry.find("WHERE") {
        let where_clause = &querry[where_index + 6..];
        let or_conditions: MyVec<&str> = where_clause.split(" OR ").collect();
        let mut parsed_conditions = MyVec::new();
        for or_condition in or_conditions.iter() {
            let and_conditions: MyVec<&str> = or_condition.split(" AND ").collect();
            let mut conditions = MyVec::new();
            for condition in and_conditions.iter() {
                if let Some(parsed_condition) = parse_condition(condition) {
                    conditions.push(parsed_condition);
                }
            }
            parsed_conditions.push(conditions);
        }
        return Some(parsed_conditions);
    }
    None
}

fn parse_condition(condition: &str) -> Option<Condition> {
    // Find the operator and split the condition
    if let Some(pos) = condition.find("=") {
        let field = condition[..pos].trim().to_string();
        let value = condition[pos + 1..].trim().replace("'", "").to_string(); // Right operand (value)
        return Some(Condition {
            field,
            value,
        });
    }

    None
}
