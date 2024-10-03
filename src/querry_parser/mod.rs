use crate::Schema;
use crate::MyVec;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{ BufRead, Write, BufReader };

struct Condition {
    field: String,
    value: String,
}

pub fn parse_insert(input: String, schema: &Schema) {
    let parts: MyVec<&str> = input.split_whitespace().collect();
    let table = parts[2];

    if let Some(values_index) = parts.iter().position(|&x| x == "VALUES") {
        let values_part = &parts[values_index + 1..].join(" "); // Combine remaining parts
        let values = values_part.trim_start_matches('(').trim_end_matches(')').trim(); // Remove parentheses
        let values_list: MyVec<&str> = values.split("), (").collect(); // Split by comma and parentheses

        // Write each record to CSV
        for value in values_list.iter() {
            let cleaned_value = value
                .replace("'", "")
                .replace("(", "")
                .replace(")", "")
                .replace(" ", ""); // Remove extra characters

            let not_full_csv_index = find_not_full_csv(schema, table);
            let path = format!("{}/{}/{}.csv", schema.name, table, not_full_csv_index);
            let mut not_full_csv = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&path)
                .expect("failed to open CSV file for writing");
            writeln!(not_full_csv, "{}", cleaned_value).expect("failed to write data to CSV");
        }
    } else {
        println!("'VALUES' not found");
    }
}

fn find_not_full_csv(schema: &Schema, table: &str) -> i32 {
    let mut not_full_csv_index = 0;

    loop {
        not_full_csv_index += 1;
        let path = format!("{}/{}/{}.csv", schema.name, table, not_full_csv_index);

        // Try to open the file
        match fs::File::open(&path) {
            Ok(file) => {
                // If the file is open, check the number of lines
                let reader = BufReader::new(&file);
                if reader.lines().count() - 1 < (schema.tuples_limit as usize) {
                    // If the file is not full, return it
                    return not_full_csv_index;
                }
                // If the file is full, continue searching
            }
            #[allow(unused_variables)]
            Err(e) => {
                // If the file does not exist, create a new one
                let mut file = fs::File::create(&path).expect("failed to create new csv file");
                let columns = schema.structure.get(table).unwrap().join(",");
                writeln!(file, "{}", columns).unwrap();
                return not_full_csv_index;
            }
        }
    }
}

pub fn parse_delete(querry: String, schema: &Schema) {
    let parts: MyVec<&str> = querry.split(" ").collect();
    let table = parts[2];
    if let Some(head) = schema.structure.get(table) {
        let parsed_conditions = parse_where(&querry).unwrap();
        let mut file_index = 0;
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
                let mut data_for_condition: HashMap<String, String> = HashMap::new();
                for (field, value) in head.iter().zip(line.iter()) {
                    data_for_condition.insert(format!("{}.{}", table, field), value.to_string());
                }
                if !eval_conditions(&parsed_conditions, &data_for_condition) {
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
    } else {
        println!("No such table in DB");
        return;
    }
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

fn eval_conditions(conditions: &MyVec<MyVec<Condition>>, data: &HashMap<String, String>) -> bool {
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

pub fn parse_select(querry: String, schema: &Schema) {
    let parts: MyVec<&str> = querry.split(" ").collect();
    let parsed_conditions = parse_where(&querry);
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

    let mut table_columns: HashMap<String, MyVec<String>> = HashMap::new();

    // Iterate over each column and split it into table and column name
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
    execute_select(
        tables
            .iter()
            .map(|s| s.to_string())
            .collect(), // Convert MyVec<&str> to MyVec<String>
        table_columns, // Pass parsed tables and columns
        schema, // Pass schema (table data)
        parsed_conditions // Pass WHERE conditions
    );
}

fn execute_select(
    tables: MyVec<String>,
    table_columns: HashMap<String, MyVec<String>>,
    schema: &Schema,
    conditions: Option<MyVec<MyVec<Condition>>>
) {
    // Read all data for each table
    let mut table_data: MyVec<MyVec<HashMap<String, String>>> = MyVec::new();
    for table in tables.iter() {
        let data = read_all_table_data(table, schema); // Use the new function
        table_data.push(data);
    }

    // Perform table joins (Cartesian product)
    let mut joined_data = table_data[0].clone(); // Start with the first table
    for i in 1..table_data.len() {
        joined_data = cartesian_product(&joined_data, &table_data[i]);
    }

    // Filter rows if there are WHERE conditions
    let filtered_data = if let Some(conds) = conditions {
        let mut result = MyVec::new(); // Create a new MyVec

        for row in joined_data.iter() {
            // Iterate over references
            if eval_conditions(&conds, row) {
                result.push(row.clone()); // Clone and add to the new MyVec
            }
        }

        result // Return filtered data
    } else {
        joined_data // If no conditions, return the original data
    };

    // Select and print the needed columns
    for row in filtered_data.iter() {
        let mut selected_row = MyVec::new();
        for (table, cols) in &table_columns {
            for col in cols.iter() {
                let key = format!("{}.{}", table, col); // Form key: table1.Surname
                if let Some(value) = row.get(&key) {
                    selected_row.push(value.as_str());
                }
            }
        }
        println!("{}", selected_row.join(", "));
    }
}

fn read_all_table_data(table_name: &str, schema: &Schema) -> MyVec<HashMap<String, String>> {
    let mut all_data = MyVec::new();
    let mut file_index = 1;

    loop {
        // Form the path to the file
        let file_path = format!("{}/{}/{}.csv", schema.name, table_name, file_index);

        // Try to open the file
        match fs::File::open(&file_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                let mut lines = reader.lines();

                // Read the header
                let header_line = lines.next().unwrap().unwrap();
                let headers: MyVec<&str> = header_line.split(',').collect();

                // Read data from the file and add to all_data
                for line in lines {
                    let line = line.unwrap();
                    let values: MyVec<&str> = line.split(',').collect();
                    let mut row = HashMap::new();
                    for (header, value) in headers.iter().zip(values.iter()) {
                        row.insert(format!("{}.{}", table_name, header), value.to_string());
                    }
                    all_data.push(row);
                }

                file_index += 1; // Move to the next file
            }
            Err(_) => {
                // If the file is not found, exit the loop
                break;
            }
        }
    }

    all_data
}

// Function to perform Cartesian product of rows from two tables
fn cartesian_product(
    table1: &MyVec<HashMap<String, String>>,
    table2: &MyVec<HashMap<String, String>>
) -> MyVec<HashMap<String, String>> {
    let mut result = MyVec::new();

    for row1 in table1.iter() {
        for row2 in table2.iter() {
            let mut combined_row = row1.clone(); // Clone the first row
            combined_row.extend(row2.clone()); // Add data from the second row
            result.push(combined_row);
        }
    }
    return result;
}
