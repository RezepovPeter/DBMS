use crate::{ Schema, Condition, DbResponse };
use crate::{ MyVec, MyHashMap };
use crate::db_api::{ /*lock_table, unlock_table, is_locked,*/ increment_pk_sequence };
use crate::utils::{ cartesian_product, read_all_table_data, find_not_full_csv };
use std::fs::OpenOptions;
use std::io::{ BufRead, Write, BufReader };
use std::sync::{ Mutex, Arc };

//Execute Functions
fn execute_insert(table: &str, values_list: MyVec<&str>, schema: &Schema) -> DbResponse {
    for value in values_list.iter() {
        let cleaned_value = value
            .replace("'", "")
            .replace("(", "")
            .replace(")", "")
            .replace(" ", "");

        let not_full_csv_index = find_not_full_csv(schema, table);
        let path = format!("{}/{}/{}.csv", schema.name, table, not_full_csv_index);

        let not_full_csv_mutex = Arc::new(
            Mutex::new(
                OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&path)
                    .expect("failed to open CSV file for writing")
            )
        );
        let mut not_full_csv = match not_full_csv_mutex.lock() {
            Ok(m) => m,
            Err(_) => {
                return DbResponse::Error("Table is currently locked".to_string());
            }
        };
        let id = increment_pk_sequence(schema.name.as_str(), table);
        writeln!(not_full_csv, "{}", format!("{},{}", id, cleaned_value)).expect(
            "failed to write data to CSV"
        );
    }
    return DbResponse::Success(None);
}

fn execute_delete(
    table: &str,
    parsed_conditions: MyVec<MyVec<Condition>>,
    schema: &Schema
) -> DbResponse {
    if let Some(head) = schema.structure.get(table) {
        let mut file_index = 0;

        loop {
            file_index += 1;
            let mut remaining_lines: MyVec<String> = MyVec::new();
            remaining_lines.push(head.join(",")); // Добавляем заголовок в оставшиеся строки

            let path = format!("{}/{}/{}.csv", schema.name, table, file_index);
            let file_result = OpenOptions::new().read(true).open(&path);

            // Если файл не найден, выходим из цикла
            let file = match file_result {
                Ok(file) => file,
                Err(_) => {
                    break;
                }
            };

            // Сохраняем результат Mutex в переменную
            let file_mutex = Mutex::new(file);

            // Блокируем файл для чтения
            let table_file = match file_mutex.lock() {
                Ok(m) => m,
                Err(_) => {
                    return DbResponse::Error("Table is currently locked".to_string());
                }
            };

            // Передаем файл в BufReader, используя deref() для доступа к файлу
            let reader = BufReader::new(&*table_file);
            let mut lines = reader.lines();
            lines.next(); // Пропускаем заголовок

            // Обрабатываем каждую строку
            for line in lines {
                let line = line.unwrap();
                let line: MyVec<&str> = line.split(",").collect();
                let mut data_for_condition: MyHashMap<String, String> = MyHashMap::new();

                for (field, value) in head.iter().zip(line.iter()) {
                    data_for_condition.insert(format!("{}.{}", table, field), value.to_string());
                }

                // Если строка не подлежит удалению, добавляем её в оставшиеся строки
                if !execute_conditions(&parsed_conditions, &data_for_condition) {
                    remaining_lines.push(line.join(","));
                }
            }

            // Открываем файл для записи, очищаем его
            let mut table_file = match OpenOptions::new().write(true).truncate(true).open(&path) {
                Ok(file) => file,
                Err(_) => {
                    return DbResponse::Error("Failed to open file for writing".to_string());
                }
            };

            // Записываем оставшиеся строки обратно в файл
            for line in remaining_lines.iter() {
                writeln!(table_file, "{}", line).expect("Failed to write line");
            }
        }

        DbResponse::Success(None)
    } else {
        DbResponse::Error("No such table in DB".to_string())
    }
}

fn execute_select(
    tables: MyVec<&str>,
    columns: MyVec<&str>,
    conditions: Option<MyVec<MyVec<Condition>>>,
    schema: &Schema
) -> DbResponse {
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
        let data = match read_all_table_data(table, schema) {
            Ok(d) => d,
            Err(_) => {
                return DbResponse::Error("One or more tables are currently locked".to_string());
            }
        };
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
            .collect::<MyVec<_>>()
    } else {
        joined_data
    };

    let mut result_matrix = Vec::new();

    for row in filtered_data.iter() {
        let mut selected_row = Vec::new();
        for (table, cols) in table_columns.iter() {
            for col in cols.iter() {
                let key = format!("{}.{}", table, col);
                if let Some(value) = row.get(&key) {
                    selected_row.push(value.clone());
                }
            }
        }
        result_matrix.push(selected_row);
    }

    return DbResponse::Success(Some(result_matrix));
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
pub fn parse_insert(input: String, schema: &Schema) -> DbResponse {
    let parts: MyVec<&str> = input.split_whitespace().collect();
    let table = parts[2];

    if let Some(values_index) = parts.iter().position(|&x| x == "VALUES") {
        let values_part = &parts[values_index + 1..].join(" ");
        let values = values_part.trim_start_matches('(').trim_end_matches(')').trim();
        let values_list: MyVec<&str> = values.split("), (").collect();
        return execute_insert(table, values_list, schema);
    } else {
        return DbResponse::Error("'VALUES' not found".to_string());
    }
}

pub fn parse_delete(query: String, schema: &Schema) -> DbResponse {
    let parts: MyVec<&str> = query.split(" ").collect();
    let table = parts[2];

    if let Some(parsed_conditions) = parse_where(&query) {
        return execute_delete(table, parsed_conditions, schema);
    } else {
        return DbResponse::Error("No WHERE clause found".to_string());
    }
}

pub fn parse_select(query: String, schema: &Schema) -> DbResponse {
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

    return execute_select(tables, columns, parsed_conditions, schema);
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
