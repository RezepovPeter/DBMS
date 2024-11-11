use crate::Schema;
use crate::{ MyVec, MyHashMap };
use std::fs;
use fs::OpenOptions;
use std::io::{ BufReader, BufRead, Write };
use std::sync::{ Arc, Mutex };

pub fn read_schema(path: &str) -> serde_json::Result<Schema> {
    let file = fs::File::open(path).expect("cannot open schema.json file");
    let reader = BufReader::new(file);

    let schema: Schema = serde_json::from_reader(reader).expect("cannot parse json into structure");

    Ok(schema)
}

// Function to perform Cartesian product of rows from two tables
pub fn cartesian_product(
    table1: &MyVec<MyHashMap<String, String>>,
    table2: &MyVec<MyHashMap<String, String>>
) -> MyVec<MyHashMap<String, String>> {
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

pub fn read_all_table_data(
    table_name: &str,
    schema: &Schema
) -> Result<MyVec<MyHashMap<String, String>>, String> {
    let mut all_data = MyVec::new();
    let mut file_index = 1;

    loop {
        // Form the path to the file
        let file_path = format!("{}/{}/{}.csv", schema.name, table_name, file_index);

        // Try to open the file
        let file_result = OpenOptions::new().read(true).open(&file_path);

        match file_result {
            Ok(file) => {
                let file_mutex = Arc::new(Mutex::new(file));

                // Блокируем файл для чтения
                let file_lock = match file_mutex.lock() {
                    Ok(locked_file) => locked_file,
                    Err(_) => {
                        return Err(format!("Failed to lock file: {}", file_path));
                    }
                };

                // Если файл успешно заблокирован, выполняем чтение
                let reader = BufReader::new(&*file_lock);
                let mut lines = reader.lines();

                // Read the header
                let header_line = lines.next().unwrap().unwrap();
                let headers: MyVec<&str> = header_line.split(',').collect();

                // Read data from the file and add to all_data
                for line in lines {
                    let line = line.unwrap();
                    let values: MyVec<&str> = line.split(',').collect();
                    let mut row = MyHashMap::new();
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

    Ok(all_data)
}

pub fn find_not_full_csv(schema: &Schema, table: &str) -> Result<i32, String> {
    let mut not_full_csv_index = 0;
    let file_mutex = Arc::new(Mutex::new(()));

    loop {
        not_full_csv_index += 1;
        let path = format!("{}/{}/{}.csv", schema.name, table, not_full_csv_index);

        // Блокируем доступ к файлу с помощью Mutex
        let _lock = match file_mutex.lock() {
            Ok(m) => m,
            Err(_) => {
                return Err("Table is blocked".to_string());
            }
        }; // Блокировка синхронизирована

        // Try to open the file
        match OpenOptions::new().read(true).open(&path) {
            Ok(file) => {
                // If the file is open, check the number of lines
                let reader = BufReader::new(file);
                if reader.lines().count() - 1 < (schema.tuples_limit as usize) {
                    // If the file is not full, return it
                    return Ok(not_full_csv_index);
                }
                // If the file is full, continue searching
            }
            Err(_) => {
                // If the file does not exist, create a new one
                let mut file = fs::File::create(&path).expect("failed to create new csv file");
                let columns = schema.structure.get(table).unwrap().join(",");
                writeln!(file, "{}", columns).unwrap();
                return Ok(not_full_csv_index);
            }
        }
    }
}
