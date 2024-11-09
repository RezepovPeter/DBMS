use std::io::{ Read, Write };
use std::fs::OpenOptions;
use std::path::Path;
use crate::querry_parser::{ parse_insert, parse_delete, parse_select };
use crate::Schema;
use std::fs;

pub fn execute_query(query: String, schema: &Schema) {
    if query.starts_with("INSERT INTO") {
        parse_insert(query, &schema);
    } else if query.starts_with("DELETE FROM") {
        parse_delete(query, &schema);
    } else if query.starts_with("SELECT") {
        parse_select(query, &schema);
    } else {
        println!("Bad query");
    }
}

pub fn init_db(schema: &Schema) {
    // Clear all CSV files except 1.csv
    // clear_csv_files(&schema);

    // Create the database
    fs::create_dir_all(&schema.name).expect("failed to create dir for DB");
    for (table_name, columns) in &schema.structure {
        // Create tables
        let table_path = format!("{}/{}", &schema.name, &table_name);
        fs::create_dir_all(&table_path).unwrap();

        // Create CSV files
        let data_path = format!("{}/{}", &table_path, "1.csv");
        let block_path = format!("{}/{}_pk", &table_path, table_name);
        let block_path_sequence = format!("{}/{}_pk_sequence", &table_path, table_name);
        let lock_path = format!("{}/{}_lock", &table_path, table_name);
        let data_path = Path::new(data_path.as_str());
        let block_path = Path::new(block_path.as_str());
        let block_path_sequence = Path::new(block_path_sequence.as_str());
        let lock_path = Path::new(lock_path.as_str());
        if !data_path.exists() {
            let mut file = fs::File::create(data_path).expect("failed to create csv file");
            // Update CSV files
            let header = columns.join(",");
            writeln!(file, "{}", header).expect("failed to make header of csv file");
        }
        if !block_path.exists() {
            let mut file = fs::File::create(block_path).expect("failed to create block file");
            writeln!(file, "0").unwrap();
        }
        if !block_path_sequence.exists() {
            let mut file = fs::File
                ::create(block_path_sequence)
                .expect("failed to create block_sequence file");
            writeln!(file, "0").unwrap();
        }
        if !lock_path.exists() {
            let mut file = fs::File
                ::create(lock_path)
                .expect("failed to create block_sequence file");

            writeln!(file, "0").expect("Failed to write to file");
        }
    }
}

pub fn clear_csv_files(schema: &Schema) {
    let db_path = &schema.name;

    for (table_name, _) in &schema.structure {
        let table_path = format!("{}/{}", db_path, table_name);

        // Attempt to get the list of files in the table directory
        if let Ok(entries) = fs::read_dir(table_path) {
            for entry in entries.filter_map(Result::ok) {
                // Remove all files except 1.csv
                let file_path = entry.path();
                fs::remove_file(file_path).expect("Failed to remove file");
            }
        }
    }
}

pub fn is_locked(table_name: &str, schema: &Schema) -> bool {
    let lock_path = format!("{}/{}/{}_lock", schema.name, table_name, table_name);
    if let Ok(mut file) = fs::File::open(&lock_path) {
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Failed to read lock file");
        return content.trim() == "1";
    }
    false
}

pub fn lock_table(table_name: &str, schema: &Schema) {
    let lock_path = format!("{}/{}/{}_lock", schema.name, table_name, table_name);
    let mut file = fs::File::create(&lock_path).expect("failed to create lock file");
    writeln!(file, "1").expect("Failed to write to lock file");
}

pub fn unlock_table(table_name: &str, schema: &Schema) {
    let lock_path = format!("{}/{}/{}_lock", schema.name, table_name, table_name);
    let mut file = fs::File::create(&lock_path).expect("failed to create lock file");
    writeln!(file, "0").expect("Failed to write to lock file");
}

pub fn increment_pk_sequence(schema_name: &str, table_name: &str) {
    let sequence_path = format!("{}/{}/{}_pk_sequence", schema_name, table_name, table_name);
    let sequence = Path::new(&sequence_path);

    if sequence.exists() {
        let mut file = fs::File::open(&sequence).expect("Failed to open pk_sequence file");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Failed to read pk_sequence file");

        let current_value: u64 = content
            .trim()
            .parse()
            .expect("Invalid number in pk_sequence file");
        let new_value = current_value + 1;

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&sequence)
            .expect("Failed to open pk_sequence file for writing");
        writeln!(file, "{}", new_value).expect("Failed to write new pk_sequence value to file");
    } else {
        let mut file = fs::File::create(&sequence).expect("Failed to create pk_sequence file");
        writeln!(file, "1").expect("Failed to write to new pk_sequence file");
    }
}
