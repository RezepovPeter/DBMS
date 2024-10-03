use std::io::Write;
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
        let data_path = Path::new(data_path.as_str());
        if !data_path.exists() {
            let mut file = fs::File::create(data_path).expect("failed to create csv file");
            // Update CSV files
            let header = columns.join(",");
            writeln!(file, "{}", header).expect("failed to make header of csv file");
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
