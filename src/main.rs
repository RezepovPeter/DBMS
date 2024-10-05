mod querry_parser;
mod vector;
mod hash_map;
mod db_api;

use db_api::{ execute_query, init_db, clear_csv_files };
use vector::MyVec;
use std::{ collections::HashMap, io };
use std::fs;
use std::io::BufReader;
use serde::{ Deserialize, Serialize };

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    name: String,
    tuples_limit: i32,
    structure: HashMap<String, Vec<String>>,
}

fn read_schema(path: &str) -> serde_json::Result<Schema> {
    let file = fs::File::open(path).expect("cannot open schema.json file");
    let reader = BufReader::new(file);

    let schema: Schema = serde_json::from_reader(reader).expect("cannot parse json into structure");

    Ok(schema)
}

// Function to clear all CSV files except 1.csv
#[allow(dead_code)]
fn main() {
    let schema: Schema;
    match read_schema("src/schema.json") {
        Ok(output) => {
            schema = output;
        }
        Err(e) => {
            println!("Failed to read schema: {}", e);
            std::process::exit(1);
        }
    }

    init_db(&schema);

    // User input
    loop {
        let mut query = String::new();
        io::stdin().read_line(&mut query).expect("Failed to read a query from console");
        if query.trim() == "CLEAR DB" {
            clear_csv_files(&schema);
        } else {
            execute_query(query, &schema);
        }
    }
}
