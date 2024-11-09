mod querry_parser;
mod vector;
mod hash_map;
mod db_api;
mod structs;
mod utils;

use structs::{ Schema, Condition };
use db_api::{ execute_query, init_db, clear_csv_files };
use vector::MyVec;
use hash_map::MyHashMap;
use std::io;
use utils::read_schema;

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
