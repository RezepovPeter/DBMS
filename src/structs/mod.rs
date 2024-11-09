use std::collections::HashMap;
use serde::{ Deserialize, Serialize };

pub struct Condition {
    pub field: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub name: String,
    pub tuples_limit: i32,
    pub structure: HashMap<String, Vec<String>>,
}
