use std::collections::HashMap;
use serde::{ Deserialize, Serialize };

pub struct Condition {
    pub field: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub tuples_limit: i32,
    pub structure: HashMap<String, Vec<String>>,
}

pub enum DbResponse {
    Success(Option<Vec<Vec<String>>>),
    Error(String),
}
