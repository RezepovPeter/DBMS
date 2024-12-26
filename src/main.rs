mod querry_parser;
mod vector;
mod hash_map;
mod db_api;
mod structs;
mod utils;

#[cfg(test)]
mod tests;

use tokio::net::TcpListener;
#[allow(unused_imports)]
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use structs::{ Schema, Condition, DbResponse };
use db_api::{ execute_query, init_db, clear_csv_files };
use vector::MyVec;
use hash_map::MyHashMap;
use utils::read_schema;

#[tokio::main]
async fn main() -> std::io::Result<()> {
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

    let listener = TcpListener::bind("0.0.0.0:1337").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let schema = schema.clone();

        tokio::spawn(async move {
            let mut buffer = vec![0; 1024];
            loop {
                let received_data = match socket.read(&mut buffer).await {
                    Ok(inp) if inp == 0 => {
                        return;
                    }
                    Ok(inp) => { inp }
                    Err(_) => {
                        return;
                    }
                };

                let received_data = match String::from_utf8(buffer[..received_data].to_vec()) {
                    Ok(string) => string,
                    Err(e) => {
                        eprintln!("Failed to transform received_data into string: {}", e);
                        return;
                    }
                };

                if received_data.trim() == "CLEAR DB" {
                    clear_csv_files(&schema);
                } else {
                    match execute_query(received_data, &schema) {
                        DbResponse::Success(None) => {
                            socket.write_all("SUCCES\n".as_bytes()).await.unwrap();
                            socket.write_all("END\n".as_bytes()).await.unwrap();
                        }
                        DbResponse::Error(error) => {
                            socket.write_all(format!("{}\n", error).as_bytes()).await.unwrap();
                            socket.write_all("END\n".as_bytes()).await.unwrap();
                        }
                        DbResponse::Success(Some(matrix)) => {
                            socket.write_all("SUCCES\n".as_bytes()).await.unwrap();
                            for row in matrix.iter() {
                                let row_str = row
                                    .iter()
                                    .map(|x| x.to_string())
                                    .collect::<Vec<String>>()
                                    .join(" ");
                                socket
                                    .write_all(format!("{}\n", row_str).as_bytes()).await
                                    .unwrap();
                            }
                            socket.write_all("END\n".as_bytes()).await.unwrap();
                        }
                    };
                }
            }
        });
    }
}
