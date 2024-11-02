#![allow(unused_imports)]
mod resp;
use crate::resp::Value;
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    handle_conn(stream).await
                    });
                }
                Err(e) => {
                    println!("error: {}", e);

                }
        }
    }
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n

async fn handle_conn(stream: TcpStream) {
    let mut handler = resp::RespHandler::new(stream);
    println!("Starting read loop");
    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);
        
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_lowercase().as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                c => {println!("{:?}",command);panic!("Cannot handle command {}", c)}
            }
        } else {
            break;
        };
        println!("Sending value {:?}", response);
        handler.write_value(response).await.unwrap();
    }
}
fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        },
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}
fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}