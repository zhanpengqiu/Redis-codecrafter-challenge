#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener,TcpStream},
};
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                handle_client(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop{
        let read_count=stream.read(&mut buffer).unwrap();
        if read_count == 0 {
            break;
        }

        stream.write(b"+PONG\r\n").unwrap();
    }

}