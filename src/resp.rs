use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt}};
use std::fmt::{Write};
use bytes::BytesMut;
use anyhow::Result;
use std::fmt;
// 参数用于输入到database中
#[derive(Clone, Debug,Eq, Hash, PartialEq)]
pub enum Value {
    SimpleString(String),
    Error(String),
    BulkString(Option<String>),
    Integer(i64),
    Array(Vec<Value>),
}
impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::Error(s) => format!("-{}\r\n", s),
            Value::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            Value::BulkString(None) => format!("$-1\r\n",),
            Value::Integer(i) => format!(":{}\r\n", i),
            Value::Array(v) => {
                let mut s = String::new();
                write!(s, "*{}\r\n", v.len()).unwrap();
                for item in v {
                    write!(s, "{}", item.serialize()).unwrap();
                }
                s
            }
        }
    }
}
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::SimpleString(s) => write!(f, "{}", s),
            Value::Error(e) => write!(f, "{}", e),
            Value::BulkString(Some(s)) => write!(f, "{}", s),
            Value::BulkString(None) => write!(f, "(nil)"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Array(arr) => {
                let mut output = String::new();
                for item in arr {
                    output.push_str(&format!("{}\n", item));
                }
                write!(f, "{}", output.trim_end())
            }
        }
    }
}


pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}
impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }
    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse_message(self.buffer.split())?;
        Ok(Some(v))
    }
    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write(value.serialize().as_bytes()).await?;
        Ok(())
    }
}
fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}
fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();
        return Ok((Value::SimpleString(string), len + 1))
    }
    return Err(anyhow::anyhow!("Invalid string {:?}", buffer));
}
fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let (array_length, mut bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let array_length = parse_int(line)?;
        (array_length, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };
    let mut items = vec![];
    for _ in 0..array_length {
        let (array_item, len) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        items.push(array_item);
        bytes_consumed += len;
    }
    return Ok((Value::Array(items), bytes_consumed))
}
fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let bulk_str_len = parse_int(line)?;
        (bulk_str_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };
    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;
    Ok((Value::BulkString(Some(String::from_utf8(buffer[bytes_consumed..end_of_bulk_str].to_vec())?)), total_parsed))
}
fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}
fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}