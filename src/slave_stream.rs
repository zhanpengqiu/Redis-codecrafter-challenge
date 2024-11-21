// 定义 Slaves 结构体
use crate::resp::RespHandler;
use crate::resp::Value;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time;
use std::fs::File;
use std::io::{self, Read};
#[warn(unused_variables)]
#[derive(Debug)]
pub struct Slaves {
    slave_addrs: Arc<Mutex<HashMap<String,String>>>,
    slave_handler:Arc<Mutex<Vec<RespHandler>>>,
    // command: String,
    slave_offsets:Arc<Mutex<Vec<i32>>>,
}

impl Slaves {
    pub fn new() -> Self {
        Slaves {
            slave_addrs: Arc::new(Mutex::new(HashMap::new())),
            slave_handler: Arc::new(Mutex::new(Vec::new())),
            slave_offsets: Arc::new(Mutex::new(Vec::new())),
        }
    }
    //用于循环发送心跳包给slave
    pub async fn r#loop(&self) {
        loop {

            //每个一秒钟处理一次
            time::sleep(time::Duration::from_secs(1)).await;
        }
    }

    pub async fn shake_hand_addr_info(&self, in_addr: String,listen_addr: String) {
        let mut slave_addrs_lock = self.slave_addrs.lock().await;
        
        slave_addrs_lock.insert(in_addr,listen_addr);
        println!("New ShakeHand connection came");
    }

    pub async fn add_new_slave_handler(&mut self,in_addr:String){
        let slave_addrs_lock = self.slave_addrs.lock().await;
        
        match slave_addrs_lock.get(&in_addr){
            Some(listen_addr) => {
                let mut slave_handler_lock = self.slave_handler.lock().await;
                let mut slave_offsets_lock = self.slave_offsets.lock().await;
                println!("{:?}",listen_addr);
                let mut handler = RespHandler::new(TcpStream::connect(listen_addr).await.unwrap());
                println!("Add new Slave{:?}", handler);
                let val=Slaves::get_rdbfile("dasd".to_string());
                handler.write_value(val).await.unwrap();
                slave_handler_lock.push(handler);
                slave_offsets_lock.push(0);
            },
            None => println!("No handler found for {}",in_addr),
        }
    }
    pub fn get_rdbfile(path:String)->Value{
        // 打开文件
        let mut file = File::open("./dump.rdb").unwrap();

        // 创建一个缓冲区
        let mut buffer = Vec::new();

        // 读取文件内容到缓冲区
        file.read_to_end(&mut buffer).unwrap();

        Value::RdbFile(buffer)
    }


}