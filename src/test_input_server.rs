use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::atomic::Ordering;
use chrono::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_config = ServerConfig {
        hostname: "127.0.0.1".to_string(),
        port: 8080,
        num_buffers: 10000,
        buffer_size: 10000,
        gathering_interval: 1,
        deadline: std::time::Duration::from_secs(1),
    };
    
    let executable_path = "/home/x/rustProjects/nes_simulation_starter_rs/target/debug/tcp_input_server";
    
    let mut server_process = start_server_process(executable_path, &server_config)?;
    
    //sleep
    std::thread::sleep(std::time::Duration::from_secs(5));
    
    //connect to server
    let mut client = std::net::TcpStream::connect(format!("{}:{}", server_config.hostname, server_config.port))?;
    
    //receive data from server
    let tuple_size = 40 - 16;
    let total_bytes = tuple_size * server_config.num_buffers * server_config.buffer_size;
    let mut buf = vec![0; total_bytes];
    
    let mut sleep_threshold = Some(total_bytes / 4);
    
    let mut bytes_read = client.read(&mut buf)?;
    while bytes_read < total_bytes {
        let new_bytes_read = client.read(&mut buf[bytes_read..])?;
        if new_bytes_read == 0 {
            break;
        }
        bytes_read += new_bytes_read;
        
        let gb_read = bytes_read as f64 / 1024.0 / 1024.0 / 1024.0;
        println!("Read {} bytes in total ({} GB)", bytes_read, gb_read);
        
        if let Some(threshold) = sleep_threshold {
            if bytes_read > threshold {
                println!("Sleeping for 10 seconds");
                std::thread::sleep(std::time::Duration::from_millis(10000));
                sleep_threshold = None;
            }
        }
    }
    
    for i in (0..buf.len()).step_by(tuple_size) {
        let tuple = &buf[i..i + tuple_size];
        let tuple_string = get_tuple_string(tuple);
        // println!("{}", tuple_string);
        let tuple_id = u64::from_le_bytes([tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], tuple[5], tuple[6], tuple[7]]);
        let sequence_number = u64::from_le_bytes([tuple[8], tuple[9], tuple[10], tuple[11], tuple[12], tuple[13], tuple[14], tuple[15]]);
        assert_eq!(tuple_id, 1); 
        assert_eq!(sequence_number, i as u64 / tuple_size as u64);
    }
    
    server_process.kill()?;
    
    Ok(()) 
}

struct Tuple {
    id: u64,
    sequence_number: u64,
    event_timestamp: u64,
}

fn get_tuple_string(binary_tuple: &[u8]) -> String {
    let id = u64::from_le_bytes([binary_tuple[0], binary_tuple[1], binary_tuple[2], binary_tuple[3], binary_tuple[4], binary_tuple[5], binary_tuple[6], binary_tuple[7]]);
    let sequence_number = u64::from_le_bytes([binary_tuple[8], binary_tuple[9], binary_tuple[10], binary_tuple[11], binary_tuple[12], binary_tuple[13], binary_tuple[14], binary_tuple[15]]);
    let event_timestamp = u64::from_le_bytes([binary_tuple[16], binary_tuple[17], binary_tuple[18], binary_tuple[19], binary_tuple[20], binary_tuple[21], binary_tuple[22], binary_tuple[23]]);
    // let ingestion_timestamp = u64::from_le_bytes([binary_tuple[24], binary_tuple[25], binary_tuple[26], binary_tuple[27], binary_tuple[28], binary_tuple[29], binary_tuple[30], binary_tuple[31]]);
    // let output_timestamp = u64::from_le_bytes([binary_tuple[32], binary_tuple[33], binary_tuple[34], binary_tuple[35], binary_tuple[36], binary_tuple[37], binary_tuple[38], binary_tuple[39]]);
    format!("{},{},{}", id, sequence_number, event_timestamp)
}

fn start_server_process(executable_path: &str, server_config: &ServerConfig) -> Result<std::process::Child, Box<dyn std::error::Error>> {
    println!("starting input server");
    let mut source_input_server_process = Command::new(&executable_path)
        .arg(server_config.hostname.clone())
        .arg(server_config.port.to_string())
        .arg(server_config.num_buffers.to_string())
        .arg(server_config.buffer_size.to_string())
        .arg(server_config.gathering_interval.to_string())
        .arg(server_config.deadline.as_millis().to_string())
        .spawn()?;
    println!("input server process id {}", source_input_server_process.id());
    Ok(source_input_server_process)
}

struct ServerConfig {
    pub hostname: String,
    pub port: u16,
    pub num_buffers: usize,
    pub buffer_size: usize,
    pub gathering_interval: u64,
    pub deadline: std::time::Duration,
}