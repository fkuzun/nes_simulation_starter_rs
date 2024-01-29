use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use std::env;

fn main() -> std::io::Result<()> {
    // Parse environment variables
    let args: Vec<String> = env::args().collect();
    // if args.len() != 6 {
    //     eprintln!("Usage: {} <hostname> <port> <num_buffers> <buffer_size> <gathering_interval>", args[0]);
    //     std::process::exit(1);
    // }
    // let hostname = &args[1];
    // let port = args[2].parse::<u16>().expect("Invalid port number");
    // let num_buffers = args[3].parse::<usize>().expect("Invalid number of buffers");
    // let buffer_size = args[4].parse::<usize>().expect("Invalid buffer size");
    // let gathering_interval = args[5].parse::<u64>().expect("Invalid gathering interval");

    // Create a TCP listener bound to a specific address and port
    let listener = TcpListener::bind("127.0.0.1:54321")?;
    println!("Server listening on port 54321...");

    // Accept incoming connections and handle them in separate threads
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // Spawn a new thread to handle each client connection
                thread::spawn(move || {
                    if let Err(err) = handle_client(&mut stream) {
                        eprintln!("Error handling client: {}", err);
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_client(stream: &mut TcpStream) -> std::io::Result<()> {
    loop {
    // Generate data to write into the socket
    let data = generate_data(123, 113)?;

    // Write data into the socket
    stream.write_all(&data)?;
    //std::thread::sleep(std::time::Duration::from_millis(1));
    }

    Ok(())
}

fn generate_data(id: i64, num_tuples: i64) -> std::io::Result<Vec<u8>> {
        let mut tuple_data = vec![];
      for i in 0..num_tuples {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap()
            .as_secs(); // Get current timestamp in seconds

        let id_bytes = id.to_le_bytes();
        let sequence_bytes = i.to_le_bytes();
        let timestamp_bytes = timestamp.to_le_bytes();

        // Concatenate all the bytes
        tuple_data.extend_from_slice(&id_bytes);
        tuple_data.extend_from_slice(&sequence_bytes);
        tuple_data.extend_from_slice(&timestamp_bytes);
      }

    Ok(tuple_data)
}
