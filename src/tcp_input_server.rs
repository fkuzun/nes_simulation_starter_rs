use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use std::env;
use std::sync::mpsc::{channel, Receiver, Sender};

fn main() -> std::io::Result<()> {
    // Parse environment variables
    let args: Vec<String> = env::args().collect();
    if args.len() < 7 {
        eprintln!("Usage: {} <hostname> <port> <num_buffers> <buffer_size> <gathering_interval> <join_match_interval (optional)>", args[0]);
        std::process::exit(1);
    }
    let hostname = &args[1][..];
    let port = args[2].parse::<u16>().expect("Invalid port number");
    let num_buffers = args[3].parse::<usize>().expect("Invalid number of buffers");
    let buffer_size = args[4].parse::<usize>().expect("Invalid buffer size");
    let gathering_interval = args[5].parse::<u64>().expect("Invalid gathering interval");
    let deadline = std::time::Duration::from_millis(args[6].parse::<u64>().expect("Invalid deadline"));
    let join_match_interval = args[7].parse::<u64>().unwrap_or(1);
    // let join_match_interval = 7;

    // Create a TCP listener bound to a specific address and port
    let listener = match TcpListener::bind((hostname, port)) {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Error creating TCP listener for input server on port {}: {}", port, e);
            std::process::exit(1);
        }
    };
     
    println!("Server listening on port {}...", port);
    println!("Deadline for ingestion: {:?}", deadline);

    let gather_interval = std::time::Duration::from_millis(gathering_interval);

    let mut id_count = 1;
    // Accept incoming connections and handle them in separate threads
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a new thread to handle each client connection
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, id_count, num_buffers, buffer_size, gather_interval, deadline, join_match_interval) {
                        eprintln!("Error handling client: {}", err);
                    }
                });
                id_count += 1;
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }

    Ok(())
}

fn handle_client(mut stream: TcpStream, id: u64, num_buffers: usize, buffer_size: usize, gathering_interval: std::time::Duration, deadline: std::time::Duration, join_match_interval: u64) -> std::io::Result<()> {
    println!("Starting tcp writer thread");
    
    let (sender, receiver): (Sender<Option<Vec<u8>>>, Receiver<Option<Vec<u8>>>) = channel();
    thread::spawn(move || {
        loop {
            if let Ok(received) = receiver.recv() {
                if let Some(data) = received {
                    // Write data into the socket
                    match  &stream.write_all(&data) {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("Error writing to socket: {}", err);
                            break;
                        }
                    }
                } else {
                    break
                }
            }
        }
    });
    
    //sleep if deadline is not reached
    println!("Deadline: {:?}", deadline);
    println!("Current time: {:?}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap());
    if deadline > SystemTime::now().duration_since(UNIX_EPOCH).unwrap() {
        let remaining = deadline - SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        println!("Deadline not reached, sleeping for {:?}", remaining);
        std::thread::sleep(remaining);
    }
    println!("Deadline reached, starting to write to socket");
    
    let mut next_emission_time = deadline;

    let mut sequence_nr = 0;
    for _buffer in 0..num_buffers {
        // Generate data to write into the socket
        let data = generate_data(id, &mut sequence_nr, buffer_size, join_match_interval)?;
        sender.send(Some(data)).unwrap();

        next_emission_time += gathering_interval;
        let curr_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        if next_emission_time > curr_time {
            let remaining = next_emission_time - curr_time;
            // println!("next emission not reached, sleeping for {:?}", remaining);
            std::thread::sleep(remaining);
        }
    }
    
    println!("All buffers done for id {}", id);

    std::thread::sleep(std::time::Duration::from_secs(10));

    Ok(())
}

fn generate_data(id: u64, sequence_nr: &mut u64, num_tuples: usize, join_match_interval: u64) -> std::io::Result<Vec<u8>> {
    let mut tuple_data = vec![];
    for _i in 0..num_tuples {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap()
            .as_nanos() as u64; // Get current timestamp in milliseconds

        let join_id = if *sequence_nr % join_match_interval == 0 {
            *sequence_nr * 1000
        } else {
            id
        };
        
        let id_bytes = id.to_le_bytes();
        let sequence_bytes = sequence_nr.to_le_bytes();
        let timestamp_bytes = timestamp.to_le_bytes();
        let join_id_bytes = join_id.to_le_bytes();

        // Concatenate all the bytes
        tuple_data.extend_from_slice(&id_bytes);
        tuple_data.extend_from_slice(&join_id_bytes); //TODO: add function to calculate join id
        tuple_data.extend_from_slice(&sequence_bytes);
        tuple_data.extend_from_slice(&timestamp_bytes);
        *sequence_nr += 1;
    }

    Ok(tuple_data)
}