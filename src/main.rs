use std::error::Error;
use std::fmt::format;
use std::fs::File;
use std::io::Write;
use std::net::TcpListener;
use std::ops::Add;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use simulation_runner_lib::*;
use simulation_runner_lib::analyze::create_notebook;

fn main() -> Result<(), Box<dyn Error>> {
    //todo: read this from file
    let nes_root_dir = PathBuf::from("/home/x/uni/ba/standalone/nebulastream/build");
    let relative_worker_path = PathBuf::from("nes-worker/nesWorker");
    let relative_coordinator_path = PathBuf::from("nes-coordinator/nesCoordinator");
    let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/single_source_half_second_reconnects_reconfig.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/input_data_config.toml");
    let output_directory = PathBuf::from("/home/x/uni/ba/experiments");
    let simulation_config = SimulationConfig {
        nes_root_dir,
        relative_worker_path,
        relative_coordinator_path,
        input_config_path,
        output_directory
    };
    let nes_executable_paths = NesExecutablePaths::new(&simulation_config);
    let coordinator_path = &nes_executable_paths.coordinator_path;
    let worker_path = &nes_executable_paths.worker_path;
    let mut experiment = simulation_config.generate_experiment_configs().expect("Could not create experiment");

    let shutdown_triggered = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown_triggered);
    ctrlc::set_handler(move || {
        s.store(true, Ordering::SeqCst);
    }).expect("TODO: panic message");


    //start source input server
    let mut source_input_server_process = Command::new("/home/x/rustProjects/nes_simulation_starter_rs/target/release/tcp_input_server")
        //todo: do not hard code
        .arg("127.0.0.1")
        .arg(experiment.input_config.parameters.source_input_server_port.to_string())
        .arg(experiment.num_buffers.to_string())
        .arg(experiment.input_config.default_source_input.tuples_per_buffer.to_string())
        .arg(experiment.input_config.default_source_input.gathering_interval.as_millis().to_string())
        .spawn()?;



    let experiment_duration = experiment.input_config.parameters.runtime.add(Duration::from_secs(10));
    let experiment_start = SystemTime::now();
    if let Ok(_) = experiment.start(nes_executable_paths, Arc::clone(&shutdown_triggered)) {
        let desired_line_count = experiment.total_number_of_tuples_to_ingest;
        // Bind the TCP listener to the specified address and port
        let listener = TcpListener::bind("127.0.0.1:12345").unwrap();
        let mut line_count = 0; // Counter for the lines written
        // Open the CSV file for writing
        let mut file = File::create(&experiment.experiment_output_path).unwrap();

        // Accept incoming connections and handle them
        for stream in listener.incoming() {
            let stream = stream.unwrap();

            // Handle the connection
            if let Err(err) = handle_connection(stream, &mut line_count, desired_line_count, &mut file) {
                eprintln!("Error handling connection: {}", err);
            }

            // Check if the maximum number of lines has been written
            if line_count >= desired_line_count as usize {
                file.flush();
                break;
            }
        }        //shutdown_triggered.store(true, SeqCst);

        // while !shutdown_triggered.load(Ordering::SeqCst) {
        //
        //
        //
        //     //todo: this code is probably not needed anymore as we are now listening on the tcp connection
        //     // let current_time = SystemTime::now();
        //     // if let Ok(elapsed_time) = current_time.duration_since(experiment_start) {
        //     //     if elapsed_time > experiment_duration {
        //     //         break
        //     //     }
        //     // }
        //     // sleep(Duration::from_secs(1));
        // }
    }



    // loop {
    //     // Check the file periodically
    //     let line_count = count_lines_in_file(experiment.sink_output_path.as_path()).unwrap();
    //     println!("Current line count: {} of {}", line_count, desired_line_count);
    //
    //     // Check if the desired line count is reached
    //     if line_count >= desired_line_count.try_into().unwrap() {
    //         println!("Desired line count reached!");
    //         break;
    //     }
    //
    //     // Wait for some time before checking again
    //     sleep(Duration::from_secs(10)); // Wait for 10 seconds before checking again
    // }


    experiment.kill_processes()?;
    source_input_server_process.kill()?;
    create_notebook(&experiment.experiment_output_path, &PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/Analyze-new.ipynb"), &experiment.generated_folder.join("analysis.ipynb"))?;
    Ok(())
}
