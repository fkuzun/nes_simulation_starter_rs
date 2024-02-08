use std::error::Error;
use std::fmt::format;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::ops::Add;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::SeqCst;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Local};
use tokio::task;
use tokio::time::timeout;
use simulation_runner_lib::*;
use simulation_runner_lib::analyze::create_notebook;

fn main() -> Result<(), Box<dyn Error>> {
    //todo: read this from file
    let nes_root_dir = PathBuf::from("/home/x/uni/ba/standalone/nebulastream/build");
    let relative_worker_path = PathBuf::from("nes-worker/nesWorker");
    let relative_coordinator_path = PathBuf::from("nes-coordinator/nesCoordinator");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/single_source_half_second_reconnects_no_reconfig.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_moving_multiple_fixed_source_no_reconf.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_moving_multiple_fixed_source_no_reconnect_to_fied_source_no_reconf.toml");
    let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_moving_multiple_fixed_source_no_reconnect_to_field_source_iterate_reconf_tuples_interval_speedup.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_static_multiple_fixed_source_iterate.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_moving_multiple_fixed_source_no_reconnect_to_fied_source_reconf.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_moving_multiple_fixed_source_reconf.toml");
    //let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/input_data_config.toml");
    //let output_directory = PathBuf::from("/home/x/uni/ba/experiments");
    let output_directory = PathBuf::from("/media/x/28433579-5ade-44c1-a46c-c99efbf9b8c0/home/sqy/experiments");
    let abort_experiment_info_directory = PathBuf::from("/home/x/uni/ba/experiments");
    //let output_directory = PathBuf::from("/media/x/28433579-5ade-44c1-a46c-c99efbf9b8c0/home/sqy/experiments");


    let simulation_config = SimulationConfig {
        nes_root_dir,
        relative_worker_path,
        relative_coordinator_path,
        input_config_path,
        output_directory: output_directory.clone(),
    };
    let nes_executable_paths = NesExecutablePaths::new(&simulation_config);
    let coordinator_path = &nes_executable_paths.coordinator_path;
    let worker_path = &nes_executable_paths.worker_path;
    //let mut experiment = simulation_config.generate_experiment_configs().expect("Could not create experiment");
    let mut experiments = simulation_config.generate_experiment_configs().expect("Could not create experiment");

    let shutdown_triggered = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown_triggered);
    ctrlc::set_handler(move || {
        s.store(true, Ordering::SeqCst);
    }).expect("TODO: panic message");

    //create runtime
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let total_number_of_runs = experiments.len();
    'all_experiments: for (index, experiment) in experiments.iter_mut().enumerate() {
        let run_number = index + 1;
        if (shutdown_triggered.load(Ordering::SeqCst)) {
            experiment.kill_processes()?;
            break;
        }

        let experiment_duration = experiment.input_config.parameters.runtime.add(Duration::from_secs(10));

        println!("Starting experiment {} of {}", run_number, total_number_of_runs);
        println!("{}", toml::to_string(&experiment.input_config).unwrap());
        for attempt in 1..=1 {
            //start source input server
            let mut source_input_server_process = Command::new("/home/x/rustProjects/nes_simulation_starter_rs/target/release/tcp_input_server")
                //todo: do not hard code
                .arg("127.0.0.1")
                .arg(experiment.input_config.parameters.source_input_server_port.to_string())
                .arg(experiment.num_buffers.to_string())
                .arg(experiment.input_config.default_source_input.tuples_per_buffer.to_string())
                .arg(experiment.input_config.default_source_input.gathering_interval.as_millis().to_string())
                .spawn()?;

            let experiment_start = SystemTime::now();
            let now: DateTime<Local> = Local::now();
            println!("{}: Starting attempt {}", now, attempt);
            if let Ok(_) = experiment.start(&nes_executable_paths, Arc::clone(&shutdown_triggered)) {
                //let num_sources = experiment.input_config.parameters.place_default_source_on_fixed_node_ids.len() + experiment.
                let desired_line_count = experiment.total_number_of_tuples_to_ingest;
                // Bind the TCP listener to the specified address and port

                let mut line_count = AtomicUsize::new(0); // Counter for the lines written
                let line_count = Arc::new(line_count);
                //let  line_count = 0; // Counter for the lines written
                // Open the CSV file for writing
                let file_path = format!("{}_run:{}.csv", &experiment.experiment_output_path.to_str().unwrap(), attempt);
                let mut file = File::create(&file_path).unwrap();
                let mut file = Arc::new(file);
                let query_string = experiment.input_config.parameters.query_string.clone();

                // Use the runtime
                rt.block_on(async {
                    //todo: in the future we need to implement on the nes side parsing of ip and not only the port
                    //let listener = tokio::net::TcpListener::bind("127.0.0.1:12345").await.unwrap();
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let listener_port = listener.local_addr().unwrap().port();
                    println!("Listening for output tuples on port {}", listener_port);
                    let deployed = task::spawn_blocking(move || {ExperimentSetup::submit_query(listener_port, query_string).is_ok()}).await;
                    if let Ok(true) = deployed {
                        while !shutdown_triggered.load(Ordering::SeqCst) {
                            let timeout_duration = experiment_duration * 2;
                            let accept_result = timeout(timeout_duration, listener.accept()).await;

                            match accept_result {
                                Ok(Ok((stream, _))) => {
                                    // Handle the connection
                                    //tokio::spawn(handle_connection(stream, &mut line_count, desired_line_count, &mut file));
                                    let mut file_clone = file.clone();
                                    let mut line_count_clone = line_count.clone();
                                    let mut shutdown_triggered_clone = shutdown_triggered.clone();
                                    let mut experiment_start_clone = experiment_start.clone();
                                    let mut experiment_dureation_clone = experiment_duration.clone();
                                    let desired_line_count_copy = desired_line_count;
                                    tokio::spawn(async move {
                                        if let Err(e) = handle_connection(stream, line_count_clone, desired_line_count_copy, file_clone, shutdown_triggered_clone, experiment_start_clone, experiment_dureation_clone).await {
                                            eprintln!("Error handling connection: {}", e);
                                        }
                                    });
                                }
                                Ok(Err(e)) => {
                                    eprintln!("Error accepting connection: {}", e);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                            // Check if the maximum number of lines has been written
                            if line_count.load(SeqCst) >= desired_line_count as usize {
                                file.flush().expect("TODO: panic message");
                                break;
                            }
                            //check timeout
                            let current_time = SystemTime::now();
                            if let Ok(elapsed_time) = current_time.duration_since(experiment_start) {
                                if elapsed_time > experiment_duration + Duration::from_secs(30) {
                                    break;
                                }
                            }
                        }
                    }
                });
                if line_count.load(SeqCst) < desired_line_count as usize {
                    // Handle timeout here
                    let mut error_file = OpenOptions::new()
                        .append(true)
                        .create(true)
                        //.open(&experiment.generated_folder.join("error.txt"))
                        .open(&output_directory.join("error.csv"))
                        .unwrap();
                    //let error_string = format!("Aborted experiment in attempt {}", attempt);
                    let error_string = format!("{},{},{},{}\n", experiment.generated_folder.to_str().ok_or("Could not convert output directory to string")?, attempt, line_count.load(SeqCst), desired_line_count);
                    println!("Writing error string: {}", error_string);
                    error_file.write_all(error_string.as_bytes()).expect("Error while writing error message to file");
                }
                experiment.kill_processes()?;
                source_input_server_process.kill()?;
                let current_time = SystemTime::now();
                println!("Finished attempt for experiment {} of {}. attempt: {} running for {:?}", run_number, total_number_of_runs, attempt, current_time.duration_since(experiment_start));
                let mut tuple_count_string = format!("{},{},{}\n", attempt, line_count.load(SeqCst), desired_line_count);
                let tuple_count_path = file_path.clone().add("tuple_count.csv");
                let mut tuple_count_file = File::create(PathBuf::from(tuple_count_path)).unwrap();
                tuple_count_file.write_all(tuple_count_string.as_bytes()).expect("Error while writing tuple count to file");
                //create_notebook(&experiment.experiment_output_path, &PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/Analyze-new.ipynb"), &experiment.generated_folder.join("analysis.ipynb"))?;
                create_notebook(&PathBuf::from(&file_path), &PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/Analyze-new.ipynb"), &experiment.generated_folder.join(format!("analysis_run{}.ipynb", attempt)))?;
                if (shutdown_triggered.load(Ordering::SeqCst)) {
                    break;
                }
                // Check if the maximum number of lines has been written
                // if line_count.load(SeqCst) >= desired_line_count as usize {
                //     file.flush().expect("TODO: panic message");
                //     break;
                // }
            } else {
                //todo: move inside the experiment impl
                println!("Experiment failed to start");
            }
            experiment.kill_processes()?;
            source_input_server_process.kill()?;
        }
        if (shutdown_triggered.load(Ordering::SeqCst)) {
            break;
        }
    }
    Ok(())
}
