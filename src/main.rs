use std::env;
use std::error::Error;
use std::fmt::format;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::net::TcpListener;
use std::ops::Add;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::SeqCst;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use chrono::{DateTime, Local};
use execute::{Execute, shell};
use reqwest::Url;
use tokio::task;
use tokio::time::timeout;
use simulation_runner_lib::*;
use simulation_runner_lib::analyze::create_notebook;

fn main() -> Result<(), Box<dyn Error>> {
    // let mut command = shell("ps -ef | grep 'tcp_input_server' | grep -v grep | awk '{print $2}' | xargs -r kill -9");
    // command.stdout(Stdio::piped());
    // 
    // let output = command.execute_output().unwrap();
    // 
    // println!("{}", String::from_utf8(output.stdout).unwrap());
    // let mut command = shell("ps -ef | grep 'nesWorker' | grep -v grep | awk '{print $2}' | xargs -r kill -9");
    // command.stdout(Stdio::piped());
    // 
    // let output = command.execute_output().unwrap();
    // 
    // println!("{}", String::from_utf8(output.stdout).unwrap());
    // let mut command = shell("ps -ef | grep 'nesCoordinator' | grep -v grep | awk '{print $2}' | xargs -r kill -9");
    // command.stdout(Stdio::piped());

    // let output = command.execute_output().unwrap();

    // println!("{}", String::from_utf8(output.stdout).unwrap());

    //todo: read this from file

    let args: Vec<String> = env::args().collect();
    if args.len() < 5 || args.len() > 8 {
        eprintln!("Usage: {} <nes directory> <experiment input config path> <output directory> <tcp input server executable> <log level (optional)>, <experiment path for retrial (optional)>", args[0]);
        std::process::exit(1);
    }

    let nes_root_dir = PathBuf::from(&args[1]);
    let input_config_path = PathBuf::from(&args[2]);
    let output_directory = PathBuf::from(&args[3]);
    let input_server_path = PathBuf::from(&args[4]);
    let runs: u64 = args[5].parse().unwrap();
    let log_level: LogLevel = if args.len() >= 7 {
        println!("Log level: {}", &args[6]);
        serde_json::from_str::<LogLevel>(&format!("\"{}\"", &args[6])).unwrap_or_else(|e| {
            eprintln!("Could not parse log level: {}", e);
            LogLevel::LOG_ERROR
        })
    } else {
        LogLevel::LOG_ERROR
    };
    let run_for_retrial_path = if args.len() == 8 {
        Some(PathBuf::from(&args[7]))
    } else {
        None
    };

    // let nes_root_dir = PathBuf::from("/home/x/uni/ba/standalone/nebulastream/build");
    let relative_worker_path = PathBuf::from("nes-worker/nesWorker");
    let relative_coordinator_path = PathBuf::from("nes-coordinator/nesCoordinator");
    // let input_config_path = PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/one_moving_multiple_fixed_source_no_reconnect_to_field_source_iterate_reconf_tuples_interval_speedup.toml");
    // let output_directory = PathBuf::from("/media/x/28433579-5ade-44c1-a46c-c99efbf9b8c0/home/sqy/experiments");

    //check if retrial is complete
    //let run_for_retrial_path = Some(PathBuf::from("/media/x/28433579-5ade-44c1-a46c-c99efbf9b8c0/home/sqy/long_runs/merged/one_moving_multiple_fixed_source_no_reconnect_to_field_source_iterate_reconf_tuples_interval_speedup.toml2024-02-08_17-25-01"));

    //let runs = 7;

    let simulation_config = SimulationConfig {
        nes_root_dir,
        relative_worker_path,
        relative_coordinator_path,
        input_config_path,
        output_directory: output_directory.clone(),
        run_for_retrial_path,
    };
    let nes_executable_paths = NesExecutablePaths::new(&simulation_config);
    let coordinator_path = &nes_executable_paths.coordinator_path;
    let worker_path = &nes_executable_paths.worker_path;
    //let mut experiment = simulation_config.generate_experiment_configs().expect("Could not create experiment");

    // let mut experiments = if simulation_config.run_for_retrial_path.is_some() {
    //     simulation_config.generate_retrials().expect("Could not create experiment")
    // } else {
    //     simulation_config.generate_experiment_configs().expect("Could not create experiment")
    // };
    let mut experiments = simulation_config.generate_experiment_configs(runs).expect("Could not create experiment");

    let shutdown_triggered = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown_triggered);
    ctrlc::set_handler(move || {
        s.store(true, Ordering::SeqCst);
    }).expect("TODO: panic message");

    //create runtime
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let total_number_of_runs = experiments.len();
    'all_experiments: for (index, (experiment, runs)) in experiments.iter_mut().enumerate() {
        let run_number = index + 1;
        if (shutdown_triggered.load(Ordering::SeqCst)) {
            experiment.kill_processes()?;
            break;
        }

        // let experiment_duration = experiment.input_config.parameters.runtime.add(Duration::from_secs(10));
        let experiment_duration = experiment.input_config.get_total_time();

        println!("Starting experiment {} of {}", run_number, total_number_of_runs);
        println!("{}", toml::to_string(&experiment.input_config).unwrap());
        println!("performing runs {:?}", runs);
        for attempt in runs {
            let experiment_start = SystemTime::now();
            let ingestion_start = experiment_start.add(experiment.input_config.parameters.deployment_time_offset); 
            //start source input server
            println!("starting input server");
            let mut source_input_server_process = Command::new(&input_server_path)
                .arg("127.0.0.1")
                .arg(experiment.input_config.parameters.source_input_server_port.to_string())
                .arg(experiment.num_buffers.to_string())
                .arg(experiment.input_config.default_source_input.tuples_per_buffer.to_string())
                .arg(experiment.input_config.default_source_input.gathering_interval.as_millis().to_string())
                .arg(ingestion_start.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string())
                .spawn()?;
            println!("input server process id {}", source_input_server_process.id());


            let reconnect_start = ingestion_start.add(experiment.input_config.parameters.warmup);
            let now: DateTime<Local> = Local::now();
            println!("{}: Starting attempt {}", now, attempt);
            if let Ok(_) = experiment.start(&nes_executable_paths, Arc::clone(&shutdown_triggered), &log_level) {

                let rest_port = 8081;
                // create rest topology updater
                let rest_topology_updater = rest_node_relocation::REST_topology_updater::new(
                    experiment.central_topology_updates.clone(), 
                    reconnect_start.duration_since(SystemTime::UNIX_EPOCH).unwrap(), 
                    Duration::from_millis(10), Url::parse(&format!("http://127.0.0.1:{}/v1/nes/topology/update", &rest_port.to_string())).unwrap(),
                    experiment.initial_topology_update.as_ref().unwrap().clone());
                //todo: check if we need to join this thread
                print_topology(rest_port).unwrap();
                if let Ok(rest_topology_updater_thread) = rest_topology_updater.start() {
                print_topology(rest_port).unwrap();
                //println!("press any key to proceed");
                //let input: String = text_io::read!("{}\n");

                
                //let num_sources = experiment.input_config.parameters.place_default_source_on_fixed_node_ids.len() + experiment.
                let desired_line_count = experiment.total_number_of_tuples_to_ingest;
                // Bind the TCP listener to the specified address and port

                let mut line_count = AtomicUsize::new(0); // Counter for the lines written
                let line_count = Arc::new(line_count);
                //let  line_count = 0; // Counter for the lines written
                // Open the CSV file for writing
                let file_path = format!("{}_run:{}.csv", &experiment.experiment_output_path.to_str().unwrap(), attempt);
                let mut file = File::create(&file_path).unwrap();
                let mut file = Arc::new(Mutex::new(file));
                let mut completed_threads = AtomicUsize::new(0);
                let mut completed_threads = Arc::new(completed_threads);
                let query_string = experiment.input_config.parameters.query_strings.clone();
                std::thread::sleep(Duration::from_secs(10));

                // Use the runtime
                rt.block_on(async {
                    //todo: in the future we need to implement on the nes side parsing of ip and not only the port
                    //let listener = tokio::net::TcpListener::bind("127.0.0.1:12345").await.unwrap();
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let listener_port = listener.local_addr().unwrap().port();
                    println!("Listening for output tuples on port {}", listener_port);
                    //let deployed = task::spawn_blocking(move || {ExperimentSetup::submit_queries(listener_port, query_string).is_ok()}).await;
                    let deployed = task::spawn_blocking(move || {ExperimentSetup::submit_queries(listener_port, query_string).is_ok()});
                    //if let Ok(true) = deployed {
                    let mut num_spawned = 0;
                    {
                        while !shutdown_triggered.load(Ordering::SeqCst) {
                            //let timeout_duration = experiment_duration * 2;
                            // let timeout_duration = experiment_duration + experiment.input_config.parameters.cooldown_time + Duration::from_secs(40);
                            // let timeout_duration = experiment_duration + Duration::from_secs(60);
                            let timeout_duration = experiment_duration;
                            let accept_result = timeout(timeout_duration, listener.accept()).await;

                            match accept_result {
                                Ok(Ok((stream, _))) => {
                                    // Handle the connection
                                    //tokio::spawn(handle_connection(stream, &mut line_count, desired_line_count, &mut file));
                                    let mut file_clone = file.clone();
                                    let mut line_count_clone = line_count.clone();
                                    let mut shutdown_triggered_clone = shutdown_triggered.clone();
                                    let mut experiment_start_clone = experiment_start.clone();
                                    let mut timeout_duration_clone = timeout_duration.clone();
                                    let desired_line_count_copy = desired_line_count;
                                    let completed_threads_clone = completed_threads.clone();
                                    num_spawned += 1;
                                    tokio::spawn(async move {
                                        if let Err(e) = handle_connection(stream, line_count_clone, desired_line_count_copy, file_clone, shutdown_triggered_clone, experiment_start_clone, timeout_duration).await {
                                            eprintln!("Error handling connection: {}", e);
                                        }
                                        completed_threads_clone.fetch_add(1, Ordering::SeqCst);
                                    });
                                }
                                Ok(Err(e)) => {
                                    eprintln!("Error accepting connection: {}", e);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                            loop {
                                let current_time = SystemTime::now();
                                if let Ok(elapsed_time) = current_time.duration_since(experiment_start) {
                                    //if elapsed_time > timeout_duration + experiment.input_config.parameters.cooldown_time * 2 {
                                    if (completed_threads.load(SeqCst) == num_spawned && num_spawned > 0) || elapsed_time > experiment_duration * 10 || line_count.load(SeqCst) >= desired_line_count as usize || shutdown_triggered.load(Ordering::SeqCst) {
                                        println!("flushing file");
                                        file.lock().unwrap().flush().expect("TODO: panic message");
                                        break;
                                    }
                                    println!("timeout not reached, waiting for tupels to be written");
                                    println!("{} threads of {} completed", completed_threads.load(SeqCst), num_spawned);
                                    sleep(Duration::from_secs(5));
                                }
                            }
                        //check timeout
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
                //get_reconnect_list(8081).unwrap();
                experiment.kill_processes()?;
                source_input_server_process.kill()?;
                let current_time = SystemTime::now();
                println!("Finished attempt for experiment {} of {}. attempt: {} running for {:?}", run_number, total_number_of_runs, attempt, current_time.duration_since(experiment_start));
                let mut tuple_count_string = format!("{},{},{}\n", attempt, line_count.load(SeqCst), desired_line_count);
                let tuple_count_path = file_path.clone().add("tuple_count.csv");
                let mut tuple_count_file = File::create(PathBuf::from(tuple_count_path)).unwrap();
                tuple_count_file.write_all(tuple_count_string.as_bytes()).expect("Error while writing tuple count to file");
                let mut actual_reconnect_calls = rest_topology_updater_thread.join().unwrap();
                let reconnect_list_path = file_path.clone().add("reconnects.csv");
                let mut reconnect_list_file = File::create(PathBuf::from(reconnect_list_path)).unwrap();
                reconnect_list_file.write_all(actual_reconnect_calls.iter().map(|x| x.as_nanos().to_string()).collect::<Vec<String>>().join("\n").as_bytes()).expect("Error while writing reconnect list to file");
                //create_notebook(&experiment.experiment_output_path, &PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/Analyze-new.ipynb"), &experiment.generated_folder.join("analysis.ipynb"))?;
                //let notebook_path = &PathBuf::from("/home/x/uni/ba/experiments/nes_experiment_input/Analyze-new.ipynb");
                //if notebook_path.exists() {
                if let Some(notebook_path) = &simulation_config.get_analysis_script_path() {
                    create_notebook(&PathBuf::from(&file_path), &notebook_path, &experiment.generated_folder.join(format!("analysis_run{}.ipynb", attempt)))?;
                } else {
                    println!("No analysis script defined")
                }
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
                println!("Failed to add all mobile edges");
            }
        } else {
            //todo: move inside the experiment impl
            println!("Experiment failed to start");
        }
            //get_reconnect_list(8081).unwrap();
            experiment.kill_processes()?;
            source_input_server_process.kill()?;
        }
        experiment.kill_processes()?;
        if (shutdown_triggered.load(Ordering::SeqCst)) {
            break;
        }
        let wait_time = 30;
        println!("Finished experiment sleeping {} seconds before next experiment", wait_time);
        sleep(Duration::from_secs(wait_time));
    }
    Ok(())
}
