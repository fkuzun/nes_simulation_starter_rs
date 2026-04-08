use chrono::{DateTime, Local};
use reqwest::Url;
use simulation_runner_lib::analyze::create_notebook;
use simulation_runner_lib::*;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Add;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use std::env;
use tokio::task;
use tokio::time::timeout;

fn main() -> Result<(), Box<dyn Error>> {
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

    let relative_worker_path = PathBuf::from("nes-worker/nesWorker");
    let relative_coordinator_path = PathBuf::from("nes-coordinator/nesCoordinator");

    let simulation_config = SimulationConfig {
        nes_root_dir,
        relative_worker_path,
        relative_coordinator_path,
        input_config_path,
        output_directory: output_directory.clone(),
        run_for_retrial_path,
        output_type: OutputType::AVRO,
    };
    let nes_executable_paths = NesExecutablePaths::new(&simulation_config);
    let _coordinator_path = &nes_executable_paths.coordinator_path;
    let _worker_path = &nes_executable_paths.worker_path;

    let mut experiments = simulation_config
        .generate_experiment_configs(runs)
        .expect("Could not create experiment");

    let shutdown_triggered = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&shutdown_triggered);
    ctrlc::set_handler(move || {
        s.store(true, Ordering::SeqCst);
    })
    .expect("TODO: panic message");

    let rt = tokio::runtime::Runtime::new().unwrap();

    let total_number_of_runs = experiments.len();
    for (index, (experiment, runs)) in experiments.iter_mut().enumerate() {
        let run_number = index + 1;
        if shutdown_triggered.load(Ordering::SeqCst) {
            experiment.kill_processes()?;
            break;
        }

        let experiment_duration = experiment.input_config.get_total_time();

        println!(
            "Starting experiment {} of {}",
            run_number, total_number_of_runs
        );
        println!("{}", toml::to_string(&experiment.input_config).unwrap());
        println!("performing runs {:?}", runs);
        for attempt in runs {
            if let Ok(_) = experiment.start(
                &nes_executable_paths,
                Arc::clone(&shutdown_triggered),
                &log_level,
            ) {
                let experiment_start = SystemTime::now();
                let ingestion_start =
                    experiment_start.add(experiment.input_config.parameters.deployment_time_offset);

                let reconnect_start =
                    ingestion_start.add(experiment.input_config.parameters.warmup);
                let start_date_time = DateTime::<Local>::from(experiment_start);
                let ingestion_start_date_time = DateTime::<Local>::from(ingestion_start);
                let reconnect_start_date_time = DateTime::<Local>::from(reconnect_start);
                println!("Experiment started at {}, begin ingesting tuples at {}, start reconnects at {}", start_date_time, ingestion_start_date_time, reconnect_start_date_time);
                let now: DateTime<Local> = Local::now();
                println!("{}: Starting attempt {}", now, attempt);
                println!("starting input server");
                let mut source_input_server_process = Command::new(&input_server_path)
                    .arg("127.0.0.1")
                    .arg(
                        experiment
                            .input_config
                            .parameters
                            .source_input_server_port
                            .to_string(),
                    )
                    .arg(experiment.num_buffers.to_string())
                    .arg(
                        experiment
                            .input_config
                            .default_source_input
                            .tuples_per_buffer
                            .to_string(),
                    )
                    .arg(
                        experiment
                            .input_config
                            .default_source_input
                            .gathering_interval
                            .as_millis()
                            .to_string(),
                    )
                    .arg(
                        ingestion_start
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .expect("Error while subtracting unix epock from ingestion start")
                            .as_millis()
                            .to_string(),
                    )
                    .arg(
                        experiment
                            .input_config
                            .parameters
                            .join_match_interval
                            .to_string(),
                    )
                    .spawn()?;
                println!(
                    "input server process id {}",
                    source_input_server_process.id()
                );

                let rest_port = 8081;
                let rest_topology_updater = rest_node_relocation::REST_topology_updater::new(
                    experiment.simulated_reconnects.topology_updates.clone(),
                    reconnect_start
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap(),
                    experiment.input_config.parameters.speedup_factor,
                    Url::parse(&format!(
                        "http://127.0.0.1:{}/v1/nes/topology/update",
                        &rest_port.to_string()
                    ))
                    .unwrap(),
                    experiment.simulated_reconnects.initial_parents.clone(),
                    experiment.input_config.parameters.reconnect_runtime,
                );
                print_topology(rest_port).unwrap();
                if let Ok(rest_topology_updater_thread) = rest_topology_updater.start() {
                    print_topology(rest_port).unwrap();

                    let desired_line_count = experiment.total_number_of_tuples_to_emit;

                    let line_count = AtomicUsize::new(0);
                    let line_count = Arc::new(line_count);

                    let file_path = format!(
                        "{}_run:{}.csv",
                        &experiment.experiment_output_path.to_str().unwrap(),
                        attempt
                    );
                    let file = File::create(&file_path).unwrap();

                    let file = Arc::new(Mutex::new(AvroOutputWriter::new(file)));

                    let completed_threads = AtomicUsize::new(0);
                    let completed_threads = Arc::new(completed_threads);
                    let query_string = experiment.input_config.parameters.query_string.clone();

                    let query_strings = build_query_strings(
                        &experiment.input_config.parameters.place_default_sources_on_node_ids_path,
                        &query_string,
                        experiment.input_config.parameters.window_size,
                        experiment.input_config.parameters.query_duplication_factor,
                    );

                    dbg!(&query_strings);
                    let desired_line_count_per_thread =
                        experiment.total_number_of_tuples_to_emit / query_strings.len() as u64;
                    std::thread::sleep(Duration::from_secs(10));

                    rt.block_on(async {
                        let listener =
                            tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let listener_port = listener.local_addr().unwrap().port();
                        println!("Listening for output tuples on port {}", listener_port);
                        let _deployed = task::spawn_blocking(move || {
                            ExperimentSetup::submit_queries(listener_port, query_strings).is_ok()
                        });
                        let mut num_spawned = 0;
                        {
                            while !shutdown_triggered.load(Ordering::SeqCst)
                                && ((completed_threads.load(SeqCst) < num_spawned)
                                    || num_spawned == 0)
                            {
                                let reconnect_timout = Duration::from_secs(20);
                                let timeout_duration = experiment_duration;
                                let accept_result =
                                    timeout(reconnect_timout, listener.accept()).await;

                                match accept_result {
                                    Ok(Ok((stream, _))) => {
                                        let file_clone = file.clone();
                                        let line_count_clone = line_count.clone();
                                        let shutdown_triggered_clone =
                                            shutdown_triggered.clone();
                                        let experiment_start_clone = experiment_start.clone();
                                        let _timeout_duration_clone = timeout_duration.clone();
                                        let desired_line_count_copy = desired_line_count;
                                        let completed_threads_clone = completed_threads.clone();
                                        num_spawned += 1;
                                        tokio::spawn(async move {
                                            if let Err(e) = handle_connection(
                                                stream,
                                                line_count_clone,
                                                desired_line_count_per_thread,
                                                desired_line_count_copy,
                                                file_clone.clone(),
                                                shutdown_triggered_clone,
                                                experiment_start_clone,
                                                timeout_duration,
                                                simulation_config.output_type,
                                            )
                                            .await
                                            {
                                                eprintln!(
                                                    "Error handling connection: {}",
                                                    e
                                                );
                                            }
                                            completed_threads_clone
                                                .fetch_add(1, Ordering::SeqCst);
                                        });
                                    }
                                    Ok(Err(e)) => {
                                        println!("Error accepting connection: {}", e);
                                    }
                                    Err(e) => {
                                        println!("Could not establish connection during this loop iteration ({}) proceeding with check if we can finish the experiment now", e);
                                    }
                                }
                                println!(
                                    "Completed threads: {}, Spawned threads {}",
                                    completed_threads.load(SeqCst),
                                    num_spawned
                                );
                            }
                            loop {
                                let current_time = SystemTime::now();
                                if let Ok(elapsed_time) =
                                    current_time.duration_since(experiment_start)
                                {
                                    if completed_threads.load(SeqCst) == num_spawned
                                        && num_spawned > 0
                                        || elapsed_time > experiment_duration * 10
                                        || line_count.load(SeqCst)
                                            >= desired_line_count as usize
                                        || shutdown_triggered.load(Ordering::SeqCst)
                                    {
                                        println!("flushing file");
                                        file.lock()
                                            .unwrap()
                                            .flush()
                                            .expect("TODO: panic message");
                                        break;
                                    }
                                    println!(
                                        "timeout not reached, waiting for tuples to be written"
                                    );
                                    println!(
                                        "{} threads of {} completed",
                                        completed_threads.load(SeqCst),
                                        num_spawned
                                    );
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                            }
                        }
                    });
                    if line_count.load(SeqCst) < desired_line_count as usize {
                        let mut error_file = OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&output_directory.join("error.csv"))
                            .unwrap();
                        let error_string = format!(
                            "{},{},{},{}\n",
                            experiment
                                .generated_folder
                                .to_str()
                                .ok_or("Could not convert output directory to string")?,
                            attempt,
                            line_count.load(SeqCst),
                            desired_line_count
                        );
                        println!("Writing error string: {}", error_string);
                        error_file
                            .write_all(error_string.as_bytes())
                            .expect("Error while writing error message to file");
                    }
                    experiment.kill_processes()?;
                    source_input_server_process.kill()?;
                    let current_time = SystemTime::now();
                    println!(
                        "Finished attempt for experiment {} of {}. attempt: {} running for {:?}",
                        run_number,
                        total_number_of_runs,
                        attempt,
                        current_time.duration_since(experiment_start)
                    );
                    let tuple_count_string = format!(
                        "{},{},{}\n",
                        attempt,
                        line_count.load(SeqCst),
                        desired_line_count
                    );
                    let tuple_count_path = file_path.clone().add("tuple_count.csv");
                    let mut tuple_count_file =
                        File::create(PathBuf::from(tuple_count_path)).unwrap();
                    tuple_count_file
                        .write_all(tuple_count_string.as_bytes())
                        .expect("Error while writing tuple count to file");
                    let actual_reconnect_calls = rest_topology_updater_thread.join().unwrap();
                    let reconnect_list_path = file_path.clone().add("reconnects.csv");
                    let mut reconnect_list_file =
                        File::create(PathBuf::from(reconnect_list_path)).unwrap();
                    reconnect_list_file
                        .write_all(
                            actual_reconnect_calls
                                .iter()
                                .map(|x| x.as_nanos().to_string())
                                .collect::<Vec<String>>()
                                .join("\n")
                                .as_bytes(),
                        )
                        .expect("Error while writing reconnect list to file");
                    if let Some(notebook_path) = &simulation_config.get_analysis_script_path() {
                        create_notebook(
                            &PathBuf::from(&file_path),
                            &notebook_path,
                            &experiment
                                .generated_folder
                                .join(format!("analysis_run{}.ipynb", attempt)),
                        )?;
                    } else {
                        println!("No analysis script defined")
                    }
                    if shutdown_triggered.load(Ordering::SeqCst) {
                        break;
                    }
                } else {
                    println!("Failed to add all mobile edges");
                }
                source_input_server_process.kill()?;
            } else {
                println!("Experiment failed to start");
            }

            experiment.kill_processes()?;
            let wait_time = 30;
            println!(
                "Finished run sleeping {} seconds before next run",
                wait_time
            );
            sleep(Duration::from_secs(wait_time));
        }

        experiment.kill_processes()?;
        if shutdown_triggered.load(Ordering::SeqCst) {
            break;
        }
        let wait_time = 30;
        println!(
            "Finished experiment sleeping {} seconds before next experiment",
            wait_time
        );
        sleep(Duration::from_secs(wait_time));
    }
    Ok(())
}
