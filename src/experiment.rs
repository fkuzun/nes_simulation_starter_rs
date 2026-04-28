use std::collections::HashMap;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{fs, time};

use itertools::Itertools;
use nes_tools::query::SubmitQueryResponse;
use nes_tools::topology::{ExecuteQueryRequest, PlacementStrategyType};
use regex::Regex;
use relative_path::RelativePathBuf;

use crate::config::{
    ExperimentType, FixedTopology, InputConfig, InputMobilityconfig, LogLevel,
    MobilityInputConfigList, SimulatedReconnects, SimulationConfig, SourceInputMethod,
};
use crate::nes_types::{
    CoordinatorConfiguration, FieldType, FixedWorkerConfig, LogicalSource, LogicalSourceField,
    MobileWorkerConfig, OptimizerConfiguration, PhysicalSource, PhysicalSourceConfiguration,
    PhysicalSourceType, YamlWritable,
};
use crate::query::parse_source_groups;
use crate::rest_node_relocation::{TopologyUpdate, TopologyUpdateList};
use crate::util::{
    add_edges_from_list, create_folder_with_timestamp, get_expected_join_output_count,
    wait_for_coordinator, wait_for_topology,
};

#[allow(dead_code)]
fn get_available_port(mut range: std::ops::Range<u16>) -> Option<u16> {
    range.find(|port| port_is_available(*port))
}

#[allow(dead_code)]
fn port_is_available(port: u16) -> bool {
    match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(_) => true,
        Err(_) => false,
    }
}

#[allow(dead_code)]
pub struct ExperimentSetup {
    output_config_directory: PathBuf,
    output_source_input_directory: PathBuf,
    output_trajectory_directory: PathBuf,
    sink_output_path: PathBuf,
    pub experiment_output_path: PathBuf,
    pub generated_folder: PathBuf,
    fixed_config_paths: Vec<PathBuf>,
    mobile_config_paths: Vec<PathBuf>,
    output_coordinator_config_path: PathBuf,
    coordinator_process: Option<Child>,
    pub mobile_worker_processes: Vec<Child>,
    pub fixed_worker_processes: Vec<Child>,
    edges: Vec<(u64, u64)>,
    pub input_config: InputConfig,
    pub total_number_of_tuples_to_emit: u64,
    pub num_buffers: u128,
    pub simulated_reconnects: SimulatedReconnects,
}

impl ExperimentSetup {
    pub fn start(
        &mut self,
        executable_paths: &crate::config::NesExecutablePaths,
        shutdown_triggered: Arc<AtomicBool>,
        log_level: &LogLevel,
    ) -> Result<(), Box<dyn Error>> {
        let _ = self.kill_processes();
        self.fixed_worker_processes = vec![];
        self.mobile_worker_processes = vec![];

        let rest_port = 8081;

        self.start_coordinator(
            &executable_paths.coordinator_path,
            Arc::clone(&shutdown_triggered),
            rest_port,
            &log_level,
        )?;

        wait_for_topology(Some(1), Arc::clone(&shutdown_triggered), rest_port)?;

        println!("starting fixed workers");
        self.start_fixed_workers(
            &executable_paths.worker_path,
            Arc::clone(&shutdown_triggered),
            &log_level,
        )?;

        println!("wait for fixed workers");
        wait_for_topology(
            Some(self.fixed_worker_processes.len() + 1),
            Arc::clone(&shutdown_triggered),
            rest_port,
        )?;

        println!("adding fixed edges");
        self.add_edges(rest_port)?;

        println!("starting mobile workers");
        self.start_mobile(
            &executable_paths.worker_path,
            Arc::clone(&shutdown_triggered),
            &log_level,
        )?;

        println!("waiting for mobile workers to be online");
        sleep(Duration::from_secs(60));
        wait_for_topology(
            Some(self.fixed_worker_processes.len() + self.mobile_worker_processes.len() + 1),
            Arc::clone(&shutdown_triggered),
            rest_port,
        )?;
        println!("mobile workers are online");

        Ok(())
    }

    pub fn submit_queries(
        output_port: u16,
        query_strings: Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        for query_string in query_strings {
            Self::submit_query(output_port, query_string)?;
        }
        Ok(())
    }

    pub fn submit_query(output_port: u16, query_string: String) -> Result<(), Box<dyn Error>> {
        let execute_query_request = ExecuteQueryRequest {
            user_query: query_string.replace("{OUTPUT}", &output_port.to_string()),
            placement: PlacementStrategyType::BottomUp,
        };
        let client = reqwest::blocking::Client::new();
        let result = client
            .post("http://127.0.0.1:8081/v1/nes/query/execute-query")
            .json(&execute_query_request)
            .send()?;
        let reply: SubmitQueryResponse = result.json()?;
        if reply.queryId == 0 {
            return Err("Could not submit query, received invalid query id 0".into());
        };
        Ok(())
    }

    fn add_edges(&self, rest_port: u16) -> Result<(), Box<dyn Error>> {
        let edges = &self.edges;
        add_edges_from_list(&rest_port, edges)
    }

    pub fn kill_processes(&mut self) -> Result<(), Box<dyn Error>> {
        for mobile_worker in &mut self.mobile_worker_processes {
            println!("killing mobile worker");
            mobile_worker.kill().expect("could not kill worker");
        }
        for fixedWorker in &mut self.fixed_worker_processes {
            println!("killing fixed worker");
            fixedWorker.kill().expect("could not kill worker");
        }
        match self.coordinator_process.take() {
            None => {
                println!("coordinator process not found")
            }
            Some(mut p) => p.kill()?,
        }
        Ok(())
    }

    fn start_fixed_workers(
        &mut self,
        worker_path: &Path,
        shutdown_triggered: Arc<AtomicBool>,
        log_level: &LogLevel,
    ) -> Result<(), Box<dyn Error>> {
        self.fixed_config_paths.sort();
        for path in &self.fixed_config_paths {
            println!("{:?}", path);
            sleep(Duration::from_millis(200));
            if shutdown_triggered.load(Ordering::SeqCst) {
                return Err(String::from("Shutdown triggered").into());
            }
            let process = Command::new(worker_path)
                .arg(format!("--configPath={}", path.display()))
                .arg(format!(
                    "--logLevel={}",
                    &serde_json::to_string(log_level).unwrap().trim_matches('\"')
                ))
                .spawn()?;

            self.fixed_worker_processes.push(process);
        }
        Ok(())
    }

    fn start_mobile(
        &mut self,
        worker_path: &Path,
        shutdown_triggered: Arc<AtomicBool>,
        log_level: &LogLevel,
    ) -> Result<(), Box<dyn Error>> {
        self.mobile_config_paths.sort();
        for path in &self.mobile_config_paths {
            println!("{:?}", path);
            sleep(Duration::from_millis(200));
            if shutdown_triggered.load(Ordering::SeqCst) {
                return Err(String::from("Shutdown triggered").into());
            }
            let process = Command::new(worker_path)
                .arg(format!("--configPath={}", path.display()))
                .arg(format!(
                    "--logLevel={}",
                    &serde_json::to_string(log_level).unwrap().trim_matches('\"')
                ))
                .spawn()?;
            self.mobile_worker_processes.push(process);
        }
        Ok(())
    }

    fn start_coordinator(
        &mut self,
        coordinator_path: &Path,
        shutdown_triggered: Arc<AtomicBool>,
        _rest_port: u16,
        log_level: &LogLevel,
    ) -> Result<(), Box<dyn Error>> {
        self.coordinator_process = Some(
            Command::new(&coordinator_path)
                .arg("--restServerCorsAllowedOrigin=*")
                .arg(format!(
                    "--configPath={}",
                    self.output_coordinator_config_path.display()
                ))
                .arg(format!(
                    "--logLevel={}",
                    &serde_json::to_string(log_level).unwrap().trim_matches('\"')
                ))
                .spawn()?,
        );

        std::thread::sleep(time::Duration::from_secs(5));
        wait_for_coordinator(Arc::clone(&shutdown_triggered))?;
        std::thread::sleep(time::Duration::from_secs(1));
        Ok(())
    }
}

/// Creates the complete experiment directory structure and all NES configuration files.
impl InputConfig {
    #[allow(unused_assignments)]
    fn generate_output_config(
        &mut self,
        generated_folder: &Path,
        experiment_type: ExperimentType,
    ) -> Result<ExperimentSetup, Box<dyn Error>> {
        println!("generating output config");
        let output_config_directory = generated_folder.join("config");
        fs::create_dir_all(&output_config_directory).expect("Failed to create folder");
        let output_source_input_directory = output_config_directory.join("source_input");
        fs::create_dir_all(&output_source_input_directory).expect("Failed to create folder");
        let output_trajectory_directory = output_config_directory.join("trajectory");
        fs::create_dir_all(&output_trajectory_directory).expect("Failed to create folder");
        let output_worker_config_directory = output_config_directory.join("worker_config");
        fs::create_dir_all(&output_worker_config_directory).expect("Failed to create folder");
        let output_coordinator_config_path =
            output_config_directory.join("coordinator_config.yaml");
        let output_topology_path =
            generated_folder.join(self.paths.get_fixed_topology_nodes_path_relative());
        let sink_output_path = generated_folder.join("replace_me.csv");
        let experiment_output_path = generated_folder.join("out");
        let mut logicalSources = vec![];

        println!("generating logical sources");
        let place_default_sources_on_node_ids =
            parse_source_groups(&self.parameters.place_default_sources_on_node_ids_path);

        let names = if !experiment_type.is_stateful() {
            place_default_sources_on_node_ids
                .into_values()
                .flatten()
                .unique()
                .collect_vec()
        } else {
            let mut names = vec![];

            let mut source_count_map = HashMap::<String, u64>::new();

            for v in place_default_sources_on_node_ids.into_values().flatten() {
                println!("Processing node: {}", v);
                let source_count = source_count_map.entry(v.clone()).or_insert(0);
                *source_count += 1;
                println!("Current count for {}: {}", v, source_count);
                names.push(format!("{}s{}", v, source_count));
            }
            println!("map: {:#?}", source_count_map);
            names
        };
        let source_fields = |include_join_id: bool| -> Vec<LogicalSourceField> {
            let mut fields = vec![LogicalSourceField {
                name: "id".to_string(),
                Type: FieldType::UINT64,
            }];
            if include_join_id {
                fields.push(LogicalSourceField {
                    name: "join_id".to_string(),
                    Type: FieldType::UINT64,
                });
            }
            fields.extend([
                LogicalSourceField {
                    name: "value".to_string(),
                    Type: FieldType::UINT64,
                },
                LogicalSourceField {
                    name: "event_timestamp".to_string(),
                    Type: FieldType::UINT64,
                },
                LogicalSourceField {
                    name: "processing_timestamp".to_string(),
                    Type: FieldType::UINT64,
                },
                LogicalSourceField {
                    name: "output_timestamp".to_string(),
                    Type: FieldType::UINT64,
                },
            ]);
            fields
        };
        for n in &names {
            println!("name: {}", n);
        }
        for name in names {
            logicalSources.push(LogicalSource {
                logicalSourceName: name.to_string(),
                fields: source_fields(experiment_type.is_stateful()),
            });
        }

        println!("register fake_migration_source");
        logicalSources.push(LogicalSource {
            logicalSourceName: "fake_migration_source".to_owned(),
            fields: source_fields(experiment_type.is_stateful()),
        });

        println!("generating coordinator config");
        let coordinator_config = CoordinatorConfiguration {
            enableProactiveDeployment: self.parameters.enable_proactive_deployment,
            logicalSources,
            logLevel: crate::config::LogLevel::LOG_ERROR,
            optimizer: OptimizerConfiguration {
                enableIncrementalPlacement: self.parameters.enable_query_reconfiguration,
                placementAmendmentThreadCount: self.parameters.placementAmendmentThreadCount,
            },
        };
        coordinator_config.write_to_file(&output_coordinator_config_path)?;

        println!(
            "reading fixed topology: {}",
            self.paths.get_fixed_topology_nodes_path().to_str().unwrap()
        );
        let json_string = std::fs::read_to_string(&self.paths.get_fixed_topology_nodes_path())?;
        let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;
        fs::write(&output_topology_path, json_string)?;

        let numberOfTuplesToProducePerBuffer = match self.default_source_input.source_input_method {
            SourceInputMethod::CSV => self.default_source_input.tuples_per_buffer.try_into()?,
            SourceInputMethod::TCP => 0,
        };

        println!("generating fixed worker configs");
        let mut _next_free_port = 5000;
        let mut fixed_config_paths = vec![];
        let num_buffers = self.get_data_production_time().as_millis()
            / self.default_source_input.gathering_interval.as_millis();
        let mut total_number_of_tuples_to_emit = 0;
        let mut max_fixed_id = 0;

        // Parse the source groups file once and pass the map to get_physical_sources_for_node
        let source_groups =
            parse_source_groups(&self.parameters.place_default_sources_on_node_ids_path);

        for (input_id, location) in &topology.nodes {
            if input_id > &max_fixed_id {
                max_fixed_id = *input_id;
            }
            let (physical_sources, number_of_slots) = self.get_physical_sources_for_node(
                numberOfTuplesToProducePerBuffer,
                num_buffers,
                &mut total_number_of_tuples_to_emit,
                *input_id + 1,
                &source_groups,
                experiment_type,
            );
            let worker_config = FixedWorkerConfig {
                rpcPort: None,
                dataPort: None,
                numberOfSlots: number_of_slots.unwrap_or(*topology.slots.get(input_id).unwrap()),
                nodeSpatialType: "FIXED_LOCATION".to_string(),
                fieldNodeLocationCoordinates: format!("{}, {}", location[0], location[1]),
                workerId: *input_id + 1,
                physicalSources: physical_sources,
                logLevel: crate::config::LogLevel::LOG_ERROR,
                numWorkerThreads: self.parameters.num_worker_threads,
                enableIncrementalPlacement: self.parameters.enable_query_reconfiguration,
            };
            let yaml_path =
                output_worker_config_directory.join(format!("fixed_worker{}.yaml", input_id));
            worker_config.write_to_file(&yaml_path)?;
            _next_free_port += 2;
            fixed_config_paths.push(yaml_path);
        }

        println!("creating mobility config from");
        let mut mobile_config_paths = vec![];
        let mobility_input_config_path_option = &self.paths.get_mobility_config_list_path();

        let (mut input_id, mobility_input_config, simulated_reconnects) =
            if let Some(path) = mobility_input_config_path_option {
                println!("trying to create mobility input config from simulated reconnects file");
                let json_string = std::fs::read_to_string(&path)?;
                let simulated_reconnects: SimulatedReconnects =
                    serde_json::from_str(json_string.as_str())?;
                let mobility_input_config = simulated_reconnects.get_mobility_input_config_list();
                (
                    max_fixed_id + 1,
                    mobility_input_config,
                    simulated_reconnects,
                )
            } else {
                panic!("No path set for mobility input config")
            };

        let mut generated_mobility_configs = vec![];
        let central_topology_update_list = TopologyUpdateList::new();

        println!("generating mobile worker configs");
        for worker_mobility_input_config in mobility_input_config.worker_mobility_configs {
            let generated_mobility_config = InputMobilityconfig {
                mobility_base_path: Some(output_trajectory_directory.clone()),
                locationProviderConfig: RelativePathBuf::from_path("invalid").unwrap(),
                locationProviderType: "BASE".to_owned(),
                reconnectPredictorType: worker_mobility_input_config.reconnectPredictorType,
                precalcReconnectPath: RelativePathBuf::from_path("invalid").unwrap(),
            };
            generated_mobility_configs.push(generated_mobility_config.clone());

            let (physical_sources, number_of_slots) = self.get_physical_sources_for_node(
                numberOfTuplesToProducePerBuffer,
                num_buffers,
                &mut total_number_of_tuples_to_emit,
                input_id + 1,
                &source_groups,
                experiment_type,
            );

            let worker_config = MobileWorkerConfig {
                fieldNodeLocationCoordinates: "0,0".into(),
                rpcPort: None,
                dataPort: None,
                workerId: input_id + 1,
                numberOfSlots: number_of_slots.unwrap_or(0),
                nodeSpatialType: "MOBILE_NODE".to_owned(),
                mobility: generated_mobility_config.to_mobility_config(),
                physicalSources: physical_sources,
                logLevel: crate::config::LogLevel::LOG_ERROR,
                numWorkerThreads: self.parameters.num_worker_threads,
                enableIncrementalPlacement: self.parameters.enable_query_reconfiguration,
            };
            let yaml_path =
                output_worker_config_directory.join(format!("mobile_worker{}.yaml", input_id));
            worker_config.write_to_file(&yaml_path)?;
            mobile_config_paths.push(yaml_path);
            input_id += 1;
            _next_free_port += 2;
            let _num_tuples =
                num_buffers as u64 * self.default_source_input.tuples_per_buffer as u64;
        }

        let cvec: Vec<TopologyUpdate> = central_topology_update_list.into();
        let reconnect_json = serde_json::to_string_pretty(&cvec).unwrap();
        println!("{}", reconnect_json);

        let output_central_reconnect_path =
            output_trajectory_directory.join("central_reconnects.json");
        fs::write(&output_central_reconnect_path, reconnect_json)
            .expect("Could not write central reconnects");
        let list_of_generated_mobility_configs = MobilityInputConfigList {
            worker_mobility_configs: generated_mobility_configs,
            central_topology_update_list_path: Some(output_central_reconnect_path.clone()),
        };
        list_of_generated_mobility_configs
            .write_to_file(&output_trajectory_directory.join("mobility_configs.toml"));

        let mut edges = vec![];
        for (parent, children) in topology.children {
            for child in children {
                edges.push((parent, child))
            }
        }

        // For join queries, each source pair produces one output per matched input,
        // so expected output = half total input.
        let total_number_of_tuples_to_emit = if experiment_type.is_stateful() {
            total_number_of_tuples_to_emit / 2
        } else {
            total_number_of_tuples_to_emit
        };
        Ok(ExperimentSetup {
            output_config_directory,
            output_source_input_directory,
            output_trajectory_directory,
            sink_output_path,
            experiment_output_path,
            fixed_config_paths,
            mobile_config_paths,
            output_coordinator_config_path,
            coordinator_process: None,
            mobile_worker_processes: vec![],
            fixed_worker_processes: vec![],
            edges,
            total_number_of_tuples_to_emit,
            input_config: self.clone(),
            num_buffers,
            generated_folder: generated_folder.to_path_buf(),
            simulated_reconnects,
        })
    }

    fn get_physical_sources_for_node(
        &mut self,
        numberOfTuplesToProducePerBuffer: u64,
        num_buffers: u128,
        total_number_of_tuples_to_ingest: &mut u64,
        input_id: u64,
        place_default_sources_on_node_ids: &HashMap<String, Vec<String>>,
        experiment_type: ExperimentType,
    ) -> (Vec<PhysicalSource>, Option<u16>) {
        let (physical_sources, number_of_slots) = if let Some((_, logical_source_names)) =
            place_default_sources_on_node_ids.get_key_value(&input_id.to_string())
        {
            let num_tuples =
                num_buffers as u64 * self.default_source_input.tuples_per_buffer as u64;
            let mut sources = vec![];

            for (index, logical_source_name) in logical_source_names.iter().enumerate() {
                let source_count = self
                    .source_count_map
                    .entry(logical_source_name.clone())
                    .or_insert(0);
                *source_count += 1;

                let num_tuples = if experiment_type.is_stateful() {
                    get_expected_join_output_count(
                        num_tuples,
                        self.parameters.window_size,
                        self.parameters.join_match_interval,
                    )
                } else {
                    num_tuples
                };

                println!(
                    "Adding source: {}, (add {} to {})",
                    logical_source_name, num_tuples, *total_number_of_tuples_to_ingest
                );
                *total_number_of_tuples_to_ingest += num_tuples;
                let logical_source_name = if experiment_type.is_stateful() {
                    format!("{}s{}", logical_source_name, source_count)
                } else {
                    logical_source_name.clone()
                };
                println!("{}", logical_source_name);
                sources.push(PhysicalSource {
                    logicalSourceName: logical_source_name,
                    physicalSourceName: format!("physical_{}", index).to_owned(),
                    Type: PhysicalSourceType::CSV_SOURCE,
                    configuration: PhysicalSourceConfiguration {
                        filePath: self.parameters.source_input_server_port.to_string(),
                        skipHeader: false,
                        sourceGatheringInterval: time::Duration::from_millis(0),
                        numberOfTuplesToProducePerBuffer,
                    },
                });
            }

            let source_count = sources.len() as u16;
            (sources, Some(source_count))
        } else {
            (vec![], None)
        };
        (physical_sources, number_of_slots)
    }
}

impl SimulationConfig {
    #[allow(unused_assignments)]
    pub fn generate_retrials(
        &self,
        number_of_runs: u64,
    ) -> Result<Vec<(String, InputConfig, Vec<u64>)>, Box<dyn Error>> {
        let mut setups = vec![];
        let re = Regex::new(r"out_run:(\d+)\.csvtuple_count\.csv").unwrap();
        for entry in fs::read_dir(self.run_for_retrial_path.as_ref().unwrap())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                println!("Checking directory");
                let mut tuple_count_output = None;
                let mut runs_to_repeat = vec![];
                let mut succesful_runs = vec![];
                for entry in fs::read_dir(&path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = &path.file_name() {
                            if let Some(captures) = re.captures(name.to_str().unwrap()) {
                                let content = fs::read_to_string(&path)?;
                                println!("{}", content);
                                let mut parts = content.trim().split(',');

                                let a: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap();
                                let b: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap();
                                let c: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap();
                                tuple_count_output = Some((a, b, c));
                                match tuple_count_output {
                                    None => {}
                                    Some((_, actual, expected)) => {
                                        if actual == expected {
                                            succesful_runs.push(captures[1].parse().unwrap());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                for run in 0..number_of_runs {
                    if !succesful_runs.contains(&run) {
                        runs_to_repeat.push(run);
                    }
                }

                if !runs_to_repeat.is_empty() {
                    let config_path = path.join("input_config_copy.toml");

                    println!("Adding config");
                    let input_config =
                        read_to_string(&config_path).expect("Could not read config file");
                    println!("{:?}", input_config);
                    let mut input_config: InputConfig =
                        toml::from_str(&*input_config).expect("could not parse config file");
                    input_config.paths.set_base_path(path);
                    setups.push((
                        entry.file_name().to_str().unwrap().to_string(),
                        input_config,
                        runs_to_repeat,
                    ));
                }
            }
        }
        Ok(setups)
    }

    pub fn generate_experiment_configs(
        &self,
        number_of_runs: u64,
    ) -> Result<Vec<(ExperimentSetup, Vec<u64>)>, Box<dyn Error>> {
        let (generated_main_folder, input_config_list): (PathBuf, Vec<(String, InputConfig, Vec<u64>)>) = if self.run_for_retrial_path.is_some() {
            println!("rerun");
            let folder_prefix = self
                .run_for_retrial_path
                .as_ref()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let generated_main_folder =
                create_folder_with_timestamp(self.output_directory.clone(), folder_prefix);
            (
                generated_main_folder,
                self.generate_retrials(number_of_runs)?,
            )
        } else {
            println!("generate new run");
            let generated_main_folder = self.create_generated_folder();
            let multi_simulation_config = self.read_multi_simulation_input_config();
            (
                generated_main_folder,
                multi_simulation_config.generate_input_configs(number_of_runs),
            )
        };
        println!("writing setups");
        let mut setups = vec![];
        for (short_name, mut input_config, runs) in input_config_list {
            let generated_folder = generated_main_folder.join(short_name);
            fs::create_dir_all(&generated_folder)?;
            let input_config_copy_path = generated_folder.join("input_config_copy.toml");
            let toml_string = toml::to_string(&input_config)?;
            let mut file = File::create(input_config_copy_path)?;
            println!("{}", &toml_string);
            file.write_all(toml_string.as_bytes())?;

            setups.push((
                input_config.generate_output_config(&generated_folder, self.experiment_type)?,
                runs,
            ));
        }
        Ok(setups)
    }
}
