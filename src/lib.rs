use std::collections::{hash_map, HashMap};
use std::error::Error;
use std::fmt::format;
use std::hash::Hasher;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::raw::{time_t, uid_t};
use std::process::{Child, Command, Stdio};
use std::{fs, io, sync, time};
use std::env::consts::OS;
use std::ffi::OsStr;
use std::fs::{File, read_to_string};
use std::net::TcpListener;
use std::ops::{Add, Range, Sub};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::SeqCst;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use sync::atomic;
use chrono::Local;
use nes_tools::launch::Launch;
use nes_tools::query::SubmitQueryResponse;
use nes_tools::topology::{AddEdgeReply, AddEdgeRequest, ExecuteQueryRequest, PlacementStrategyType};
use serde_with::serde_as;
use yaml_rust::{YamlEmitter, YamlLoader};
use crate::FieldType::UINT64;
use crate::WorkerConfigType::Fixed;
use serde_with::DurationMilliSeconds;
use serde_with::DurationNanoSeconds;
use serde_with::DurationSeconds;
use tokio::io::AsyncBufReadExt;
use regex::Regex;
use relative_path::RelativePathBuf;
use crate::rest_node_relocation::TopologyUpdate;

pub mod analyze;

pub mod rest_node_relocation;
mod MobileDeviceQuadrants;

const INPUT_FOLDER_SUB_PATH: &'static str = "nes_experiment_input";
const INPUT_CONFIG_NAME: &'static str = "input_data_config.toml";
//const PORT_RANGE: std::ops::Range<u16> = 10_000..20_000;
const PORT_RANGE: std::ops::Range<u16> = 7000..8000;


fn get_available_port(mut range: Range<u16>) -> Option<u16> {
    range.find(|port| port_is_available(*port))
}

fn port_is_available(port: u16) -> bool {
    match TcpListener::bind(("127.0.0.1", port)) {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub fn add_edges_from_list(rest_port: &u16, edges: &Vec<(u64, u64)>) -> Result<(), Box<dyn Error>> {
    let client = reqwest::blocking::Client::new();
    for (parent_id, child_id) in edges {
        if parent_id == &1 {
            continue;
        }
        let link_request = AddEdgeRequest {
            parent_id: *parent_id,
            child_id: *child_id,
        };
        let result = client.post(format!("http://127.0.0.1:{}/v1/nes/topology/addAsChild", &rest_port.to_string()))
            .json(&link_request).send()?;
        //println!("{}", result.text().unwrap());
        let reply: AddEdgeReply = result.json()?;
        if !reply.success {
            return Err("Could not add edge".into());
        }
        let link_request = AddEdgeRequest {
            parent_id: 1,
            child_id: *child_id,
        };
        let result = client.delete("http://127.0.0.1:8081/v1/nes/topology/removeAsChild")
            .json(&link_request).send()?;
        //assert!(result.json().unwrap());
        let reply: AddEdgeReply = result.json()?;
        if !reply.success {
            return Err("Could not add edge".into());
        }
    };
    Ok(())
}

//const START_OF_SOURCE_INPUT_SERVER_PORT_RANGE: u16 = 10_000;
//const START_OF_SINK_OUTPUT_SERVER_PORT_RANGE: u16 = 11_000;
//const START_OF_SINK_OUTPUT_SERVER_PORT_RANGE: u16 = 11_000;

//fn get_output_server_port()

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MultiSimulationInputConfig {
    pub enable_query_reconfiguration: Vec<bool>,
    pub enable_proactive_deployment: Vec<bool>,
    pub tuples_per_buffer: Vec<usize>,
    pub speedup_factor: Vec<f64>,
    placementAmendmentThreadCount: Vec<u16>,
    #[serde_as(as = "Vec<DurationMilliSeconds<u64>>")]
    pub gathering_interval: Vec<Duration>,
    pub default_config: InputConfig,
    pub analysis_script: Option<RelativePathBuf>,
}

impl MultiSimulationInputConfig {
    pub fn read_input_from_file(file_path: &Path) -> Result<Self, Box<dyn Error>> {
        let config: Self = toml::from_str(&*read_to_string(file_path)?)?;
        Ok(config)
    }

    pub fn get_reconfig_short_name(&self) -> String {
        String::from("reconf")
    }

    pub fn get_tuples_per_buffer_short_name(&self) -> String {
        String::from("tuplesPerBuffer")
    }

    pub fn get_proactive_short_name(&self) -> String {
        String::from("proactive")
    }

    pub fn get_gathering_interval_short_name(&self) -> String {
        String::from("gatheringInterval")
    }

    pub fn get_speedup_short_name(&self) -> String {
        String::from("speedup")
    }
    
    pub fn get_amnenment_threads_short_name(&self) -> String {
        String::from("amendmentThreads")
    }

    pub fn get_short_name_value_separator(&self) -> String {
        String::from(":")
    }

    pub fn get_short_name_to_short_name_separator(&self) -> String {
        String::from("_")
    }

    pub fn get_short_name_with_value(&self, short_name: &str, value: &str) -> String {
        format!("{}{}{}", short_name, self.get_short_name_value_separator(), value)
    }

    pub fn generate_input_configs(&self, number_of_runs: u64) -> Vec<(String, InputConfig, Vec<u64>)> {
        let mut configs = vec![];
        //let mut source_input_server_port = START_OF_SOURCE_INPUT_SERVER_PORT_RANGE;
        for &enable_query_reconfiguration in &self.enable_query_reconfiguration {
            for &enable_proactive_deployment in &self.enable_proactive_deployment {
                if !enable_query_reconfiguration && enable_proactive_deployment {
                    println!("skipping config with reconfiguration disabled and proactive deployment enabled");
                    continue;
                }
                for &tuples_per_buffer in &self.tuples_per_buffer {
                    for &gathering_interval in &self.gathering_interval {
                        for &speedup_factor in &self.speedup_factor {
                            for &placementAmendmentThreadCount in &self.placementAmendmentThreadCount {
                                let config = InputConfig {
                                    parameters: Parameters {
                                        enable_query_reconfiguration,
                                        enable_proactive_deployment,
                                        speedup_factor,
                                        placementAmendmentThreadCount,
                                        //source_input_server_port,
                                        ..self.default_config.parameters.clone()
                                    },
                                    default_source_input: DefaultSourceInput {
                                        tuples_per_buffer,
                                        gathering_interval,
                                        ..self.default_config.default_source_input.clone()
                                    },
                                    ..self.default_config.clone()
                                };

                                //source_input_server_port += 1;
                                let mut short_name = self.get_short_name_with_value(&self.get_reconfig_short_name(), &enable_query_reconfiguration.to_string());
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(&self.get_proactive_short_name(), &enable_proactive_deployment.to_string()));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(&self.get_tuples_per_buffer_short_name(), &tuples_per_buffer.to_string()));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(&self.get_gathering_interval_short_name(), &gathering_interval.as_millis().to_string()));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(&self.get_speedup_short_name(), &speedup_factor.to_string()));
                                short_name.push_str(&self.get_short_name_with_value(&self.get_amnenment_threads_short_name(), &placementAmendmentThreadCount.to_string()));
                                configs.push((short_name, config, (0..number_of_runs).collect()));
                            }
                        }
                    }
                }
            }
            // let config = InputConfig {
            //     parameters: Parameters {
            //         enable_query_reconfiguration,
            //         ..self.default_config.parameters.clone()
            //     },
            //     ..self.default_config.clone()
            // };
            // let short_name = self.get_short_name_with_value(&self.get_reconfig_short_name(), &enable_query_reconfiguration.to_string());
            // configs.push((short_name, config));
        };
        configs
    }
}

#[derive(Deserialize, Debug)]
pub struct SimulationConfig {
    pub nes_root_dir: PathBuf,
    pub relative_worker_path: PathBuf,
    pub relative_coordinator_path: PathBuf,
    pub output_directory: PathBuf,
    pub input_config_path: PathBuf,
    pub run_for_retrial_path: Option<PathBuf>,
}

impl SimulationConfig {
    //get the absolute path to the analysis script defined in the multisimulation config file
    pub fn get_analysis_script_path(&self) -> Option<PathBuf> {
        let multi_conf = MultiSimulationInputConfig::read_input_from_file(&self.input_config_path).expect("could not read multi simulation config file");
        if let Some(script_path) = &multi_conf.analysis_script {
            // let mut path = self.input_config_path.clone();
            // path.to_path_buf();
            let abs_path = script_path.to_path(self.input_config_path.parent().expect("could not get parent path of input config file"));
            if abs_path.exists() {
                Some(abs_path)
            } else {
                None
            }
        } else {
            None
        }
    }


    // fn get_input_folder_path(&self) -> PathBuf {
    //     let mut path = self.experiment_directory.clone();
    //     path.push(INPUT_FOLDER_SUB_PATH);
    //     path
    // }

    fn get_input_config_path(&self) -> PathBuf {
        // let mut path = self.experiment_directory.clone();
        // path.push(INPUT_FOLDER_SUB_PATH);
        // path.push(INPUT_CONFIG_NAME);
        // path
        self.input_config_path.clone()
    }

    fn read_input_config(&self) -> InputConfig {
        let input_config: InputConfig = toml::from_str(&*read_to_string(&self.get_input_config_path()).expect("Could not read config file")).expect("could not parse config file");
        input_config
    }

    fn read_multi_simulation_input_config(&self) -> MultiSimulationInputConfig {
        let file_path = self.get_input_config_path();
        let mut input_config: MultiSimulationInputConfig = toml::from_str(&read_to_string(&file_path).expect("Could not read config file")).expect("could not parse config file");
        input_config.default_config.paths.set_base_path(file_path.parent().expect("could not get parent path of input config file").to_owned());
        input_config
    }

    fn create_generated_folder(&self) -> PathBuf {
        //create_folder_with_timestamp(self.experiment_directory.clone(), "generated_experiment_")
        create_folder_with_timestamp(self.output_directory.clone(), self.input_config_path.file_name().unwrap().to_str().unwrap())
    }

    pub fn generate_retrials(&self) -> Result<Vec<(String, InputConfig, Vec<u64>)>, Box<dyn Error>> {

        //iterate over all subfolders in retrial path and check for tuple_count files
        let mut setups = vec![];
        let re = Regex::new(r"out_run:(\d+)\.csvtuple_count\.csv").unwrap();
        for entry in fs::read_dir(self.run_for_retrial_path.as_ref().unwrap())? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                println!("Checking directory");
                let mut tuple_count_output = None;
                let mut runs_to_repeat = vec![];
                for entry in fs::read_dir(&path)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = &path.file_name() {
                            if let Some(captures) = re.captures(name.to_str().unwrap()) {
                                //if name.to_str().unwrap().contains("tuple_count") {
                                let content = fs::read_to_string(&path)?;
                                println!("{}", content);
                                let mut parts = content.trim().split(',');

                                let a: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap();
                                let b: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap();
                                let c: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap();
                                tuple_count_output = Some((a, b, c));
                                if let Some((_, 0, _)) | None = tuple_count_output {
                                    runs_to_repeat.push(captures[1].parse().unwrap());
                                }
                            }
                        }
                    }
                }
                if !runs_to_repeat.is_empty() {
                    //if let Some((_, 0, _)) | None = tuple_count_output {
                    let config_path = path.join("input_config_copy.toml");

                    println!("Adding config");
                    let input_config = read_to_string(config_path).expect("Could not read config file");
                    println!("{:?}", input_config);
                    let input_config: InputConfig = toml::from_str(&*input_config).expect("could not parse config file");
                    setups.push((entry.file_name().to_str().unwrap().to_string(), input_config, runs_to_repeat));
                }
            }
        }
        // let input_config_list = multi_simulation_config.generate_input_configs();
        // let mut setups = vec![];
        // for (short_name, input_config) in input_config_list {
        //     let generated_folder = generated_main_folder.join(short_name);
        //     fs::create_dir_all(&generated_folder)?;
        //     let input_config_copy_path = generated_folder.join("input_config_copy.toml");
        //     let toml_string = toml::to_string(&input_config)?;
        //     let mut file = File::create(input_config_copy_path)?;
        //     file.write_all(toml_string.as_bytes())?;
        //
        //     setups.push(input_config.generate_output_config(&generated_folder)?);
        // }
        Ok(setups)
    }

    pub fn generate_experiment_configs(&self, number_of_runs: u64) -> Result<Vec<(ExperimentSetup, Vec<u64>)>, Box<dyn Error>> {
        let (generated_main_folder, input_config_list) = if self.run_for_retrial_path.is_some() {
            println!("rerun");
            let folder_prefix = self.run_for_retrial_path.as_ref().unwrap().file_name().unwrap().to_str().unwrap();
            let generated_main_folder = create_folder_with_timestamp(self.output_directory.clone(), folder_prefix);
            (generated_main_folder, self.generate_retrials()?)
        } else {
            println!("generate new run");
            let generated_main_folder = self.create_generated_folder();
            //let input_config = self.read_input_config();
            let multi_simulation_config = self.read_multi_simulation_input_config();
            (generated_main_folder, multi_simulation_config.generate_input_configs(number_of_runs))
        };
        //let input_config_list = multi_simulation_config.generate_input_configs();
        println!("writing setups");
        let mut setups = vec![];
        for (short_name, input_config, runs) in input_config_list {
            let generated_folder = generated_main_folder.join(short_name);
            fs::create_dir_all(&generated_folder)?;
            let input_config_copy_path = generated_folder.join("input_config_copy.toml");
            let toml_string = toml::to_string(&input_config)?;
            let mut file = File::create(input_config_copy_path)?;
            println!("{}", &toml_string);
            file.write_all(toml_string.as_bytes())?;

            setups.push((input_config.generate_output_config(&generated_folder)?, runs));
        }
        Ok(setups)
        // let input_config_copy_path = generated_folder.join("input_config_copy.toml");
        // let toml_string = toml::to_string(&input_config)?;
        // let mut file = File::create(input_config_copy_path)?;
        // file.write_all(toml_string.as_bytes())?;
        //
        // input_config.generate_output_config(&generated_folder)
    }
}


pub struct NesExecutablePaths {
    pub worker_path: PathBuf,
    pub coordinator_path: PathBuf,
}

impl NesExecutablePaths {
    pub fn new(config: &SimulationConfig) -> Self {
        let mut worker_path = config.nes_root_dir.clone();
        worker_path.push(&config.relative_worker_path);
        let mut coordinator_path = config.nes_root_dir.clone();
        coordinator_path.push(&config.relative_coordinator_path);
        Self {
            worker_path,
            coordinator_path,
        }
    }
}


#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Parameters {
    pub enable_query_reconfiguration: bool,
    pub enable_proactive_deployment: bool,
    pub speedup_factor: f64,
    //pub amount_of_reconnects: Option<u64>,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub deployment_time_offset: Duration,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub warmup: Duration,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub reconnect_runtime: Duration,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub cooldown_time: Duration,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub post_cooldown_time: Duration,
    pub reconnect_input_type: ReconnectPredictorType,
    pub source_input_server_port: u16,
    pub query_strings: Vec<String>,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub reconnect_start_offset: Duration,
    //#[serde_as(as = "HashMap<String, String>")]

    pub place_default_sources_on_node_ids: HashMap<String, Vec<String>>,
    pub logical_source_names: Vec<String>,
    pub num_worker_threads: u64,
    placementAmendmentThreadCount: u16,
}


#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DefaultSourceInput {
    pub tuples_per_buffer: usize,
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub gathering_interval: Duration,
    pub source_input_method: SourceInputMethod,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum SourceInputMethod {
    CSV,
    TCP,
}


use serde::Deserializer;
use std::fmt;
use crate::config::Paths;
use crate::MobileDeviceQuadrants::QuadrantConfig;

pub fn deserialize_relative_path<'de, D>(deserializer: D) -> Result<RelativePathBuf, D::Error>
    where
        D: Deserializer<'de>,
{
    // let s = String::deserialize(deserializer)?;
    // RelativePathBuf::from_str(&s).map_err(serde::de::Error::custom)
    let p = PathBuf::deserialize(deserializer)?;
    Ok(RelativePathBuf::from_path(p).expect("only relative paths are allowed in the config file"))
}

pub mod config {
    use std::path::PathBuf;
    use relative_path::RelativePathBuf;
    use serde::{Deserialize, Serialize};
    use crate::MobileDeviceQuadrants::QuadrantConfig;

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Paths {
        #[serde(skip)]
        base_path: Option<PathBuf>,
        #[serde(deserialize_with = "super::deserialize_relative_path")]
        fixed_topology_nodes: RelativePathBuf,
        //#[serde(deserialize_with = "super::deserialize_relative_path")]
        //mobile_trajectories_directory: RelativePathBuf,
        mobile_trajectories_directory: MobileTopologyInput,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub enum MobileTopologyInput {
        Quadrants(QuadrantConfig),
        #[serde(deserialize_with = "super::deserialize_relative_path")]
        TrajectoriesDir(RelativePathBuf),

    }

    impl Paths {
        pub fn get_fixed_topology_nodes_path(&self) -> PathBuf {
            self.fixed_topology_nodes.to_path(self.base_path.as_ref().expect("base path not set"))
        }
        pub fn get_quadrant_config(&self) -> Option<QuadrantConfig> {
            if let MobileTopologyInput::Quadrants(config) = &self.mobile_trajectories_directory {
                Some(config.clone())
            } else {
                None
            }
        }

        pub fn get_mobile_trajectories_directory(&self) -> Option<PathBuf> {
            if let MobileTopologyInput::TrajectoriesDir(dir) = &self.mobile_trajectories_directory {
                Some(dir.to_path(self.base_path.as_ref().expect("base path not set")))
            } else {
                println!("cannot get path for non directory input type because quadrant method is used");
                None
            }
        }

        pub fn get_mobility_config_list_path(&self) -> Option<PathBuf> {
            let mut option = self.get_mobile_trajectories_directory();
            if let Some(mut path) = option {
                path.push("mobility_config_list.toml");
                Some(path)
            } else {
                None
            }
        }

        pub fn set_base_path(&mut self, base_path: PathBuf) {
            self.base_path = Some(base_path);
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct InputConfig {
    pub parameters: Parameters,
    pub default_source_input: DefaultSourceInput,
    paths: Paths,
    //pub quadrant_config: Option<QuadrantConfig>,
}

pub struct ExperimentSetup {
    output_config_directory: PathBuf,
    output_source_input_directory: PathBuf,
    output_trajectory_directory: PathBuf,
    sink_output_path: PathBuf,
    pub experiment_output_path: PathBuf,
    pub generated_folder: PathBuf,
    //output_worker_config_directory: PathBuf,
    fixed_config_paths: Vec<PathBuf>,
    mobile_config_paths: Vec<PathBuf>,
    output_coordinator_config_path: PathBuf,
    coordinator_process: Option<Child>,
    pub mobile_worker_processes: Vec<Child>,
    pub fixed_worker_processes: Vec<Child>,
    edges: Vec<(u64, u64)>,
    pub input_config: InputConfig,
    pub total_number_of_tuples_to_ingest: u64,
    pub num_buffers: u128,
    pub central_topology_updates: Vec<TopologyUpdate>,
}

#[derive(Serialize, Deserialize)]
pub struct ReconnectList {
    pub timestamps: Vec<Vec<u64>>,
}


pub fn get_reconnect_list(rest_port: u16) -> Result<ReconnectList, Box<dyn Error>> {
    let client = reqwest::blocking::Client::new();
    let result = client.get(format!("http://localhost:{}/v1/nes/query/reconnects", &rest_port.to_string())).send()?;
    //println!("list: {:?}", &result.text().unwrap());
    let reply: Vec<Vec<u64>> = result.json()?;
    //println!("received reconnect list:");
    println!("list: {:?}", reply);
    // Ok(reply)
    Ok(
        ReconnectList {
            timestamps: reply
        })
}

impl ExperimentSetup {
    pub fn start(&mut self, executable_paths: &NesExecutablePaths, shutdown_triggered: Arc<AtomicBool>, log_level: &LogLevel) -> Result<(), Box<dyn Error>> {
        self.kill_processes();
        self.fixed_worker_processes = vec![];
        self.mobile_worker_processes = vec![];

        //let rest_port = get_available_port(PORT_RANGE).ok_or("Could not find available port")?;
        let rest_port = 8081;

        self.start_coordinator(&executable_paths.coordinator_path, Arc::clone(&shutdown_triggered), rest_port, &log_level)?;

        wait_for_topology(Some(1), Arc::clone(&shutdown_triggered), rest_port)?;
        // wait_for_topology(Some(2), Arc::clone(&shutdown_triggered), rest_port)?;

        self.start_fixed_workers(&executable_paths.worker_path, Arc::clone(&shutdown_triggered), &log_level)?;

        wait_for_topology(Some(self.fixed_worker_processes.len() + 1), Arc::clone(&shutdown_triggered), rest_port)?;
        //wait_for_topology(Some(self.fixed_worker_processes.len() + 1 + 1), Arc::clone(&shutdown_triggered), rest_port)?;

        self.add_edges(rest_port)?;

        //wait for user to press key to start mobile workers
        // println!("press any key to start mobile workers");
        // let input: String = text_io::read!("{}\n");


        self.start_mobile(&executable_paths.worker_path, Arc::clone(&shutdown_triggered), &log_level)?;
        //wait_for_topology(Some(self.fixed_worker_processes.len() + self.mobile_worker_processes.len() + 1), Arc::clone(&shutdown_triggered), rest_port)?;

        sleep(Duration::from_secs(7));

        Ok(())
    }

    pub fn submit_queries(output_port: u16, query_strings: Vec<String>) -> Result<(), Box<dyn Error>> {
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
        let result = client.post("http://127.0.0.1:8081/v1/nes/query/execute-query")
            .json(&execute_query_request).send()?;
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
        //kill coordinator
        match (self.coordinator_process.take()) {
            None => { println!("coordinator process not found") }
            Some(mut p) => { p.kill()? }
        }


        //.ok_or("Coordinator process not found")?.kill()?;
        Ok(())
    }


    fn start_fixed_workers(&mut self, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>, log_level: &LogLevel) -> Result<(), Box<dyn Error>> {
        for path in &self.fixed_config_paths {
            if shutdown_triggered.load(Ordering::SeqCst) {
                return Err(String::from("Shutdown triggered").into());
            }
            let process = Command::new(worker_path)
                .arg(format!("--configPath={}", path.display()))
                .arg(format!("--logLevel={}", &serde_json::to_string(log_level).unwrap().trim_matches('\"')))
                //.arg("--logLevel=LOG_DEBUG")
                .spawn()?;

            self.fixed_worker_processes.push(process);
        };
        Ok(())
    }

    fn start_mobile(&mut self, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>, log_level: &LogLevel) -> Result<(), Box<dyn Error>> {
        for path in &self.mobile_config_paths {
            if shutdown_triggered.load(Ordering::SeqCst) {
                return Err(String::from("Shutdown triggered").into());
            }
            let process = Command::new(worker_path)
                .arg(format!("--configPath={}", path.display()))
                .arg(format!("--logLevel={}", &serde_json::to_string(log_level).unwrap().trim_matches('\"')))
                //.arg("--logLevel=LOG_DEBUG")
                .spawn()?;
            self.fixed_worker_processes.push(process);
        };
        Ok(())
    }

    fn start_coordinator(&mut self, coordinator_path: &Path, shutdown_triggered: Arc<AtomicBool>, rest_port: u16, log_level: &LogLevel) -> Result<(), Box<dyn Error>> {
        self.coordinator_process = Some(Command::new(&coordinator_path)
            .arg("--restServerCorsAllowedOrigin=*")
            .arg(format!("--configPath={}", self.output_coordinator_config_path.display()))
            //.arg(format!("--restPort={}", &rest_port.to_string()))
            .arg(format!("--logLevel={}", &serde_json::to_string(log_level).unwrap().trim_matches('\"')))
            //.arg("--logLevel=LOG_DEBUG")
            .spawn()?);

        std::thread::sleep(time::Duration::from_secs(5));
        //wait until coordinator is online
        wait_for_coordinator(Arc::clone(&shutdown_triggered))?;
        std::thread::sleep(time::Duration::from_secs(1));
        Ok(())
    }
}

impl InputConfig {
    // fn get_mobility_config_list(&self) -> Result<MobilityInputConfigList, Box<dyn Error>> {
    //     let option = self.paths.get_mobility_config_list_path();
    //     if let Some(path) = option {
    //         let config: MobilityInputConfigList = toml::from_str(&*read_to_string(&path)?)?;
    //         Ok(config)
    //     } else {
    //
    //     }
    // }

    pub fn get_data_production_time(&self) -> Duration {
        self.parameters.warmup + self.parameters.reconnect_runtime + self.parameters.cooldown_time
    }
    pub fn get_total_time(&self) -> Duration {
        self.parameters.deployment_time_offset + self.parameters.warmup + self.parameters.reconnect_runtime + self.parameters.cooldown_time + self.parameters.post_cooldown_time
    }
    fn generate_output_config(&self, mut generated_folder: &Path) -> Result<ExperimentSetup, Box<dyn Error>> {
        println!("generating output config");
        // let input_trajectories_directory = &self.paths.mobile_trajectories_directory;
        let output_config_directory = generated_folder.join("config");
        fs::create_dir_all(&output_config_directory).expect("Failed to create folder");
        let output_source_input_directory = output_config_directory.join("source_input");
        fs::create_dir_all(&output_source_input_directory).expect("Failed to create folder");
        let output_trajectory_directory = output_config_directory.join("trajectory");
        fs::create_dir_all(&output_trajectory_directory).expect("Failed to create folder");
        let output_worker_config_directory = output_config_directory.join("worker_config");
        fs::create_dir_all(&output_worker_config_directory).expect("Failed to create folder");
        let output_coordinator_config_path = output_config_directory.join("coordinator_config.yaml");
        //let sink_output_path = generated_folder.join("out.csv");
        //todo: set port here
        let sink_output_path = generated_folder.join("replace_me.csv");
        let experiment_output_path = generated_folder.join("out");
        let mut logicalSources = vec![];

        println!("generating logical sources");
        for name in &self.parameters.logical_source_names {
            logicalSources.push(LogicalSource {
                logicalSourceName: name.to_string(),
                fields: vec![
                    LogicalSourceField {
                        name: "id".to_string(),
                        Type: UINT64,
                    },
                    LogicalSourceField {
                        name: "value".to_string(),
                        Type: UINT64,
                    },
                    LogicalSourceField {
                        name: "ingestion_timestamp".to_string(),
                        Type: UINT64,
                    },
                    LogicalSourceField {
                        name: "processing_timestamp".to_string(),
                        Type: UINT64,
                    },
                    LogicalSourceField {
                        name: "output_timestamp".to_string(),
                        Type: UINT64,
                    },
                ],
            });
        }

        println!("generating coordinator config");
        //generate coordinator config
        let coordinator_config = CoordinatorConfiguration {
            //enableIncrementalPlacement: self.parameters.enable_query_reconfiguration,
            enableProactiveDeployment: self.parameters.enable_proactive_deployment,
            // logicalSources: vec![
            //     LogicalSource {
            //         logicalSourceName: "values".to_string(),
            //         fields: vec![
            //             LogicalSourceField {
            //                 name: "id".to_string(),
            //                 Type: UINT64,
            //             },
            //             LogicalSourceField {
            //                 name: "value".to_string(),
            //                 Type: UINT64,
            //             },
            //             LogicalSourceField {
            //                 name: "ingestion_timestamp".to_string(),
            //                 Type: UINT64,
            //             },
            //             LogicalSourceField {
            //                 name: "processing_timestamp".to_string(),
            //                 Type: UINT64,
            //             },
            //             LogicalSourceField {
            //                 name: "output_timestamp".to_string(),
            //                 Type: UINT64,
            //             },
            //         ],
            //     }
            // ],
            logicalSources,
            logLevel: LogLevel::LOG_ERROR,
            optimizer: OptimizerConfiguration {
                enableIncrementalPlacement: self.parameters.enable_query_reconfiguration,
                placementAmendmentThreadCount: self.parameters.placementAmendmentThreadCount,
            },
        };
        coordinator_config.write_to_file(&output_coordinator_config_path)?;

        println!("reading fixed topology: {}", self.paths.get_fixed_topology_nodes_path().to_str().unwrap());
        //start fixed workers
        let json_string = std::fs::read_to_string(&self.paths.get_fixed_topology_nodes_path())?;
        let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;


        let numberOfTuplesToProducePerBuffer = match self.default_source_input.source_input_method {
            SourceInputMethod::CSV => { self.default_source_input.tuples_per_buffer.try_into()? }
            SourceInputMethod::TCP => { 0 }
        };

        println!("generating fixed worker configs");
        let mut next_free_port = 5000;
        let mut fixed_config_paths = vec![];
        let num_buffers = self.get_data_production_time().as_millis() / self.default_source_input.gathering_interval.as_millis();
        let mut total_number_of_tuples_to_ingest = 0;
        let mut max_fixed_id = 0;
        for (input_id, location) in &topology.nodes {
            if input_id > &max_fixed_id {
                max_fixed_id = *input_id;
            }
            let (physical_sources, number_of_slots) = self.get_physical_sources_for_node(numberOfTuplesToProducePerBuffer, num_buffers, &mut total_number_of_tuples_to_ingest, *input_id);
            let worker_config = FixedWorkerConfig {
                // rpcPort: next_free_port,
                // dataPort: next_free_port + 1,
                rpcPort: None,
                dataPort: None,
                //numberOfSlots: 6000, //todo: set to 1 to stress test the plan creation
                //numberOfSlots: number_of_slots,
                numberOfSlots: number_of_slots.unwrap_or(*topology.slots.get(input_id).unwrap()),
                nodeSpatialType: "FIXED_LOCATION".to_string(),
                fieldNodeLocationCoordinates: format!("{}, {}", location[0], location[1]),
                workerId: *input_id,
                physicalSources: physical_sources,
                logLevel: LogLevel::LOG_ERROR,
                numWorkerThreads: self.parameters.num_worker_threads,
            };
            let yaml_path = output_worker_config_directory.join(format!("fixed_worker{}.yaml", input_id));
            worker_config.write_to_file(&yaml_path)?;
            next_free_port += 2;
            // if (input_id == &2) {
            //     continue
            // }
            fixed_config_paths.push(yaml_path);
        }


        //println!("reading mobility config from {}", self.paths.get_mobility_config_list_path().to_str().unwrap());
        println!("creating mobility config from");
        let mut mobile_config_paths = vec![];
        //let mobility_input_config = MobilityInputConfigList::read_input_from_file(&self.paths.get_mobility_config_list_path())?;
        let mobility_input_config_path_option = &self.paths.get_mobility_config_list_path();
        //let mut input_id = max_fixed_id + 1;

        let (mut input_id, mobility_input_config) = if let Some(path) = mobility_input_config_path_option {
            let mobility_input_config = MobilityInputConfigList::read_input_from_file(&path)?;
            (max_fixed_id + 1, mobility_input_config)
        } else {
            let quaconf = self.paths.get_quadrant_config().unwrap();
            (quaconf.mobile_start_id, quaconf.get_config_list())
        };


        let mut generated_mobility_configs = vec![];
        let mut central_topology_update_list = rest_node_relocation::TopologyUpdateList::new();

        println!("generating mobile worker configs");
        for mut worker_mobility_input_config in mobility_input_config.worker_mobility_configs {
            let generated_mobility_config;
            if let Some(_) = self.paths.get_mobile_trajectories_directory() {
                //worker_mobility_input_config.mobility_base_path = Some(self.paths.get_mobile_trajectories_directory());
                worker_mobility_input_config.mobility_base_path = self.paths.get_mobile_trajectories_directory();
                //let worker_mobility_output_config = worker_mobility_input_config.to_mobility_config();
                //for worker_mobility_input_config in fs::read_dir(input_trajectories_directory)? {
                //todo: can we remove the cretion of csv source?
                // let source_csv_path = create_input_source_data(&output_source_input_directory, id.try_into().unwrap(), num_buffers.try_into().unwrap(), self.default_source_input.tuples_per_buffer)?;
                // let mobile_trajectory_file = worker_mobility_input_config.locationProviderConfig;
                let mobile_trajectory_file = worker_mobility_input_config.get_location_provider_config_path();
                //todo: find more elegant solution for this
                if mobile_trajectory_file.extension().unwrap().to_str().unwrap() != "csv" {
                    continue;
                }

                println!("reading trajectories from {}", mobile_trajectory_file.to_str().unwrap());
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_path(mobile_trajectory_file.as_path()).unwrap();
                let waypoints: Vec<MobileWorkerWaypoint> = rdr.deserialize().collect::<Result<_, csv::Error>>().unwrap();


                let csv_name = mobile_trajectory_file.file_name().unwrap();
                let output_trajectory_path = output_trajectory_directory.join(csv_name);
                let mut csv_writer = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_path(&output_trajectory_path).unwrap();


                for mut point in waypoints {
                    point.offset = point.offset.mul_f64(self.parameters.speedup_factor);
                    //add delay of 15 seconds before starting reconnects
                    if !point.offset.is_zero() {
                        point.offset = point.offset.add(self.parameters.reconnect_start_offset);
                    }
                    if (point.offset > self.parameters.reconnect_runtime) {
                        println!("skip waypoint");
                        break;
                    }


                    csv_writer.serialize(point).unwrap();
                }

                csv_writer.flush().unwrap();

                //let mut previous_parent = None;
                let mut previous_parent_id = 1;
                let output_precalculated_reconnects = match worker_mobility_input_config.reconnectPredictorType {
                    ReconnectPredictorType::LIVE => { "none".into() }
                    ReconnectPredictorType::PRECALCULATED => {
                        // let input_precalculated_reconnects = worker_mobility_input_config.precalcReconnectPath;
                        let input_precalculated_reconnects = worker_mobility_input_config.get_precalc_reconnect_path();
                        println!("reading precalculated reconnects from {}", input_precalculated_reconnects.to_str().unwrap());
                        let mut rdr = csv::ReaderBuilder::new()
                            .has_headers(false)
                            .from_path(&input_precalculated_reconnects).unwrap();
                        let reconnects: Vec<PrecalculatedReconnect> = rdr.deserialize().collect::<Result<_, csv::Error>>().unwrap();

                        let csv_name = input_precalculated_reconnects.file_name().unwrap();
                        let output_precalc_path = output_trajectory_directory.join(csv_name);
                        let mut csv_writer = csv::WriterBuilder::new()
                            .has_headers(false)
                            .from_path(&output_precalc_path).unwrap();

                        for mut reconnect in reconnects {
                            reconnect.offset = reconnect.offset.mul_f64(self.parameters.speedup_factor);
                            //add delay of 15 seconds before starting reconnects
                            //todo: make this a config parameter
                            if !reconnect.offset.is_zero() {
                                reconnect.offset = reconnect.offset.add(Duration::from_secs(40));
                            }
                            if reconnect.offset > self.parameters.reconnect_runtime {
                                println!("skip reconnect");
                                break;
                            }
                            // if let Some(previous_parent_id) = previous_parent {
                            //     central_topology_update_list.add_reconnect(reconnect.offset, input_id, previous_parent_id, reconnect.parent_id)
                            // } else {
                            //     central_topology_update_list.add_initial_connect(input_id, reconnect.parent_id);
                            // }
                            central_topology_update_list.add_reconnect(reconnect.offset, input_id, previous_parent_id, reconnect.parent_id);
                            //previous_parent = Some(reconnect.parent_id);
                            previous_parent_id = reconnect.parent_id;
                            csv_writer.serialize(reconnect).unwrap();
                        }
                        csv_writer.flush().unwrap();
                        // output_precalc_path
                        PathBuf::from(csv_name)
                    }
                };
                generated_mobility_config = InputMobilityconfig {
                    mobility_base_path: Some(output_trajectory_directory.clone()),
                    //locationProviderConfig: output_trajectory_path,
                    locationProviderConfig: RelativePathBuf::from_path(csv_name).unwrap(),
                    //locationProviderType: "CSV".to_owned(),
                    locationProviderType: "BASE".to_owned(),
                    // locationProviderConfig: String::from(output_trajectory_path.to_str()
                    //     .ok_or("Could not get output trajectory path")?),
                    reconnectPredictorType: worker_mobility_input_config.reconnectPredictorType,
                    //precalcReconnectPath: output_precalculated_reconnects,
                    precalcReconnectPath: RelativePathBuf::from_path(output_precalculated_reconnects).unwrap(),
                };
            } else {
                generated_mobility_config = InputMobilityconfig {
                    mobility_base_path: Some(output_trajectory_directory.clone()),
                    //locationProviderConfig: output_trajectory_path,
                    locationProviderConfig: RelativePathBuf::from_path("invalid").unwrap(),
                    //locationProviderType: "CSV".to_owned(),
                    locationProviderType: "BASE".to_owned(),
                    // locationProviderConfig: String::from(output_trajectory_path.to_str()
                    //     .ok_or("Could not get output trajectory path")?),
                    reconnectPredictorType: worker_mobility_input_config.reconnectPredictorType,
                    //precalcReconnectPath: output_precalculated_reconnects,
                    precalcReconnectPath: RelativePathBuf::from_path("invalid").unwrap(),
                };
            }
            generated_mobility_configs.push(generated_mobility_config.clone());

            let (physical_sources, number_of_slots) = self.get_physical_sources_for_node(numberOfTuplesToProducePerBuffer, num_buffers, &mut total_number_of_tuples_to_ingest, input_id);

            //create config
            let worker_config = MobileWorkerConfig {
                fieldNodeLocationCoordinates: "0,0".into(), //setting this only in case we are using precalculated reconnects
                // rpcPort: next_free_port,
                // dataPort: next_free_port + 1,
                rpcPort: None,
                dataPort: None,
                workerId: input_id,
                //numberOfSlots: 1,
                //numberOfSlots: number_of_slots.unwrap(),
                numberOfSlots: number_of_slots.unwrap_or(0),
                nodeSpatialType: "MOBILE_NODE".to_owned(),
                mobility: generated_mobility_config.to_mobility_config(),
                // physicalSources: vec![
                //     PhysicalSource {
                //         logicalSourceName: "values".to_owned(),
                //         physicalSourceName: "values".to_owned(),
                //         Type: PhysicalSourceType::CSV_SOURCE,
                //         configuration: PhysicalSourceConfiguration {
                //             //filePath: source_csv_path.to_str().ok_or("could not get source csv path")?.to_string(),
                //             filePath: self.parameters.source_input_server_port.to_string(),
                //             skipHeader: false,
                //             //todo: fix this
                //             sourceGatheringInterval: time::Duration::from_millis(0), //self.default_source_input.gathering_interval,
                //             //numberOfTuplesToProducePerBuffer: self.default_source_input.tuples_per_buffer.try_into()?,
                //             numberOfTuplesToProducePerBuffer,
                //             //numberOfBuffersToProduce: num_buffers.try_into()?,
                //         },
                //     }
                // ],
                physicalSources: physical_sources,
                logLevel: LogLevel::LOG_ERROR,
                numWorkerThreads: self.parameters.num_worker_threads,
            };
            let yaml_path = output_worker_config_directory.join(format!("mobile_worker{}.yaml", input_id));
            worker_config.write_to_file(&yaml_path)?;
            mobile_config_paths.push(yaml_path);
            input_id += 1;
            next_free_port += 2;
            let num_tuples = num_buffers as u64 * self.default_source_input.tuples_per_buffer as u64;
            //total_number_of_tuples_to_ingest += num_tuples;
        };

        let cvec: Vec<TopologyUpdate> = central_topology_update_list.into();
        let reconnect_json = serde_json::to_string_pretty(&cvec).unwrap();
        println!("{}", reconnect_json);

        let output_central_reconnect_path = output_trajectory_directory.join("central_reconnects.json");
        fs::write(&output_central_reconnect_path, reconnect_json).expect("Could not write central reconnects");
        let list_of_generated_mobility_configs = MobilityInputConfigList {
            worker_mobility_configs: generated_mobility_configs,
            central_topology_update_list_path: Some(output_central_reconnect_path.clone()),
        };
        list_of_generated_mobility_configs.write_to_file(&output_trajectory_directory.join("mobility_configs.toml"));

        let mut edges = vec![];
        for (parent, children) in topology.children {
            for child in children {
                edges.push((parent, child))
            }
        }

        let central_topology_updates = if let Some(quadrants) = &self.paths.get_quadrant_config() {
            //MobileDeviceQuadrants::from(quadrants.to_owned()).get_topology_updates();
            let q: MobileDeviceQuadrants::MobileDeviceQuadrants = quadrants.to_owned().into();
            // let interval = Duration::from_millis((1000f * self.parameters.speedup_factor) as );
            let interval = Duration::from_secs(1).mul_f64(self.parameters.speedup_factor);
            //let reconnect_time = self.parameters.runtime - self.parameters.cooldown_time;
            q.get_update_vector(self.parameters.reconnect_runtime, interval)
        } else {
            cvec
        };

        Ok(ExperimentSetup {
            output_config_directory,
            output_source_input_directory,
            output_trajectory_directory,
            //output_worker_config_directory,
            sink_output_path,
            experiment_output_path,
            fixed_config_paths,
            mobile_config_paths,
            output_coordinator_config_path,
            coordinator_process: None,
            mobile_worker_processes: vec![],
            fixed_worker_processes: vec![],
            edges,
            total_number_of_tuples_to_ingest,
            input_config: self.clone(),
            num_buffers,
            generated_folder: generated_folder.to_path_buf(),
            //central_topology_updates: cvec,
            central_topology_updates,
        })
    }

    fn get_physical_sources_for_node(&self, numberOfTuplesToProducePerBuffer: u64, num_buffers: u128, total_number_of_tuples_to_ingest: &mut u64, input_id: u64) -> (Vec<PhysicalSource>, Option<u16>) {
        let (physical_sources, number_of_slots) = if let Some((_, logical_source_names)) = self.parameters.place_default_sources_on_node_ids.get_key_value(&input_id.to_string()) {
            let num_tuples = num_buffers as u64 * self.default_source_input.tuples_per_buffer as u64;
            let mut sources = vec![];

            //iterate over logical source names
            for (index, logical_source_name) in logical_source_names.iter().enumerate() {
                *total_number_of_tuples_to_ingest += num_tuples;
                sources.push(PhysicalSource {
                    logicalSourceName: logical_source_name.to_string(),
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ReconnectPredictorType {
    LIVE,
    PRECALCULATED,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MobilityInputConfigList {
    pub worker_mobility_configs: Vec<InputMobilityconfig>,
    pub central_topology_update_list_path: Option<PathBuf>,
}


impl MobilityInputConfigList {
    fn read_input_from_file(file_path: &Path) -> Result<Self, Box<dyn Error>> {
        let config: Self = toml::from_str(&read_to_string(file_path)?)?;
        Ok(config)
    }

    pub fn write_to_file(&self, file_path: &Path) {
        let toml_string = toml::to_string(&self).unwrap();
        let mut file = File::create(file_path).unwrap();
        file.write_all(toml_string.as_bytes()).unwrap();
    }
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
struct MobileWorkerWaypoint {
    #[serde(rename = "column1")]
    latitude: f64,
    #[serde(rename = "column2")]
    longitude: f64,
    #[serde_as(as = "DurationNanoSeconds<u64>")]
    #[serde(rename = "column3")]
    offset: Duration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct PrecalculatedReconnect {
    #[serde(rename = "column1")]
    pub parent_id: u64,
    #[serde_as(as = "DurationNanoSeconds<u64>")]
    #[serde(rename = "column2")]
    pub offset: Duration,
}


#[derive(Deserialize, Debug)]
pub struct FixedTopology {
    //todo: check if we can just make that a tuple
    pub nodes: HashMap<u64, Vec<f64>>,
    pub slots: HashMap<u64, u16>,
    pub children: HashMap<u64, Vec<u64>>,
}

#[derive(Deserialize, Debug)]
struct ActualTopology {
    edges: Vec<Edge>,
    nodes: Vec<ActualNode>,
}

#[derive(Deserialize, Debug)]
struct ActualNode {
    available_resources: u16,
    id: u64,
    ip_address: String,
    location: Option<Location>,
    nodeType: String,
}

#[derive(Deserialize, Debug)]
struct Location {
    latitude: f64,
    longitude: f64,
}

#[derive(Deserialize, Debug)]
struct Edge {
    source: u64,
    target: u64,
}

#[derive(Deserialize, Debug)]
struct ConnectivityReply {
    statusCode: u64,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum PhysicalSourceType {
    CSV_SOURCE
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct PhysicalSourceConfiguration {
    filePath: String,
    skipHeader: bool,
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    sourceGatheringInterval: Duration,
    //in millisec
    numberOfTuplesToProducePerBuffer: u64,
    //numberOfBuffersToProduce: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum SocketDomain {
    AF_INET,
}

#[derive(Debug, Serialize, Deserialize)]
enum SocketType {
    SOCKET_STREAM
}

#[derive(Debug, Serialize, Deserialize)]
enum SourceInputFormat {
    CSV
}

#[derive(Debug, Serialize, Deserialize)]
enum DecidedmMessageSize {
    TUPLE_SEPARATOR
}

#[derive(Debug, Serialize, Deserialize)]
struct TCPSourceConfiguration {
    socketDomain: SocketDomain,
    socketType: SocketType,
    port: u16,
    host: String,
    format: SourceInputFormat,
    decideMessageSize: DecidedmMessageSize,
    tupleSeparator: char,
    flushIntervalMS: u64,
}


#[derive(Debug, Serialize, Deserialize)]
struct PhysicalSource {
    logicalSourceName: String,
    physicalSourceName: String,
    #[serde(rename(deserialize = "type"))]
    #[serde(rename(serialize = "type"))]
    Type: PhysicalSourceType,
    configuration: PhysicalSourceConfiguration,
}


//todo: also add the coordinator port
#[derive(Debug, Serialize, Deserialize)]
struct MobileWorkerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    rpcPort: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataPort: Option<u16>,
    workerId: u64,
    numberOfSlots: u16,
    nodeSpatialType: String,
    mobility: Mobilityconfig,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    #[serde(default)]
    physicalSources: Vec<PhysicalSource>,
    fieldNodeLocationCoordinates: String,
    logLevel: LogLevel,
    numWorkerThreads: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct FixedWorkerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    rpcPort: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataPort: Option<u16>,
    numberOfSlots: u16,
    nodeSpatialType: String,
    fieldNodeLocationCoordinates: String,
    workerId: u64,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    #[serde(default)]
    physicalSources: Vec<PhysicalSource>,
    logLevel: LogLevel,
    numWorkerThreads: u64,
    //fieldNodeLocationCoordinates: (f64, f64),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Mobilityconfig {
    pub locationProviderConfig: PathBuf,
    pub locationProviderType: String,
    pub reconnectPredictorType: ReconnectPredictorType,
    pub precalcReconnectPath: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InputMobilityconfig {
    #[serde(skip)]
    mobility_base_path: Option<PathBuf>,
    #[serde(deserialize_with = "deserialize_relative_path")]
    locationProviderConfig: RelativePathBuf,
    pub locationProviderType: String,
    pub reconnectPredictorType: ReconnectPredictorType,
    #[serde(deserialize_with = "deserialize_relative_path")]
    precalcReconnectPath: RelativePathBuf,
}

impl InputMobilityconfig {
    pub fn get_location_provider_config_path(&self) -> PathBuf {
        self.locationProviderConfig.to_path(self.mobility_base_path.as_ref().expect("mobility base path not set"))
    }

    pub fn get_precalc_reconnect_path(&self) -> PathBuf {
        self.precalcReconnectPath.to_path(self.mobility_base_path.as_ref().expect("mobility base path not set"))
    }

    //create a mobility config containing the absolute paths
    pub fn to_mobility_config(&self) -> Mobilityconfig {
        Mobilityconfig {
            locationProviderConfig: self.get_location_provider_config_path(),
            locationProviderType: self.locationProviderType.clone(),
            reconnectPredictorType: self.reconnectPredictorType.clone(),
            precalcReconnectPath: self.get_precalc_reconnect_path(),
        }
    }
}

enum LocationProviderType {
    BASE,
    CSV,
    INVALID,
}

#[derive(Debug, Serialize, Deserialize)]
enum FieldType {
    FLOAT64,
    UINT64,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogicalSourceField {
    name: String,
    #[serde(rename(deserialize = "type"))]
    #[serde(rename(serialize = "type"))]
    Type: FieldType,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogicalSource {
    logicalSourceName: String,
    fields: Vec<LogicalSourceField>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoordinatorConfiguration {
    enableProactiveDeployment: bool,
    logicalSources: Vec<LogicalSource>,
    logLevel: LogLevel,
    optimizer: OptimizerConfiguration,
}

#[derive(Debug, Serialize, Deserialize)]
struct OptimizerConfiguration {
    enableIncrementalPlacement: bool,
    placementAmendmentThreadCount: u16,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_NONE,
}

impl CoordinatorConfiguration {
    fn write_to_file(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let yaml_string = serde_yaml::to_string(&self)?;
        let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
        let mut after_round_trip = String::new();
        YamlEmitter::new(&mut after_round_trip).dump(&round_trip_yaml[0]).unwrap();
        fs::write(&path, after_round_trip)?;
        Ok(())
    }
}

impl MobileWorkerConfig {
    fn write_to_file(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let yaml_string = serde_yaml::to_string(&self)?;
        let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
        let mut after_round_trip = String::new();
        YamlEmitter::new(&mut after_round_trip).dump(&round_trip_yaml[0]).unwrap();
        fs::write(&path, after_round_trip)?;
        Ok(())
    }
}

//todo: make this a trait
impl FixedWorkerConfig {
    fn write_to_file(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let yaml_string = serde_yaml::to_string(&self)?;
        let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
        let mut after_round_trip = String::new();
        YamlEmitter::new(&mut after_round_trip).dump(&round_trip_yaml[0]).unwrap();
        fs::write(&path, after_round_trip)?;
        Ok(())
    }
}

struct LocalWorkerHandle {
    config: WorkerConfigType,
    command: Command,
    process: Option<Child>,
    tmp_dir: String,
}

struct ProvisionalWorkerHandle {
    config: WorkerConfigType,
    process: Child,
    children: Vec<u64>,
}

trait WorkerHandle {
    fn get_nes_id(&self) -> u64;
}

impl LocalWorkerHandle {
    fn new_fixed_location_worker(command: &str, config: FixedWorkerConfig, tmp_dir: &str) -> Self {
        Self {
            config: Fixed(config),
            command: Command::new(command),
            process: None,
            tmp_dir: tmp_dir.to_owned(),
        }
    }
}

enum WorkerConfigType {
    Fixed(FixedWorkerConfig),
    Mobile(MobileWorkerConfig),
}


pub async fn handle_connection(stream: tokio::net::TcpStream, mut line_count: Arc<AtomicUsize>, desired_line_count: u64, mut file: Arc<Mutex<File>>, shutdown_triggered: Arc<AtomicBool>, start_time: SystemTime, experiment_duration: Duration) -> Result<(), Box<dyn Error>> {
    // Create a buffer reader for the incoming data
    //let mut reader = tokio::io::BufReader::new(stream);
    //let mut line = String::new();
    let mut buf = vec![];

    // Iterate over the lines received from the client and write them to the CSV file
    loop {
        //while let Ok(bytes_read) = reader.read_line(&mut line).await {
        //exit if shutdown was triggered
        if (shutdown_triggered.load(SeqCst)) {
            break;
        }
        let current_time = SystemTime::now();
        if let Ok(elapsed_time) = current_time.duration_since(start_time) {
            if elapsed_time > experiment_duration {
                break;
            }
        }


        // Increment the line count
        //line_count.fetch_add(1, Ordering::SeqCst);
        // Check if the maximum number of lines has been written
        // if line_count.load(SeqCst) >= desired_line_count as usize {
        //     break;
        // }
        // if bytes_read == 0 {
        //     break; // EOF, so end the loop
        // }
        if let Ok(bytes_read) = stream.try_read_buf(&mut buf) {}
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    //let mut reader = tokio::io::BufReader::new(&*buf);

    println!("Timeout, counting tuples an writing file");

    //count all line breaks in the received buffer
    // let mut current_valid_byte_count = 0;
    // let mut total_valid_byte_count = 0;
    // for &byte in &buf {
    //     current_valid_byte_count += 1;
    //     if byte == 10u8 {
    //         line_count.fetch_add(1, Ordering::SeqCst);
    //         total_valid_byte_count += current_valid_byte_count;
    //         current_valid_byte_count = 0;
    //     }
    // }
    // 
    // // sanity check
    // for &byte in &buf[total_valid_byte_count..] {
    //     if byte == 10u8 {
    //         panic!("Sanity check failed, found line break after valid byte count");
    //     }
    // }
    //file.write_all(&buf[0..total_valid_byte_count])?;

    let tuple_size = 40;
    let valid_bytes = buf.len() - (buf.len() % tuple_size);
    let mut lock = file.lock().unwrap();
    for i in (0..valid_bytes).step_by(tuple_size) {
        line_count.fetch_add(1, Ordering::SeqCst);
        let tuple = &buf[i..i + tuple_size];
        let tuple_string = get_tuple_string(tuple);
        lock.write_all(tuple_string.as_bytes())?;
        lock.write_all(b"\n")?;
    }


    println!("Received {} lines of {}", line_count.load(SeqCst), desired_line_count);

    Ok(())
}


fn get_tuple_string(binary_tuple: &[u8]) -> String {
    let id = u64::from_le_bytes([binary_tuple[0], binary_tuple[1], binary_tuple[2], binary_tuple[3], binary_tuple[4], binary_tuple[5], binary_tuple[6], binary_tuple[7]]);
    let sequence_number = u64::from_le_bytes([binary_tuple[8], binary_tuple[9], binary_tuple[10], binary_tuple[11], binary_tuple[12], binary_tuple[13], binary_tuple[14], binary_tuple[15]]);
    let event_timestamp = u64::from_le_bytes([binary_tuple[16], binary_tuple[17], binary_tuple[18], binary_tuple[19], binary_tuple[20], binary_tuple[21], binary_tuple[22], binary_tuple[23]]);
    let ingestion_timestamp = u64::from_le_bytes([binary_tuple[24], binary_tuple[25], binary_tuple[26], binary_tuple[27], binary_tuple[28], binary_tuple[29], binary_tuple[30], binary_tuple[31]]);
    let output_timestamp = u64::from_le_bytes([binary_tuple[32], binary_tuple[33], binary_tuple[34], binary_tuple[35], binary_tuple[36], binary_tuple[37], binary_tuple[38], binary_tuple[39]]);
    format!("{},{},{},{},{}", id, sequence_number, event_timestamp, ingestion_timestamp, output_timestamp)
}

fn count_lines_in_file(file_path: &Path) -> io::Result<usize> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;
    for _line in reader.lines() {
        line_count += 1;
    }
    Ok(line_count)
}

fn create_folder_with_timestamp(mut path: PathBuf, prefix: &str) -> PathBuf {
    // Get the current date and time in the local timezone
    let current_time: chrono::DateTime<Local> = Local::now();

    // Format the date and time as a string (e.g., "2024-01-24_12-34-56")
    let formatted_timestamp = current_time.format("%Y-%m-%d_%H-%M-%S").to_string();

    // Create the folder with the formatted timestamp as the name
    let folder_name = format!("{}{}", prefix, formatted_timestamp);
    path.push(folder_name);
    fs::create_dir_all(&path).expect("Failed to create folder");

    println!("Folder created: {}", path.display());
    path
}

fn wait_for_coordinator(shutdown_triggered: Arc<AtomicBool>) -> std::result::Result<(), Box<dyn Error>> {
    for i in 0..10 {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        if let Ok(reply) = reqwest::blocking::get("http://127.0.0.1:8081/v1/nes/connectivity/check") {
            if reply.json::<ConnectivityReply>().unwrap().success {
                println!("Coordinator has connected");
                return Ok(());
            }
        }
        sleep(Duration::from_secs(1));
    }
    println!("Coordinator did not connect");
    Err(String::from("Coordinator did not connect").into())
}

fn wait_for_topology(expected_node_count: Option<usize>, shutdown_triggered: Arc<AtomicBool>, restPort: u16) -> std::result::Result<usize, Box<dyn Error>> {
    println!("waiting for topology, rest port {}", &restPort.to_string());
    for i in 0..10 {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        if let Ok(mut reply) = reqwest::blocking::get(format!("http://127.0.0.1:{}/v1/nes/topology", restPort)) {
            let size = reply.json::<ActualTopology>().unwrap().nodes.len();
            println!("topology contains {} nodes", size);
            if let Some(expected) = expected_node_count {
                if size == expected {
                    return Ok(size);
                }
                println!("number of nodes not reached");
            }
        }
        std::thread::sleep(time::Duration::from_secs(1));
    }
    Err(String::from("Expected node count not reached in topology").into())
}

fn create_csv_file(file_path: &str, id: u32, num_rows: usize) -> Result<(), Box<dyn Error>> {
    // Create or open the CSV file
    let mut file = File::create(file_path)?;

    // Create a CSV writer
    let mut csv_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

    // Write rows to the CSV file
    for sequence_number in 0..num_rows {
        // Write the id and sequence number to the CSV file
        csv_writer.write_record(&[id.to_string(), sequence_number.to_string()])?;
    }

    // Flush the CSV writer to ensure all data is written to the file
    csv_writer.flush()?;

    Ok(())
}


fn create_input_source_data(
    directory_path: &Path,
    id: u32,
    num_buffers: usize,
    tuples_per_buffer: usize,
) -> Result<PathBuf, Box<dyn Error>> {
    // Create the directory if it doesn't exist
    fs::create_dir_all(directory_path)?;

    let file_name = format!("source_input{}.csv", id);


    // Construct the full file path
    let file_path = Path::new(directory_path).join(&file_name);

    // Calculate the number of rows based on the experiment runtime, gathering interval, and tuples per buffer
    let num_rows = tuples_per_buffer * num_buffers;

    // Call the create_csv_file function to generate the CSV file
    create_csv_file(file_path.to_str().ok_or("Error getting file string")?, id, num_rows)?;

    println!("Input file created: {}", file_path.display());

    Ok(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserializing_log_level() {
        let log_level: LogLevel = serde_json::from_str("\"LOG_DEBUG\"").unwrap();
        assert_eq!(log_level, LogLevel::LOG_DEBUG);
    }
}
