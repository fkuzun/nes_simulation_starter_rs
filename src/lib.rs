use std::collections::HashMap;
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
use std::ops::{Add, Sub};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::Ordering::SeqCst;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use sync::atomic;
use chrono::Local;
use nes_tools::launch::Launch;
use nes_tools::topology::{AddEdgeRequest, ExecuteQueryRequest, PlacementStrategyType};
use serde_with::serde_as;
use yaml_rust::{YamlEmitter, YamlLoader};
use crate::FieldType::UINT64;
use crate::WorkerConfigType::Fixed;
use serde_with::DurationMilliSeconds;
use serde_with::DurationNanoSeconds;
use serde_with::DurationSeconds;

pub mod analyze;

const INPUT_FOLDER_SUB_PATH: &'static str = "nes_experiment_input";
const INPUT_CONFIG_NAME: &'static str = "input_data_config.toml";

#[derive(Deserialize, Debug)]
pub struct SimulationConfig {
    pub nes_root_dir: PathBuf,
    pub relative_worker_path: PathBuf,
    pub relative_coordinator_path: PathBuf,
    pub output_directory: PathBuf,
    pub input_config_path: PathBuf,
}

impl SimulationConfig {
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
        let input_config: InputConfig = toml::from_str(&*read_to_string(&self.get_input_config_path()).expect("Could not read config file")).expect("could not parse confit file");
        input_config
    }

    fn create_generated_folder(&self) -> PathBuf {
        //create_folder_with_timestamp(self.experiment_directory.clone(), "generated_experiment_")
        create_folder_with_timestamp(self.output_directory.clone(), self.input_config_path.file_name().unwrap().to_str().unwrap())
    }

    pub fn generate_experiment_configs(&self) -> Result<ExperimentSetup, Box<dyn Error>> {
        let generated_folder = self.create_generated_folder();
        let input_config = self.read_input_config();
        let input_config_copy_path = generated_folder.join("input_config_copy.toml");
        let toml_string = toml::to_string(&input_config)?;
        let mut file = File::create(input_config_copy_path)?;
        file.write_all(toml_string.as_bytes())?;

        input_config.generate_output_config(&generated_folder)
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
    pub speedup_factor: f64,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub runtime: Duration,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub cooldown_time: Duration,
    pub reconnect_input_type: ReconnectPredictorType,
    pub source_input_server_port: u16,
    pub query_string: String,
    pub place_default_source_on_fixed_node_ids: Vec<u64>,
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Paths {
    fixed_topology_nodes: String,
    mobile_trajectories_directory: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct InputConfig {
    pub parameters: Parameters,
    pub default_source_input: DefaultSourceInput,
    paths: Paths,
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
}

impl ExperimentSetup {
    pub fn start(&mut self, executable_paths: NesExecutablePaths, shutdown_triggered: Arc<AtomicBool>) -> Result<(), Box<dyn Error>> {
        self.start_coordinator(&executable_paths.coordinator_path, Arc::clone(&shutdown_triggered))?;

        wait_for_topology(Some(1), Arc::clone(&shutdown_triggered))?;

        self.start_fixed_workers(&executable_paths.worker_path, Arc::clone(&shutdown_triggered))?;

        self.add_edges()?;

        //wait for user to press key to start mobile workers
        // println!("press any key to start mobile workers");
        // let input: String = text_io::read!("{}\n");


        self.start_mobile(&executable_paths.worker_path, Arc::clone(&shutdown_triggered))?;

        sleep(Duration::from_secs(4));

        let execute_query_request = ExecuteQueryRequest {
            //user_query: "Query::from(\"values\").sink(FileSinkDescriptor::create(\n  \"/tmp/test_sink\",\n  \"CSV_FORMAT\",\n  \"true\" // *\"true\"* for append, *\"false\"* for overwrite\n  ));".to_string(),
            //user_query: format!("Query::from(\"values\").sink(FileSinkDescriptor::create(\n  \"{}\",\n  \"CSV_FORMAT\",\n  \"true\" // *\"true\"* for append, *\"false\"* for overwrite\n  ));", self.sink_output_path.display()).to_string(),
            //user_query: format!("Query::from(\"values\").map(Attribute(\"value\") = Attribute(\"value\") * 2).sink(FileSinkDescriptor::create(\"{}\", \"CSV_FORMAT\", \"true\"));", self.sink_output_path.display()).to_string(),
            //user_query: String::from("Query::from(\"values\").map(Attribute(\"value\") = Attribute(\"value\") * 2).sink(FileSinkDescriptor::create(\"{OUTPUT}\", \"CSV_FORMAT\", \"true\"));").replace("{OUTPUT}", self.sink_output_path.to_str().unwrap()),
            user_query: self.input_config.parameters.query_string.replace("{OUTPUT}", self.sink_output_path.to_str().unwrap()),
            placement: PlacementStrategyType::BottomUp,
        };
        let client = reqwest::blocking::Client::new();
        let result = client.post("http://127.0.0.1:8081/v1/nes/query/execute-query")
            .json(&execute_query_request).send()?;

        Ok(())
    }

    fn add_edges(&self) -> Result<(), Box<dyn Error>> {
        let client = reqwest::blocking::Client::new();
        for (parent_id, child_id) in &self.edges {
            if parent_id == &1 {
                continue;
            }
            let link_request = AddEdgeRequest {
                parent_id: *parent_id,
                child_id: *child_id,
            };
            let result = client.post("http://127.0.0.1:8081/v1/nes/topology/addAsChild")
                .json(&link_request).send()?;
            let link_request = AddEdgeRequest {
                parent_id: 1,
                child_id: *child_id,
            };
            let result = client.delete("http://127.0.0.1:8081/v1/nes/topology/removeAsChild")
                .json(&link_request).send()?;
            //assert!(result.json().unwrap());
        };
        Ok(())
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
        self.coordinator_process.take().ok_or("Coordinator process not found")?.kill()?;
        Ok(())
    }


    fn start_fixed_workers(&mut self, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>) -> Result<(), Box<dyn Error>> {
        for path in &self.fixed_config_paths {
            if shutdown_triggered.load(Ordering::SeqCst) {
                return Err(String::from("Shutdown triggered").into());
            }
            let process = Command::new(worker_path)
                .arg(format!("--configPath={}", path.display()))
                .spawn()?;

            self.fixed_worker_processes.push(process);
        };
        Ok(())
    }

    fn start_mobile(&mut self, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>) -> Result<(), Box<dyn Error>> {
        for path in &self.mobile_config_paths {
            if shutdown_triggered.load(Ordering::SeqCst) {
                return Err(String::from("Shutdown triggered").into());
            }
            let process = Command::new(worker_path)
                .arg(format!("--configPath={}", path.display()))
                .spawn()?;
            self.fixed_worker_processes.push(process);
        };
        Ok(())
    }

    fn start_coordinator(&mut self, coordinator_path: &Path, shutdown_triggered: Arc<AtomicBool>) -> Result<(), Box<dyn Error>> {
        // self.coordinator_process = Some(Command::new(&coordinator_path)
        //     .arg("--restServerCorsAllowedOrigin=*")
        //     .arg(format!("--configPath={}", self.output_coordinator_config_path.display()))
        //     .spawn()?);
        //
        // //wait until coordinator is online
        // wait_for_coordinator(Arc::clone(&shutdown_triggered))?;
        // std::thread::sleep(time::Duration::from_secs(1));
        Ok(())
    }
}

impl InputConfig {
    fn generate_output_config(&self, mut generated_folder: &Path) -> Result<ExperimentSetup, Box<dyn Error>> {
        let input_trajectories_directory = &self.paths.mobile_trajectories_directory;
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
        let experiment_output_path = generated_folder.join("out.csv");

        //generate coordinator config
        let coordinator_config = CoordinatorConfiguration {
            enableQueryReconfiguration: self.parameters.enable_query_reconfiguration,
            logicalSources: vec![
                LogicalSource {
                    logicalSourceName: "values".to_string(),
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
                    ],
                }
            ],
        };
        coordinator_config.write_to_file(&output_coordinator_config_path)?;

        //start fixed workers
        let json_string = std::fs::read_to_string(&self.paths.fixed_topology_nodes)?;
        let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;

        let numberOfTuplesToProducePerBuffer = match self.default_source_input.source_input_method {
            SourceInputMethod::CSV => { self.default_source_input.tuples_per_buffer.try_into()? }
            SourceInputMethod::TCP => { 0 }
        };

        let mut next_free_port = 5000;
        let mut id = 2;
        let mut input_id_to_system_id_map = HashMap::new();
        let mut fixed_config_paths = vec![];
        for (input_id, location) in &topology.nodes {
            let (physical_sources, number_of_slots) = if self.parameters.place_default_source_on_fixed_node_ids.contains(input_id) {
                (vec![
                    PhysicalSource {
                        logicalSourceName: "values".to_owned(),
                        physicalSourceName: "values".to_owned(),
                        Type: PhysicalSourceType::CSV_SOURCE,
                        configuration: PhysicalSourceConfiguration {
                            //filePath: source_csv_path.to_str().ok_or("could not get source csv path")?.to_string(),
                            filePath: self.parameters.source_input_server_port.to_string(),
                            skipHeader: false,
                            //todo: fix this
                            sourceGatheringInterval: time::Duration::from_millis(100), //self.default_source_input.gathering_interval,
                            //numberOfTuplesToProducePerBuffer: self.default_source_input.tuples_per_buffer.try_into()?,
                            numberOfTuplesToProducePerBuffer,
                            //numberOfBuffersToProduce: num_buffers.try_into()?,
                        },
                    }
                ], 1)
            } else {
                (vec![], 60000)
            };
            let worker_config = FixedWorkerConfig {
                rpcPort: next_free_port,
                dataPort: next_free_port + 1,
                //numberOfSlots: 6000, //todo: set to 1 to stress test the plan creation
                numberOfSlots: number_of_slots,
                nodeSpatialType: "FIXED_LOCATION".to_string(),
                fieldNodeLocationCoordinates: format!("{}, {}", location[0], location[1]),
                workerId: id,
                physicalSources: physical_sources,
            };
            let yaml_path = output_worker_config_directory.join(format!("fixed_worker{}.yaml", id));
            worker_config.write_to_file(&yaml_path)?;
            fixed_config_paths.push(yaml_path);
            input_id_to_system_id_map.insert(input_id, id);
            id += 1;
            next_free_port += 2;
        }


        let mut total_number_of_tuples_to_ingest = 0;
        let mut mobile_config_paths = vec![];
        let num_buffers = self.parameters.runtime.as_millis() / self.default_source_input.gathering_interval.as_millis();
        let mobility_input_config = MobilityInputConfigList::read_input_from_file(&PathBuf::from(&input_trajectories_directory).join("mobile_config_list.toml"))?;
        let mut generated_mobility_configs = vec![];
        for worker_mobility_input_config in mobility_input_config.worker_mobility_configs {
            //for worker_mobility_input_config in fs::read_dir(input_trajectories_directory)? {
            //todo: can we remove the cretion of csv source?
            // let source_csv_path = create_input_source_data(&output_source_input_directory, id.try_into().unwrap(), num_buffers.try_into().unwrap(), self.default_source_input.tuples_per_buffer)?;
            let mobile_trajectory_file = worker_mobility_input_config.locationProviderConfig;
            //todo: find more elegant solution for this
            if mobile_trajectory_file.extension().unwrap().to_str().unwrap() != "csv" {
                continue;
            }

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
                if (point.offset > (self.parameters.runtime.sub(self.parameters.cooldown_time))) {
                    println!("skip waypoint");
                    break;
                }
                csv_writer.serialize(point).unwrap();
            }

            csv_writer.flush().unwrap();

            let output_precalculated_reconnects = match worker_mobility_input_config.reconnectPredictorType {
                ReconnectPredictorType::LIVE => { "none".into() }
                ReconnectPredictorType::PRECALCULATED => {
                    let input_precalculated_reconnects = worker_mobility_input_config.precalcReconnectPath;
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
                        if reconnect.offset > self.parameters.runtime.sub(self.parameters.cooldown_time) {
                            println!("skip reconnect");
                            break;
                        }
                        csv_writer.serialize(reconnect).unwrap();
                    }
                    csv_writer.flush().unwrap();
                    output_precalc_path
                }
            };

            let generated_mobility_config = Mobilityconfig {
                locationProviderConfig: output_trajectory_path,
                //locationProviderType: "CSV".to_owned(),
                locationProviderType: "BASE".to_owned(),
                // locationProviderConfig: String::from(output_trajectory_path.to_str()
                //     .ok_or("Could not get output trajectory path")?),
                reconnectPredictorType: worker_mobility_input_config.reconnectPredictorType,
                precalcReconnectPath: output_precalculated_reconnects,
            };

            generated_mobility_configs.push(generated_mobility_config.clone());

            //create config
            let worker_config = MobileWorkerConfig {
                fieldNodeLocationCoordinates: "0,0".into(), //setting this only in case we are using precalculated reconnects
                rpcPort: next_free_port,
                dataPort: next_free_port + 1,
                workerId: id,
                numberOfSlots: 1,
                nodeSpatialType: "MOBILE_NODE".to_owned(),
                mobility: generated_mobility_config,
                physicalSources: vec![
                    PhysicalSource {
                        logicalSourceName: "values".to_owned(),
                        physicalSourceName: "values".to_owned(),
                        Type: PhysicalSourceType::CSV_SOURCE,
                        configuration: PhysicalSourceConfiguration {
                            //filePath: source_csv_path.to_str().ok_or("could not get source csv path")?.to_string(),
                            filePath: self.parameters.source_input_server_port.to_string(),
                            skipHeader: false,
                            //todo: fix this
                            sourceGatheringInterval: time::Duration::from_millis(100), //self.default_source_input.gathering_interval,
                            //numberOfTuplesToProducePerBuffer: self.default_source_input.tuples_per_buffer.try_into()?,
                            numberOfTuplesToProducePerBuffer,
                            //numberOfBuffersToProduce: num_buffers.try_into()?,
                        },
                    }
                ],
            };
            let yaml_path = output_worker_config_directory.join(format!("mobile_worker{}.yaml", id));
            worker_config.write_to_file(&yaml_path)?;
            mobile_config_paths.push(yaml_path);
            id += 1;
            next_free_port += 2;
            let num_tuples = num_buffers as u64 * self.default_source_input.tuples_per_buffer as u64;
            total_number_of_tuples_to_ingest += num_tuples;
        };

        let list_of_generated_mobility_configs = MobilityInputConfigList {
            worker_mobility_configs: generated_mobility_configs,
        };
        list_of_generated_mobility_configs.write_to_file(&output_trajectory_directory.join("mobility_configs.toml"));

        let mut edges = vec![];
        for (parent, children) in topology.children {
            for child in children {
                edges.push((*input_id_to_system_id_map.get(&parent).ok_or("Could not find parent id")?, *input_id_to_system_id_map.get(&child).ok_or("Could not find child id")?))
            }
        }

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
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ReconnectPredictorType {
    LIVE,
    PRECALCULATED,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MobilityInputConfigList {
    pub worker_mobility_configs: Vec<Mobilityconfig>,
}


impl MobilityInputConfigList {
    fn read_input_from_file(file_path: &Path) -> Result<Self, Box<dyn Error>> {
        let config: Self = toml::from_str(&*read_to_string(file_path)?)?;
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
struct PrecalculatedReconnect {
    #[serde(rename = "column1")]
    parent_id: u64,
    #[serde_as(as = "DurationNanoSeconds<u64>")]
    #[serde(rename = "column3")]
    offset: Duration,
}


#[derive(Deserialize, Debug)]
pub struct FixedTopology {
    //todo: check if we can just make that a tuple
    pub nodes: HashMap<u64, Vec<f64>>,
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
    rpcPort: u16,
    dataPort: u16,
    workerId: u64,
    numberOfSlots: u16,
    nodeSpatialType: String,
    mobility: Mobilityconfig,
    physicalSources: Vec<PhysicalSource>,
    fieldNodeLocationCoordinates: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FixedWorkerConfig {
    rpcPort: u16,
    dataPort: u16,
    numberOfSlots: u16,
    nodeSpatialType: String,
    fieldNodeLocationCoordinates: String,
    workerId: u64,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    physicalSources: Vec<PhysicalSource>,
    //fieldNodeLocationCoordinates: (f64, f64),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Mobilityconfig {
    pub locationProviderConfig: PathBuf,
    pub locationProviderType: String,
    pub reconnectPredictorType: ReconnectPredictorType,
    pub precalcReconnectPath: PathBuf,
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
    enableQueryReconfiguration: bool,
    logicalSources: Vec<LogicalSource>,
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


pub fn handle_connection(stream: std::net::TcpStream, line_count: &mut usize, desired_line_count: u64, file: &mut File) -> Result<(), Box<dyn Error>> {
    // Create a buffer reader for the incoming data
    let reader = BufReader::new(&stream);


    // Iterate over the lines received from the client and write them to the CSV file
    for line in reader.lines() {
        let line = line?;
        writeln!(file, "{}", line)?;

        // Increment the line count
        *line_count += 1;

        // Check if the maximum number of lines has been written
        if *line_count >= desired_line_count as usize {
            break;
        }
    }
    println!("Received {} lines of {}", *line_count, desired_line_count);

    Ok(())
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
    loop {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        if let Ok(reply) = reqwest::blocking::get("http://127.0.0.1:8081/v1/nes/connectivity/check") {
            if reply.json::<ConnectivityReply>().unwrap().success {
                println!("Coordinator has connected");
                break Ok(());
            }
            let input: String = text_io::read!("{}\n");
            std::thread::sleep(time::Duration::from_secs(1));
        }
        //todo: sleep
    }
}

fn wait_for_topology(expected_node_count: Option<usize>, shutdown_triggered: Arc<AtomicBool>) -> std::result::Result<usize, Box<dyn Error>> {
    loop {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        if let Ok(mut reply) = reqwest::blocking::get("http://127.0.0.1:8081/v1/nes/topology") {
            let size = reply.json::<ActualTopology>().unwrap().nodes.len();
            println!("topology contains {} nodes", size);
            if let Some(expected) = expected_node_count {
                if size == expected {
                    break Ok(size);
                }
                println!("number of nodes not reached");
                std::thread::sleep(time::Duration::from_secs(1));
            } else {
                break Ok(size);
            }
        }
    }
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
