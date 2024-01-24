use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::hash::Hasher;
use std::io::{Read, Write};
use std::os::unix::raw::{time_t, uid_t};
use std::process::{Child, Command, Stdio};
use std::{fs, sync, time};
use std::fs::File;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
use sync::atomic;
use nes_tools::launch::Launch;
use nes_tools::topology::{AddEdgeRequest, ExecuteQueryRequest, PlacementStrategyType};
use yaml_rust::{YamlEmitter, YamlLoader};
use crate::FieldType::UINT64;
use crate::WorkerConfigType::Fixed;

const input_folder_sub_path: &'static str = "nes_experiment_input";

#[derive(Deserialize, Debug)]
struct SimulationConfig {
    nes_root_dir: PathBuf,
    relative_worker_path: PathBuf,
    relative_coordinator_path: PathBuf,
    experiment_directory: PathBuf,
}

impl SimulationConfig {
    fn get_input_folder_path(&self) -> PathBuf {
        let mut path = self.experiment_directory.clone();
        path.push(input_folder_sub_path.into());
        path
    }
}


struct NesExecutablePaths {
    worker_path: PathBuf,
    coordinator_path: PathBuf,
}

impl NesExecutablePaths {
    fn new(config: SimulationConfig) -> Self {
        let mut worker_path = config.nes_root_dir.clone();
        worker_path.push(config.relative_worker_path);
        let mut coordinator_path = config.nes_root_dir;
        coordinator_path.push(config.relative_coordinator_path);
        Self {
            worker_path,
            coordinator_path,
        }
    }
}


#[derive(Debug, Deserialize)]
struct Parameters {
    speedup_factor: f64,
    runtime: u32,
    cooldown_time: u32,
}

#[derive(Debug, Deserialize)]
struct DefaultSourceInput {
    tuples_per_buffer: usize,
    gathering_interval: u32,
}

#[derive(Debug, Deserialize)]
struct Paths {
    fixed_topology_nodes: String,
    mobile_trajectories_directory: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    parameters: Parameters,
    default_source_input: DefaultSourceInput,
    paths: Paths,
}


#[derive(Deserialize, Debug)]
struct FixedTopology {
    //todo: check if we can just make that a tuple
    nodes: HashMap<u64, Vec<f64>>,
    children: HashMap<u64, Vec<u64>>,
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

#[derive(Debug, Serialize, Deserialize)]
struct PhysicalSourceConfiguration {
    filePath: String,
    skipHeader: bool,
    sourceGatheringInterval: u64,
    //in millisec
    numberOfTuplesToProducePerBuffer: u64,
    numberOfBuffersToProduce: u64,
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
    nodeSpatialType: String,
    mobility: Mobilityconfig,
    physicalSources: Vec<PhysicalSource>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FixedWorkerConfig {
    rpcPort: u16,
    dataPort: u16,
    nodeSpatialType: String,
    fieldNodeLocationCoordinates: String,
    workerId: u64,
    //fieldNodeLocationCoordinates: (f64, f64),
}

#[derive(Debug, Serialize, Deserialize)]
struct Mobilityconfig {
    locationProviderConfig: String,
    locationProviderType: String,
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
    logicalSources: Vec<LogicalSource>,
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

// impl Launch for LocalWorkerHandle {
//     fn launch(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//         *self.get_process() = Some(self.get_command().spawn()?);
//         Ok(())
//     }
//
//     fn launch_with_pipe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//         todo!()
//     }
//
//     fn kill(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//         self.get_process().take().ok_or("No process exists")?.kill().or(Err("Could not kill process".into()))
//     }
// }


enum WorkerConfigType {
    Fixed(FixedWorkerConfig),
    Mobile(MobileWorkerConfig),
}

fn main() {
    //todo: read this from file
    let nes_root_dir = PathBuf::from("/home/x/uni/ba/standalone/nebulastream/build");
    let relative_worker_path = PathBuf::from("nes-worker/nesWorker");
    let relative_coordinator_path = PathBuf::from("nes-coordinator/nesCoordinator");
    let experiment_directory = PathBuf::from("/home/x/uni/ba/experiments");
    let simulation_config = SimulationConfig {
        nes_root_dir,
        relative_worker_path,
        relative_coordinator_path,
        experiment_directory
    };
    let nes_executable_paths = NesExecutablePaths::new(simulation_config);
    let coordinator_path = &nes_executable_paths.coordinator_path;
    let worker_path = &nes_executable_paths.worker_path;
    let shutdown_triggered = Arc::new(AtomicBool::new(false));
    let mut worker_processes = HashMap::new();
    let mut mobile_worker_processes = vec![];
    let mut coordinator_process = None;
    let s = Arc::clone(&shutdown_triggered);
    ctrlc::set_handler(move || {
        s.store(true, Ordering::SeqCst);
    }).expect("TODO: panic message");
    if let Ok(_) = start_children(&mut coordinator_process, &mut worker_processes, &mut mobile_worker_processes, coordinator_path, worker_path, Arc::clone(&shutdown_triggered)) {
        //wait for user to press ctrl c to exit
        while !shutdown_triggered.load(Ordering::SeqCst) {
            sleep(Duration::from_secs(1));
        }
    }


    kill_children(&mut coordinator_process, &mut worker_processes, &mut mobile_worker_processes);
}

fn start_children(coordinator_process: &mut Option<Child>, worker_processes: &mut HashMap<u64, ProvisionalWorkerHandle>, mobile_worker_processes: &mut Vec<Child>, coordinator_path: &Path, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>) -> std::result::Result<(), Box<dyn Error>> {
    let coordinator_config = CoordinatorConfiguration {
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
                ],
            }
        ]
    };
    let yaml_name = "coordinator_config.yaml";
    let yaml_string = serde_yaml::to_string(&coordinator_config)?;
    let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
    let mut after_round_trip = String::new();
    YamlEmitter::new(&mut after_round_trip).dump(&round_trip_yaml[0]).unwrap();
    fs::write(&yaml_name, after_round_trip).expect("TODO: panic message");
    *coordinator_process = Some(Command::new(coordinator_path)
        .arg("--restServerCorsAllowedOrigin=*")
        .arg(format!("--configPath={}", yaml_name))
        //.stdin(Stdio::inherit()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        //.stdin(Stdio::piped()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        .spawn()
        .expect("failed to execute coordinator"));

    //wait until coordinator is online
    //wait_for_coordinator(Arc::clone(&shutdown_triggered)).unwrap_or_else(|x| coordinator_process.kill().unwrap());
    wait_for_coordinator(Arc::clone(&shutdown_triggered))?;
    std::thread::sleep(time::Duration::from_secs(1));

    let json_string = std::fs::read_to_string("three_layer_topology.json")?;
    let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;
    dbg!(&topology);

    let mut next_free_port = 5000;
    let mut id_count = 60;

    wait_for_topology(Some(1), Arc::clone(&shutdown_triggered))?;
    start_fixed_location_workers(&topology, worker_processes, &worker_path, Arc::clone(&shutdown_triggered), &mut next_free_port, &mut id_count)?;

    //wait for user to press key to start mobile workers
    println!("press any key to start mobile workers");
    let input: String = text_io::read!("{}\n");
    start_mobile_workers("1h_dublin_bus_nanosec", worker_path, mobile_worker_processes, Arc::clone(&shutdown_triggered), &mut next_free_port);

    let execute_query_request = ExecuteQueryRequest {
        user_query: "Query::from(\"values\").sink(FileSinkDescriptor::create(\n  \"/tmp/test_sink\",\n  \"CSV_FORMAT\",\n  \"true\" // *\"true\"* for append, *\"false\"* for overwrite\n  ));".to_string(),
        placement: PlacementStrategyType::BottomUp,
    };
    let client = reqwest::blocking::Client::new();
    let result = client.post("http://127.0.0.1:8081/v1/nes/query/execute-query")
        .json(&execute_query_request).send()?;
    return Ok(());
}

fn kill_children(coordinator_process: &mut Option<Child>, worker_processes: &mut HashMap<u64, ProvisionalWorkerHandle>, mobile_worker_processes: &mut Vec<Child>) {
    for mut mobile_worker in mobile_worker_processes {
        println!("killing mobile worker");
        mobile_worker.kill().expect("could not kill worker");
    }
    for mut fixedWorker in worker_processes.values_mut() {
        println!("killing fixed worker");
        fixedWorker.process.kill().expect("could not kill worker");
    }
    //kill coordinator
    match coordinator_process {
        Some(p) => p.kill().unwrap(),
        None => {}
    }
    //let exit_status = coordinator_process.wait().unwrap();
    //println!("{}", exit_status.success());
}

//the id does currently not correspond to the actual worker id
fn start_fixed_location_workers(topology: &FixedTopology, worker_prcesses: &mut HashMap<u64, ProvisionalWorkerHandle>, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>, next_free_port: &mut u16, id: &mut u64) -> std::result::Result<(), Box<dyn Error>> {
    for (input_id, location) in &topology.nodes {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        let worker_config = FixedWorkerConfig {
            rpcPort: *next_free_port,
            dataPort: *next_free_port + 1,
            nodeSpatialType: "FIXED_LOCATION".to_string(),
            fieldNodeLocationCoordinates: format!("{}, {}", location[0], location[1]),
            workerId: *id,
        };
        let yaml_name = format!("fixed_worker_configs/fixed_worker{}.yaml", id);
        let yaml_string = serde_yaml::to_string(&worker_config)?;
        let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
        let mut after_round_trip = String::new();
        YamlEmitter::new(&mut after_round_trip).dump(&round_trip_yaml[0]).unwrap();
        fs::write(&yaml_name, after_round_trip).expect("TODO: panic message");
        let process = Command::new(worker_path)
            //worker_prcesses.push(Command::new(worker_path)
            //.arg(format!("--fieldNodeLocationCoordinates={},{}", location[0], location[1]))
            //.arg("--nodeSpatialType=FIXED_LOCATION")
            //.arg(format!("--parentId={}", actual_parent_id))
            .arg(format!("--configPath={}", yaml_name))
            .spawn()?;

        worker_prcesses.insert(*input_id, ProvisionalWorkerHandle {
            config: Fixed(worker_config),
            process,
            children: topology.children.get(input_id).unwrap().clone(),
        });
        *id += 1;
        *next_free_port += 2;
    }
    let client = reqwest::blocking::Client::new();
    for (input_id, worker_handle) in worker_prcesses.iter() {
        let config = match &worker_handle.config {
            Fixed(config) => { config }
            WorkerConfigType::Mobile(_) => { panic!() }
        };
        let parent_id = config.workerId;
        for child in &worker_handle.children {
            let child_handle = &worker_prcesses[child];
            let child_config = match &child_handle.config {
                Fixed(config) => { config }
                WorkerConfigType::Mobile(_) => { panic!() }
            };
            let child_id = child_config.workerId;
            let link_request = AddEdgeRequest {
                parent_id,
                child_id,
            };
            let result = client.post("http://127.0.0.1:8081/v1/nes/topology/addAsChild")
                .json(&link_request).send()?;
        }
    }
    Ok(())
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

fn start_mobile_workers(csv_directory: &str, worker_path: &Path, worker_processes: &mut Vec<Child>, shutdown_triggered: Arc<AtomicBool>, next_free_port: &mut u16) -> std::result::Result<(), Box<dyn Error>> {
    let paths = fs::read_dir(csv_directory)?;
    let source_count = 1;
    for path in paths {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        let path = path?;
        let file_name = path.file_name();
        println!("starting worker for vehicle {}", file_name.to_str().unwrap());
        let abs_path = path.path();
        let mut source_name = "values".to_owned();
        source_name.push_str(&source_count.to_string());
        let worker_config = MobileWorkerConfig {
            rpcPort: *next_free_port,
            dataPort: *next_free_port + 1,
            nodeSpatialType: "MOBILE_NODE".to_owned(),
            mobility: Mobilityconfig {
                locationProviderType: "CSV".to_owned(),
                locationProviderConfig: String::from(abs_path.to_str().unwrap()),
            },
            physicalSources: vec![
                PhysicalSource {
                    logicalSourceName: "values".to_owned(),
                    physicalSourceName: source_name,
                    Type: PhysicalSourceType::CSV_SOURCE,
                    configuration: PhysicalSourceConfiguration {
                        filePath: "/home/x/sequence2.csv".to_owned(),
                        skipHeader: true,
                        sourceGatheringInterval: 1000,
                        numberOfTuplesToProducePerBuffer: 10,
                        numberOfBuffersToProduce: 1000,
                    },
                }
            ],
        };
        *next_free_port += 2;
        let yaml_name = format!("mobile_configs/{}.yaml", file_name.to_str().unwrap());
        // let f = std::fs::OpenOptions::new()
        //     .write(true)
        //     .truncate(true)
        //     .create(true)
        //     .open(&yaml_name)?;
        //serde_yaml::to_writer(f, &worker_config)?;
        let yaml_string = serde_yaml::to_string(&worker_config)?;
        let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
        let mut after_round_trip = String::new();
        YamlEmitter::new(&mut after_round_trip).dump(&round_trip_yaml[0]).unwrap();
        //fs::write(&yaml_name, yaml_string).expect("TODO: panic message");
        fs::write(&yaml_name, after_round_trip).expect("TODO: panic message");
        worker_processes.push(Command::new(worker_path)
            //.arg("--nodeSpatialType=MOBILE_NODE")
            .arg(format!("--configPath={}", yaml_name))
            .spawn()
            .expect("failed to execute coordinator"));
    }
    Ok(())
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


fn create_input_data(
    directory_path: &str,
    id: u32,
    num_buffers: usize,
    tuples_per_buffer: usize,
) -> Result<(), Box<dyn Error>> {
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

    Ok(())
}

fn create_sequential_source_config()
//let num_rows = (experiment_runtime / gathering_interval) as usize * tuples_per_buffer * num_buffers;
