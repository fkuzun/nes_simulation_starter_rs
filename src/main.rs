use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::hash::Hasher;
use std::io::{Read, Write};
use std::os::unix::raw::{time_t, uid_t};
use std::process::{Child, Command, Stdio};
use std::{fs, sync, time};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::path::PathBuf;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;
use sync::atomic;
use yaml_rust::{YamlEmitter, YamlLoader};
use crate::FieldType::UINT64;

#[derive(Deserialize, Debug)]
struct SimulationConfig {
    nes_root_dir: PathBuf,
    relative_worker_path: PathBuf,
    relative_coordinator_path: PathBuf,
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
#[derive(Deserialize, Debug)]
struct FixedTopology {
    //todo: check if we can just make that a tuple
    nodes: HashMap<usize, Vec<f64>>,
    children: HashMap<usize, Vec<usize>>
}

#[derive(Deserialize, Debug)]
struct ActualTopology {
    edges: Vec<Edge>,
    nodes: Vec<ActualNode>
}

#[derive(Deserialize, Debug)]
struct ActualNode {
available_resources: usize,
    id: usize,
    ip_address: String,
    location: Option<Location>,
    nodeType: String
}

#[derive(Deserialize, Debug)]
struct Location {
    latitude: f64,
    longitude: f64
}

#[derive(Deserialize, Debug)]
struct Edge {
    source: usize,
    target: usize
}

#[derive(Deserialize, Debug)]
struct ConnectivityReply {
    statusCode: usize,
    success: bool
}

#[derive(Debug, Serialize, Deserialize)]
enum PhysicalSourceType {
    CSV_SOURCE
}

#[derive(Debug, Serialize, Deserialize)]
struct PhysicalSourceConfiguration {
    filePath: String,
    skipHeader: bool,
    sourceGatheringInterval: u64, //in millisec
    numberOfTuplesToProducePerBuffer: u64,
    numberOfBuffersToProduce: u64
}
#[derive(Debug, Serialize, Deserialize)]
struct PhysicalSource {
    logicalSourceName: String,
    physicalSourceName: String,
    #[serde(rename(deserialize = "type"))]
    #[serde(rename(serialize = "type"))]
    Type: PhysicalSourceType,
    configuration: PhysicalSourceConfiguration
}


#[derive(Debug, Serialize, Deserialize)]
struct WorkerConfig {
    nodeSpatialType: String,
    mobility: Mobilityconfig,
    physicalSources: Vec<PhysicalSource>
}

#[derive(Debug, Serialize, Deserialize)]
struct Mobilityconfig {
    locationProviderConfig: String,
    locationProviderType: String
}

#[derive(Debug, Serialize, Deserialize)]
enum FieldType {
    FLOAT64,
    UINT64
}
#[derive(Debug, Serialize, Deserialize)]
struct LogicalSourceField {
    name: String,
    #[serde(rename(deserialize = "type"))]
    #[serde(rename(serialize = "type"))]
    Type: FieldType
}
#[derive(Debug, Serialize, Deserialize)]
struct LogicalSource {
    logicalSourceName: String,
    fields: Vec<LogicalSourceField>
}
#[derive(Debug, Serialize, Deserialize)]
struct CoordinatorConfiguration {
    logicalSources: Vec<LogicalSource>
}

fn main() {
    //todo: read this from file
    let nes_root_dir =  PathBuf::from("/home/x/uni/ba/standalone/nebulastream/build");
    let relative_worker_path = PathBuf::from("nes-worker/nesWorker");
    let relative_coordinator_path = PathBuf::from("nes-coordinator/nesCoordinator");
    let simulation_config =  SimulationConfig {
        nes_root_dir,
        relative_worker_path,
        relative_coordinator_path
    };
    let nes_executable_paths = NesExecutablePaths::new(simulation_config);
    let coordinator_path = &nes_executable_paths.coordinator_path;
    let worker_path = &nes_executable_paths.worker_path;
    let shutdown_triggered = Arc::new(AtomicBool::new(false));
    let mut worker_processes = vec![];
    let mut mobile_worker_processes= vec![];
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

fn start_children(coordinator_process: &mut Option<Child>, worker_processes: &mut Vec<Child>, mobile_worker_processes: &mut Vec<Child>, coordinator_path: &Path, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>) -> std::result::Result<(), Box<dyn Error>> {
    let coordinator_config = CoordinatorConfiguration {
        logicalSources: vec![
            LogicalSource {
                logicalSourceName: "values".to_string(),
                fields: vec![
                    LogicalSourceField {
                        name: "id".to_string(),
                        Type: UINT64
                    },
                    LogicalSourceField {
                        name: "value".to_string(),
                        Type: UINT64
                    }
                ]

            }
        ]
    };
    let yaml_name  = "coordinator_config.yaml";
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

    wait_for_topology(Some(1), Arc::clone(&shutdown_triggered))?;
    start_fixed_location_workers(&topology, 1, 0, worker_processes, &worker_path, Arc::clone(&shutdown_triggered))?;

    //wait for user to press key to start mobile workers
    println!("press any key to start mobile workers");
    let input: String = text_io::read!("{}\n");
    start_mobile_workers("1h_dublin_bus_nanosec", worker_path, mobile_worker_processes, Arc::clone(&shutdown_triggered))
}

fn kill_children(coordinator_process: &mut Option<Child>, worker_processes: &mut Vec<Child>, mobile_worker_processes: &mut Vec<Child>) {
    for mut mobile_worker in mobile_worker_processes {
        println!("killing mobile worker");
        mobile_worker.kill().expect("could not kill worker");
    }
    for mut fixedWorker in worker_processes {
        println!("killing fixed worker");
        fixedWorker.kill().expect("could not kill worker");
    }
    //kill coordinator
    match coordinator_process {
        Some(p) => p.kill().unwrap(),
        None => {}
    }
    //let exit_status = coordinator_process.wait().unwrap();
    //println!("{}", exit_status.success());
}

fn start_fixed_location_workers(topology: &FixedTopology, actual_parent_id: usize, input_id: usize, worker_prcesses: &mut Vec<Child>, worker_path: &Path, shutdown_triggered: Arc<AtomicBool>) -> std::result::Result<(), Box<dyn Error>> {
    if shutdown_triggered.load(Ordering::SeqCst) {
        return Err(String::from("Shutdown triggered").into());
    }
     let system_id = wait_for_topology(None, Arc::clone(&shutdown_triggered))? + 1;
     let location = topology.nodes.get(&input_id).ok_or(format!("could not find node with id {}", system_id))?;
     worker_prcesses.push(Command::new(worker_path)
             .arg(format!("--fieldNodeLocationCoordinates={},{}", location[0], location[1]))
             .arg("--nodeSpatialType=FIXED_LOCATION")
             .arg(format!("--parentId={}", actual_parent_id))
             .spawn()?);
    //todo: re rely on the system always assigning ids one higher
    wait_for_topology(Some(system_id), Arc::clone(&shutdown_triggered));
    for child in topology.children.get(&input_id).ok_or("could not find child array")? {
        start_fixed_location_workers(topology, system_id, child.to_owned(), worker_prcesses, worker_path, Arc::clone(&shutdown_triggered))?;
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
                break Ok(())
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
                    break Ok(size)
                }
                println!("number of nodes not reached");
                std::thread::sleep(time::Duration::from_secs(1));
            } else {
                break Ok(size)
            }
        }
    }
}

fn start_mobile_workers(csv_directory: &str, worker_path: &Path, worker_processes: &mut Vec<Child>, shutdown_triggered: Arc<AtomicBool>) -> std::result::Result<(), Box<dyn Error>>{
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
        let worker_config = WorkerConfig {
            nodeSpatialType: "MOBILE_NODE".to_owned(),
            mobility: Mobilityconfig {
                locationProviderType: "CSV".to_owned(),
                locationProviderConfig: String::from(abs_path.to_str().unwrap())
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
                        numberOfBuffersToProduce: 1000
                    }
                }
            ]
        };
        let yaml_name  = format!("mobile_configs/{}.yaml", file_name.to_str().unwrap());
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
