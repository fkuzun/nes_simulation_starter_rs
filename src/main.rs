use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::hash::Hasher;
use std::io::{Read, Write};
use std::os::unix::raw::{time_t, uid_t};
use std::process::{Child, Command, Stdio};
use std::{fs, time};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::Result;

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
struct WorkerConfig {
    nodeSpatialType: String,
    mobility: Mobilityconfig
}

#[derive(Debug, Serialize, Deserialize)]
struct Mobilityconfig {
    locationProviderConfig: String,
    locationProviderType: String
}

fn main() {
    //todo: use config struct here
    let nes_build_directory = "/home/squirrel/nes_standalone/nebulastream/build/";
    let coordinator_path = nes_build_directory.to_owned() + "nes-coordinator/nesCoordinator";
    let worker_path = nes_build_directory.to_owned() + "nes-worker/nesWorker";
    let mut coordinator_process = Command::new(coordinator_path)
        .arg("--restServerCorsAllowedOrigin=http://localhost:3000")
        //.stdin(Stdio::inherit()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        //.stdin(Stdio::piped()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        .spawn()
        .expect("failed to execute coordinator");

    //wait until coordinator is online
    wait_for_coordinator().unwrap_or_else(|x| coordinator_process.kill().unwrap());
    std::thread::sleep(time::Duration::from_secs(1));

    let json_string = std::fs::read_to_string("three_layer_topology.json").unwrap();
    let topology: FixedTopology = serde_json::from_str(json_string.as_str()).unwrap();
    dbg!(&topology);

    wait_for_topology(Some(1)).unwrap();
    let mut worker_processes = vec![];
    start_fixed_location_workers(&topology, 1, 0, &mut worker_processes, &worker_path);

    //wait for user to press key to start mobile workers
    println!("press any key to start mobile workers");
    let input: String = text_io::read!("{}\n");
    start_mobile_workers("1h_dublin_bus_nanosec", worker_path.as_str()).unwrap();

    //wait for user to press key to exit
    let input: String = text_io::read!("{}\n");

    //kill coordinator
    let exit_status = coordinator_process.wait().unwrap();
    println!("{}", exit_status.success());
}

fn start_fixed_location_workers(topology: &FixedTopology, actual_parent_id: usize, input_id: usize, worker_prcesses: &mut Vec<Child>, worker_path: &str) -> std::result::Result<(), Box<dyn Error>> {
     //let system_id = parent_system_id + 1;
     let system_id = wait_for_topology(None)? + 1;
     let location = topology.nodes.get(&input_id).ok_or(format!("could not find node with id {}", system_id))?;
     worker_prcesses.push(Command::new(worker_path)
             .arg(format!("--fieldNodeLocationCoordinates={},{}", location[0], location[1]))
             .arg("--nodeSpatialType=FIXED_LOCATION")
             .arg(format!("--parentId={}", actual_parent_id))
             .spawn()
             .expect("failed to execute coordinator"));
    //todo: re rely on the system always assigning ids one higher
    wait_for_topology(Some(system_id));
    for child in topology.children.get(&input_id).ok_or("could not find child array")? {
        start_fixed_location_workers(topology, system_id, child.to_owned(), worker_prcesses, worker_path)?;
    }
    Ok(())
}

fn wait_for_coordinator() -> std::result::Result<(), Box<dyn Error>> {
    loop {
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

fn wait_for_topology(expected_node_count: Option<usize>) -> std::result::Result<usize, Box<dyn Error>> {
    loop {
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

fn start_mobile_workers(csv_directory: &str, worker_path: &str) -> std::result::Result<(), Box<dyn Error>>{
    let paths = fs::read_dir(csv_directory)?;
    let mut worker_processes = vec![];
    for path in paths {
        let path = path?;
        let file_name = path.file_name();
        println!("starting worker for vehicle {}", file_name.to_str().unwrap());
        let abs_path = path.path();
        let workerConfig = WorkerConfig {
            nodeSpatialType: "MOBILE_NODE".to_owned(),
            mobility: Mobilityconfig {
                locationProviderType: "CSV".to_owned(),
                locationProviderConfig: String::from(abs_path.to_str().unwrap())
            }
        };
        let yaml_name  = format!("mobile_configs/{}.yaml", file_name.to_str().unwrap());
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&yaml_name)?;
        serde_yaml::to_writer(f, &workerConfig)?;
        worker_processes.push(Command::new(worker_path)
            //.arg("--nodeSpatialType=MOBILE_NODE")
            .arg(format!("--configPath={}", yaml_name))
            .spawn()
            .expect("failed to execute coordinator"));

    }
    Ok(())
}
