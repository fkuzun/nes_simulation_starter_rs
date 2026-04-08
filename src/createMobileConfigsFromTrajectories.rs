use std::fs;
use simulation_runner_lib::{Mobilityconfig, ReconnectPredictorType};

fn main() {

    let input_trajectories_directory = "/home/x/uni/ba/experiments/nes_experiment_input/1h_dublin_bus_nanosec";
    let mut mobility_configs = vec![];
    for trajectory_file in fs::read_dir(input_trajectories_directory).unwrap() {
        let config = Mobilityconfig {
            locationProviderConfig: trajectory_file.unwrap().path(),
            locationProviderType: "CSV".to_string(),
            reconnectPredictorType: ReconnectPredictorType::LIVE,
            precalcReconnectPath: Default::default(),
        };
        mobility_configs.push(config);
    }

    // let list = MobilityInputConfigList {
    //     worker_mobility_configs: mobility_configs,
    //     central_topology_update_list_path: None,
    // };
    // 
    // list.write_to_file(&PathBuf::from(&input_trajectories_directory).join("mobile_config_list.toml"))

}