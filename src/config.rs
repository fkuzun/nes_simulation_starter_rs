use std::collections::HashMap;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use relative_path::RelativePathBuf;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;
use serde_with::DurationMilliSeconds;
use serde_with::DurationNanoSeconds;
use serde_with::DurationSeconds;

use crate::MobileDeviceQuadrants::QuadrantConfig;
use crate::rest_node_relocation::TopologyUpdate;
use crate::util::create_folder_with_timestamp;

#[allow(dead_code)]
pub const INPUT_FOLDER_SUB_PATH: &str = "nes_experiment_input";
#[allow(dead_code)]
pub const INPUT_CONFIG_NAME: &str = "input_data_config.toml";
#[allow(dead_code)]
pub const PORT_RANGE: std::ops::Range<u16> = 7000..8000;

pub const JOIN_QUERY: bool = true;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ReconnectPredictorType {
    LIVE,
    PRECALCULATED,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_NONE,
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum OutputType {
    CSV,
    AVRO,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum SourceInputMethod {
    CSV,
    TCP,
}

pub fn deserialize_relative_path<'de, D>(deserializer: D) -> Result<RelativePathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let p = PathBuf::deserialize(deserializer)?;
    Ok(RelativePathBuf::from_path(p).expect("only relative paths are allowed in the config file"))
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Paths {
    #[serde(skip)]
    base_path: Option<PathBuf>,
    #[serde(deserialize_with = "deserialize_relative_path")]
    fixed_topology_nodes: RelativePathBuf,
    mobile_trajectories_directory: MobileTopologyInput,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum MobileTopologyInput {
    Quadrants(QuadrantConfig),
    #[serde(deserialize_with = "deserialize_relative_path")]
    TrajectoriesDir(RelativePathBuf),
}

impl Paths {
    pub fn get_fixed_topology_nodes_path(&self) -> PathBuf {
        self.fixed_topology_nodes
            .to_path(self.base_path.as_ref().expect("base path not set"))
    }

    pub fn get_fixed_topology_nodes_path_relative(&self) -> PathBuf {
        self.fixed_topology_nodes.to_path(".")
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
            println!(
                "cannot get path for non directory input type because quadrant method is used"
            );
            None
        }
    }

    pub fn get_mobility_config_list_path(&self) -> Option<PathBuf> {
        let option = self.get_mobile_trajectories_directory();
        if let Some(mut path) = option {
            path.push("topology_updates.json");
            Some(path)
        } else {
            None
        }
    }

    pub fn set_base_path(&mut self, base_path: PathBuf) {
        self.base_path = Some(base_path);
    }
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Parameters {
    pub enable_query_reconfiguration: bool,
    pub enable_proactive_deployment: bool,
    pub speedup_factor: f64,
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
    pub query_string: String,
    #[serde_as(as = "DurationSeconds<u64>")]
    pub reconnect_start_offset: Duration,
    pub place_default_sources_on_node_ids_path: PathBuf,
    pub num_worker_threads: u64,
    pub(crate) placementAmendmentThreadCount: u16,
    pub query_duplication_factor: usize,
    pub join_match_interval: u64,
    pub window_size: u64,
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
pub struct InputConfig {
    pub parameters: Parameters,
    pub default_source_input: DefaultSourceInput,
    pub(crate) paths: Paths,
    #[serde(default)]
    pub(crate) source_count_map: HashMap<String, u64>,
}

impl InputConfig {
    pub fn get_data_production_time(&self) -> Duration {
        self.parameters.warmup + self.parameters.reconnect_runtime + self.parameters.cooldown_time
    }
    pub fn get_total_time(&self) -> Duration {
        self.parameters.deployment_time_offset
            + self.parameters.warmup
            + self.parameters.reconnect_runtime
            + self.parameters.cooldown_time
            + self.parameters.post_cooldown_time
    }
}

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
        format!(
            "{}{}{}",
            short_name,
            self.get_short_name_value_separator(),
            value
        )
    }

    pub fn generate_input_configs(
        &self,
        number_of_runs: u64,
    ) -> Vec<(String, InputConfig, Vec<u64>)> {
        let mut configs = vec![];
        for &enable_query_reconfiguration in &self.enable_query_reconfiguration {
            for &enable_proactive_deployment in &self.enable_proactive_deployment {
                if !enable_query_reconfiguration && enable_proactive_deployment {
                    println!("skipping config with reconfiguration disabled and proactive deployment enabled");
                    continue;
                }
                for &tuples_per_buffer in &self.tuples_per_buffer {
                    for &gathering_interval in &self.gathering_interval {
                        for &speedup_factor in &self.speedup_factor {
                            for &placementAmendmentThreadCount in
                                &self.placementAmendmentThreadCount
                            {
                                if enable_query_reconfiguration
                                    && placementAmendmentThreadCount == 1
                                {
                                    println!("skipping config with reconfiguration enabled and only one amendment thread");
                                    continue;
                                }
                                let config = InputConfig {
                                    parameters: Parameters {
                                        enable_query_reconfiguration,
                                        enable_proactive_deployment,
                                        speedup_factor,
                                        placementAmendmentThreadCount,
                                        ..self.default_config.parameters.clone()
                                    },
                                    default_source_input: DefaultSourceInput {
                                        tuples_per_buffer,
                                        gathering_interval,
                                        ..self.default_config.default_source_input.clone()
                                    },
                                    ..self.default_config.clone()
                                };

                                let mut short_name = self.get_short_name_with_value(
                                    &self.get_reconfig_short_name(),
                                    &enable_query_reconfiguration.to_string(),
                                );
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(
                                    &self.get_proactive_short_name(),
                                    &enable_proactive_deployment.to_string(),
                                ));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(
                                    &self.get_tuples_per_buffer_short_name(),
                                    &tuples_per_buffer.to_string(),
                                ));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(
                                    &self.get_gathering_interval_short_name(),
                                    &gathering_interval.as_millis().to_string(),
                                ));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(
                                    &self.get_speedup_short_name(),
                                    &speedup_factor.to_string(),
                                ));
                                short_name.push_str(&self.get_short_name_to_short_name_separator());
                                short_name.push_str(&self.get_short_name_with_value(
                                    &self.get_amnenment_threads_short_name(),
                                    &placementAmendmentThreadCount.to_string(),
                                ));
                                configs.push((short_name, config, (0..number_of_runs).collect()));
                            }
                        }
                    }
                }
            }
        }
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
    pub output_type: OutputType,
}

impl SimulationConfig {
    pub fn get_analysis_script_path(&self) -> Option<PathBuf> {
        let multi_conf = MultiSimulationInputConfig::read_input_from_file(&self.input_config_path)
            .expect("could not read multi simulation config file");
        if let Some(script_path) = &multi_conf.analysis_script {
            let abs_path = script_path.to_path(
                self.input_config_path
                    .parent()
                    .expect("could not get parent path of input config file"),
            );
            if abs_path.exists() {
                Some(abs_path)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_input_config_path(&self) -> PathBuf {
        self.input_config_path.clone()
    }

    #[allow(dead_code)]
    fn read_input_config(&self) -> InputConfig {
        let input_config: InputConfig = toml::from_str(
            &*read_to_string(&self.get_input_config_path()).expect("Could not read config file"),
        )
        .expect("could not parse config file");
        input_config
    }

    pub(crate) fn read_multi_simulation_input_config(&self) -> MultiSimulationInputConfig {
        let file_path = self.get_input_config_path();
        let mut input_config: MultiSimulationInputConfig =
            toml::from_str(&read_to_string(&file_path).expect("Could not read config file"))
                .expect("could not parse config file");
        input_config.default_config.paths.set_base_path(
            file_path
                .parent()
                .expect("could not get parent path of input config file")
                .to_owned(),
        );
        input_config
    }

    pub(crate) fn create_generated_folder(&self) -> PathBuf {
        create_folder_with_timestamp(
            self.output_directory.clone(),
            self.input_config_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
        )
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
    pub(crate) mobility_base_path: Option<PathBuf>,
    #[serde(deserialize_with = "deserialize_relative_path")]
    pub(crate) locationProviderConfig: RelativePathBuf,
    pub locationProviderType: String,
    pub reconnectPredictorType: ReconnectPredictorType,
    #[serde(deserialize_with = "deserialize_relative_path")]
    pub(crate) precalcReconnectPath: RelativePathBuf,
}

impl InputMobilityconfig {
    pub fn get_location_provider_config_path(&self) -> PathBuf {
        self.locationProviderConfig.to_path(
            self.mobility_base_path
                .as_ref()
                .expect("mobility base path not set"),
        )
    }

    pub fn get_precalc_reconnect_path(&self) -> PathBuf {
        self.precalcReconnectPath.to_path(
            self.mobility_base_path
                .as_ref()
                .expect("mobility base path not set"),
        )
    }

    pub fn to_mobility_config(&self) -> Mobilityconfig {
        Mobilityconfig {
            locationProviderConfig: self.get_location_provider_config_path(),
            locationProviderType: self.locationProviderType.clone(),
            reconnectPredictorType: self.reconnectPredictorType.clone(),
            precalcReconnectPath: self.get_precalc_reconnect_path(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MobilityInputConfigList {
    pub worker_mobility_configs: Vec<InputMobilityconfig>,
    pub central_topology_update_list_path: Option<PathBuf>,
}

impl MobilityInputConfigList {
    #[allow(dead_code)]
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
pub struct PrecalculatedReconnect {
    #[serde(rename = "column1")]
    pub parent_id: u64,
    #[serde_as(as = "DurationNanoSeconds<u64>")]
    #[serde(rename = "column2")]
    pub offset: Duration,
}

#[derive(Deserialize, Debug)]
pub struct FixedTopology {
    pub nodes: HashMap<u64, Vec<f64>>,
    pub slots: HashMap<u64, u16>,
    pub children: HashMap<u64, Vec<u64>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SimulatedReconnects {
    pub initial_parents: Vec<(u64, u64)>,
    pub topology_updates: Vec<TopologyUpdate>,
}

impl SimulatedReconnects {
    pub fn get_mobility_input_config_list(&self) -> MobilityInputConfigList {
        let mut mobility_configs = vec![];
        for _initial in &self.initial_parents {
            let generated_mobility_config = InputMobilityconfig {
                mobility_base_path: None,
                locationProviderConfig: RelativePathBuf::from_path("invalid").unwrap(),
                locationProviderType: "BASE".to_owned(),
                reconnectPredictorType: ReconnectPredictorType::PRECALCULATED,
                precalcReconnectPath: RelativePathBuf::from_path("invalid").unwrap(),
            };
            mobility_configs.push(generated_mobility_config.clone());
        }
        MobilityInputConfigList {
            worker_mobility_configs: mobility_configs,
            central_topology_update_list_path: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ReconnectList {
    pub timestamps: Vec<Vec<u64>>,
}

pub fn get_reconnect_list(rest_port: u16) -> Result<ReconnectList, Box<dyn Error>> {
    let client = reqwest::blocking::Client::new();
    let result = client
        .get(format!(
            "http://localhost:{}/v1/nes/query/reconnects",
            &rest_port.to_string()
        ))
        .send()?;
    let reply: Vec<Vec<u64>> = result.json()?;
    println!("list: {:?}", reply);
    Ok(ReconnectList { timestamps: reply })
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
