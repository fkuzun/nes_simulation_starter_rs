use std::error::Error;
use std::path::Path;
use std::time::Duration;
use std::fs;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationMilliSeconds;
use yaml_rust::{YamlEmitter, YamlLoader};

use crate::config::{LogLevel, Mobilityconfig};

/// Serialize to block-style YAML required by NES's C++ config parser.
/// The round-trip through yaml-rust converts serde_yaml's flow-style output.
pub(crate) trait YamlWritable: Serialize {
    fn write_to_file(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let yaml_string = serde_yaml::to_string(&self)?;
        let round_trip_yaml = YamlLoader::load_from_str(&yaml_string).unwrap();
        let mut after_round_trip = String::new();
        YamlEmitter::new(&mut after_round_trip)
            .dump(&round_trip_yaml[0])
            .unwrap();
        fs::write(path, after_round_trip)?;
        Ok(())
    }
}

impl YamlWritable for CoordinatorConfiguration {}
impl YamlWritable for MobileWorkerConfig {}
impl YamlWritable for FixedWorkerConfig {}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FieldType {
    FLOAT64,
    UINT64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogicalSourceField {
    pub(crate) name: String,
    #[serde(rename(deserialize = "type"))]
    #[serde(rename(serialize = "type"))]
    pub(crate) Type: FieldType,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogicalSource {
    pub(crate) logicalSourceName: String,
    pub(crate) fields: Vec<LogicalSourceField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum PhysicalSourceType {
    CSV_SOURCE,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PhysicalSourceConfiguration {
    pub(crate) filePath: String,
    pub(crate) skipHeader: bool,
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub(crate) sourceGatheringInterval: Duration,
    pub(crate) numberOfTuplesToProducePerBuffer: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PhysicalSource {
    pub(crate) logicalSourceName: String,
    pub(crate) physicalSourceName: String,
    #[serde(rename(deserialize = "type"))]
    #[serde(rename(serialize = "type"))]
    pub(crate) Type: PhysicalSourceType,
    pub(crate) configuration: PhysicalSourceConfiguration,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MobileWorkerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rpcPort: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dataPort: Option<u16>,
    pub(crate) workerId: u64,
    pub(crate) numberOfSlots: u16,
    pub(crate) nodeSpatialType: String,
    pub(crate) mobility: Mobilityconfig,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    #[serde(default)]
    pub(crate) physicalSources: Vec<PhysicalSource>,
    pub(crate) fieldNodeLocationCoordinates: String,
    pub(crate) logLevel: LogLevel,
    pub(crate) numWorkerThreads: u64,
    pub(crate) enableIncrementalPlacement: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FixedWorkerConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rpcPort: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dataPort: Option<u16>,
    pub(crate) numberOfSlots: u16,
    pub(crate) nodeSpatialType: String,
    pub(crate) fieldNodeLocationCoordinates: String,
    pub(crate) workerId: u64,
    #[serde(skip_serializing_if = "std::vec::Vec::is_empty")]
    #[serde(default)]
    pub(crate) physicalSources: Vec<PhysicalSource>,
    pub(crate) logLevel: LogLevel,
    pub(crate) numWorkerThreads: u64,
    pub(crate) enableIncrementalPlacement: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CoordinatorConfiguration {
    pub(crate) enableProactiveDeployment: bool,
    pub(crate) logicalSources: Vec<LogicalSource>,
    pub(crate) logLevel: LogLevel,
    pub(crate) optimizer: OptimizerConfiguration,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OptimizerConfiguration {
    pub(crate) enableIncrementalPlacement: bool,
    pub(crate) placementAmendmentThreadCount: u16,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(crate) struct ActualTopology {
    pub(crate) edges: Vec<Edge>,
    pub(crate) nodes: Vec<ActualNode>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(crate) struct ActualNode {
    pub(crate) available_resources: u16,
    pub(crate) id: u64,
    pub(crate) ip_address: String,
    pub(crate) location: Option<Location>,
    pub(crate) nodeType: String,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(crate) struct Location {
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(crate) struct Edge {
    pub(crate) source: u64,
    pub(crate) target: u64,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
pub(crate) struct ConnectivityReply {
    pub(crate) statusCode: u64,
    pub(crate) success: bool,
}
