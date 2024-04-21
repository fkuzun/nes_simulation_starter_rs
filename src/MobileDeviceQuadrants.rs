use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use crate::{MobilityInputConfigList, ReconnectPredictorType};
use crate::rest_node_relocation::{ISQPEvent, TopologyUpdate};

#[derive(Debug, Serialize, Deserialize)]
struct MobileEntry {
    device_id: u64,
    sources: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MobileDeviceQuadrants {
    //quadrant_map: BTreeMap<u64, Vec<(u64, Vec<String>)>>
    quadrant_map: BTreeMap<u64, VecDeque<(MobileEntry)>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuadrantConfig {
    pub num_quadrants: usize,
    pub devices_per_quadrant: usize,
    pub quadrant_start_id: u64,
    pub mobile_start_id: u64,
}

impl QuadrantConfig {
    pub fn get_config_list(&self) -> MobilityInputConfigList {
        let mut configs = vec![];
        for _c in 0..(self.num_quadrants * self.devices_per_quadrant) {
                configs.push(
                    crate::InputMobilityconfig {
                        mobility_base_path: None,
                        locationProviderConfig: Default::default(),
                        locationProviderType: "invalid".to_string(),
                        reconnectPredictorType: ReconnectPredictorType::PRECALCULATED,
                        precalcReconnectPath: Default::default(),
                    }
                );
        }

        MobilityInputConfigList {
            worker_mobility_configs: configs,
            central_topology_update_list_path: None,
        }
    }
}

impl From<QuadrantConfig> for MobileDeviceQuadrants {
    fn from(config: QuadrantConfig) -> Self {
        Self::populate(config.num_quadrants, config.devices_per_quadrant, config.quadrant_start_id, config.mobile_start_id)
    }
}

impl MobileDeviceQuadrants {
    fn rotate_devices(&mut self) -> Vec<ISQPEvent> {
        let mut events = vec![];
        let mut moving_device: Option<(u64, MobileEntry)> = None;
        for (quadrant_id, devices) in self.quadrant_map.iter_mut().rev() {
            //for (quadrant_id, devices) in self.quadrant_map.iter_mut() {
            Self::rotate_single_device(&mut events, &mut moving_device, *quadrant_id, devices);
            if let Some(device) = devices.pop_front() {
                moving_device = Some((*quadrant_id, device));
            }
        }
        let mut entry = self.quadrant_map.last_entry().unwrap();
        Self::rotate_single_device(&mut events, &mut moving_device, *entry.key(), entry.get_mut());
        events
    }

    fn rotate_single_device(events: &mut Vec<ISQPEvent>, moving_device: &mut Option<(u64, MobileEntry)>, quadrant_id: u64, devices: &mut VecDeque<MobileEntry>) {
        if let Some((old_quadrant, device)) = moving_device.take() {
            events.push(
                ISQPEvent {
                    parent_id: old_quadrant,
                    child_id: device.device_id,
                    action: crate::rest_node_relocation::ISQPEventAction::remove,
                }
            );
            events.push(
                ISQPEvent {
                    parent_id: quadrant_id,
                    child_id: device.device_id,
                    action: crate::rest_node_relocation::ISQPEventAction::add,
                }
            );
            devices.push_back(device);
        }
    }

    fn new() -> Self {
        Self {
            quadrant_map: BTreeMap::new()
        }
    }

    fn populate(num_quadrants: usize, devices_per_qudrant: usize, quadrant_start_id: u64, mobile_start_id: u64) -> Self {
        assert!(quadrant_start_id + num_quadrants as u64 - 1 < mobile_start_id);
        let mut quadrant_map = BTreeMap::new();
        for i in 0..num_quadrants {
            let mut devices = VecDeque::new();
            for j in 0..devices_per_qudrant {
                //devices.push((mobile_start_id + i as u64 * devices_per_qudrant as u64 + j as u64, vec![]));
                devices.push_back(MobileEntry {
                    device_id: mobile_start_id + i as u64 * devices_per_qudrant as u64 + j as u64,
                    sources: vec![],
                });
            }
            quadrant_map.insert(quadrant_start_id + i as u64, devices);
        };
        Self {
            quadrant_map
        }
    }

    pub fn get_update_vector(mut self, length: usize, interval: Duration, start_offset: Duration) -> Vec<TopologyUpdate> {
        let mut updates = vec![];

        let mut timestamp = Duration::new(0, 0);
        let mut initial_events = vec![];
        //insert initial reconnects
        for (quadrant_id, devices) in self.quadrant_map.iter() {
            for device in devices {
                initial_events.push(
                    ISQPEvent {
                        parent_id: 1,
                        child_id: device.device_id,
                        action: crate::rest_node_relocation::ISQPEventAction::remove,
                    });
                initial_events.push(
                    ISQPEvent {
                        parent_id: *quadrant_id,
                        child_id: device.device_id,
                        action: crate::rest_node_relocation::ISQPEventAction::add,
                    }
                );
            }
        }
        updates.push(TopologyUpdate {
            timestamp,
            events: initial_events,
        });
        timestamp += start_offset;

        //insert reconnects
        for _ in 0..length {
            updates.push(TopologyUpdate {
                timestamp,
                events: self.rotate_devices(),
            });
            timestamp += interval;
        }
        updates
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum test {
    A(String),
    B(u64),
    C(innerTest),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct innerTest {
    x: u64,
    y: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct testContainer {
    test1: test,
    test2: test,
}

#[cfg(test)]
mod tests {
    use crate::MobileDeviceQuadrants::{test, testContainer};

    #[test]
    fn test_toml_enum() {
        let test = testContainer {
            test1: test::A("test".to_string()),
            test2: test::B(1),
        };
        let toml = toml::to_string(&test).unwrap();
        println!("{}", toml);
    }

    #[test]
    fn test_json_output() {
        let mut mdq = super::MobileDeviceQuadrants::populate(4, 3, 1, 100);
        let json = serde_json::to_string_pretty(&mdq).unwrap();
        println!("{}", json);
        let isqp_events = mdq.rotate_devices();
        let json = serde_json::to_string_pretty(&isqp_events).unwrap();
        println!("{}", json);
        let json = serde_json::to_string_pretty(&mdq).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_list() {
        let mut mdq = super::MobileDeviceQuadrants::populate(4, 3, 1, 100);
        let json = serde_json::to_string_pretty(&mdq).unwrap();
        println!("{}", json);
        let isqp_events = mdq.get_update_vector(3, std::time::Duration::new(2, 0));
        let json = serde_json::to_string_pretty(&isqp_events).unwrap();
        println!("{}", json);
    }
}