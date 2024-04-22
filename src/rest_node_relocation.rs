use std::collections::hash_map::Entry;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::ops::Add;
use std::time;
use chrono::Duration;
use nes_tools::topology::{AddEdgeReply, AddEdgeRequest};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde::ser::{SerializeMap, SerializeSeq};
use serde_with::serde_as;
use serde_with::DurationMilliSeconds;
use serde_with::DurationNanoSeconds;
use serde_with::DurationSeconds;
use crate::add_edges_from_list;
use crate::MobileDeviceQuadrants::MobileDeviceQuadrants;

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum ISQPEventAction {
    add,
    remove,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ISQPEvent {
    #[serde(rename = "parentId")]
    pub parent_id: u64,
    #[serde(rename = "childId")]
    pub child_id: u64,
    pub action: ISQPEventAction,
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TopologyUpdate {
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub timestamp: time::Duration,
    #[serde(rename = "events")]
    pub events: Vec<ISQPEvent>,
}

pub struct TopologyUpdateList {
    pub events: BTreeMap<time::Duration, Vec<ISQPEvent>>,
}

impl TopologyUpdateList {
    pub fn new() -> Self {
        Self {
            events: BTreeMap::new(),
        }
    }
    pub fn add(&mut self, timestamp: time::Duration, event: ISQPEvent) {
        match self.events.entry(timestamp) {
            btree_map::Entry::Occupied(e) => {
                e.into_mut().push(event);
            }
            btree_map::Entry::Vacant(e) => {
                e.insert(vec![event]);
            }
        }
    }

    pub fn add_initial_event(&mut self, event: ISQPEvent) {
        match self.events.entry(time::Duration::new(0, 0)) {
            btree_map::Entry::Occupied(e) => {
                e.into_mut().push(event);
            }
            btree_map::Entry::Vacant(e) => {
                e.insert(vec![event]);
            }
        }
    }

    pub fn add_initial_connect(&mut self, child_id: u64, parent_id: u64) {
        self.add_initial_event(ISQPEvent {
            parent_id: parent_id,
            child_id: child_id,
            action: ISQPEventAction::add,
        });
    }

    pub fn add_reconnect(&mut self, timestamp: time::Duration, child_id: u64, old_parent_id: u64, new_parent_id: u64) {
        self.add(timestamp, ISQPEvent {
            parent_id: old_parent_id,
            child_id,
            action: ISQPEventAction::remove,
        });
        self.add(timestamp, ISQPEvent {
            parent_id: new_parent_id,
            child_id,
            action: ISQPEventAction::add,
        });
    }
}

impl From<TopologyUpdateList> for Vec<TopologyUpdate> {
    fn from(list: TopologyUpdateList) -> Self {
        let mut updates = vec![];
        for (timestamp, events) in list.events {
            updates.push(TopologyUpdate {
                timestamp,
                events,
            });
        }
        updates
    }
}

impl Default for TopologyUpdateList {
    fn default() -> Self {
        Self::new()
    }
}

pub struct REST_topology_updater {
    topology_updates: Vec<TopologyUpdate>,
    start_time: time::Duration,
    interval: time::Duration,
    url: Url,
    client: reqwest::blocking::Client,
}

impl REST_topology_updater {
    pub fn new(topology_updates: Vec<TopologyUpdate>, start_time: time::Duration, interval: time::Duration, url: Url) -> Self {
        Self {
            topology_updates,
            start_time,
            interval,
            url,
            client: reqwest::blocking::Client::new(),
        }
    }

    // send a topology update to the REST API
    fn send_topology_update(&self, update: &TopologyUpdate) {
        let response = self.client.post(self.url.clone())
            .json(&update.events)
            .send()
            .unwrap();
        assert!(response.status().is_success());
    }

    // start periodic sending of topology updates
    pub fn start(self) -> std::thread::JoinHandle<()> {
        self.perform_initial_reconnect();
        //start new thread
        std::thread::spawn(|| {
            self.run();
        })
    }

    fn perform_initial_reconnect(&self) {
        let initial_update = self.topology_updates.first().unwrap();
        let mut edges = vec![];
        for event in &initial_update.events {
            if event.action == ISQPEventAction::add {
                edges.push((
                    event.parent_id,
                    event.child_id
                ));
            }
        }
        let rest_port = self.url.port().unwrap();
        add_edges_from_list(&rest_port, &edges).expect("Could not add initial edges");
    }

    fn run(self) {
        println!("Starting central topology updates");
        for update in &self.topology_updates {
            if (update.timestamp.as_nanos() == 0) {
                continue;
            }
            let update_time = update.timestamp.add(self.start_time);
            let mut now = time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
            while now < update_time {
                println!("Waiting for next update, going to sleep");
                std::thread::sleep(update_time - now);
                now = time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
            }
            self.send_topology_update(update);
            println!("Sent update at {:?}", update_time);
        }
    }
}
