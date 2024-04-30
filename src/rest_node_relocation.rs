
use std::collections::{btree_map, BTreeMap};
use std::error::Error;
use std::ops::Add;
use std::time;


use reqwest::{Url};
use serde::{Deserialize, Serialize};

use serde_with::serde_as;
use serde_with::DurationMilliSeconds;


use crate::add_edges_from_list;


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
    initial_updates: Vec<(u64, u64)>,
    start_time: time::Duration,
    interval: time::Duration,
    url: Url,
    client: reqwest::blocking::Client,
}

impl REST_topology_updater {
    pub fn new(topology_updates: Vec<TopologyUpdate>, start_time: time::Duration, interval: time::Duration, url: Url, initial_updates: Vec<(u64, u64)>) -> Self {
        Self {
            topology_updates,
            start_time,
            interval,
            url,
            client: reqwest::blocking::Client::new(),
            initial_updates,
        }
    }

    // send a topology update to the REST API
    fn send_topology_update(&self, update: &TopologyUpdate) -> Result<(), Box<dyn Error>> {
        let response = self.client.post(self.url.clone())
            .json(&update.events)
            .send()?;
        if response.status().is_success() {
            Ok(())
        } else {
            //return error
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Failed to send topology update")))
        }
    }

    // start periodic sending of topology updates
    pub fn start(self) -> Result<std::thread::JoinHandle<Vec<time::Duration>>, Box<dyn Error>> {
        self.perform_initial_reconnect()?;
        //start new thread
        Ok(
        std::thread::spawn(|| {
            self.run()
        }))
    }

    fn perform_initial_reconnect(&self) -> Result<(), Box<dyn Error>> {
        //let initial_update = self.topology_updates.first().unwrap();
        let rest_port = self.url.port().unwrap();
        println!("Adding initial mobile edges");
        add_edges_from_list(&rest_port, &self.initial_updates)
    }

    fn run(self) -> Vec<time::Duration> {
        println!("Starting central topology updates");
        let mut actual_calls = vec![];
        for update in &self.topology_updates {
            //if (update.timestamp.as_nanos() == 0) {
            //    continue;
            //}
            let update_time = update.timestamp.add(self.start_time);
            let mut now = time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
            while now < update_time {
                //println!("Waiting for next update, going to sleep");
                std::thread::sleep(update_time - now);
                now = time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
            }
            if let Ok(_) = self.send_topology_update(update) {
                actual_calls.push(now);
            } else {
                return actual_calls
            }
            // println!("Sent update at {:?}", now);
        }
        actual_calls
    }
}
