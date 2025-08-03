
use std::collections::{btree_map, BTreeMap};
use std::error::Error;
use std::ops::Add;
use std::time;
use chrono::Duration;
use reqwest::{Url};
use serde::{Deserialize, Serialize};

use serde_with::serde_as;
use serde_with::DurationMilliSeconds;


use crate::lib_stateful::add_edges_from_list;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub enum ISQPEventAction {
    add,
    remove,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ISQPEvent {
    #[serde(rename = "parentId")]
    pub parent_id: u64,
    #[serde(rename = "childId")]
    pub child_id: u64,
    pub action: ISQPEventAction,
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug)]
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
    speedup: f64,
    url: Url,
    client: reqwest::blocking::Client,
    max_runtime: time::Duration,
}

impl REST_topology_updater {
    pub fn new(topology_updates: Vec<TopologyUpdate>, start_time: time::Duration, speedup: f64, url: Url, initial_updates: Vec<(u64, u64)>, max_runtime: time::Duration) -> Self {
        Self {
            topology_updates,
            start_time,
            speedup,
            url,
            client: reqwest::blocking::Client::new(),
            initial_updates,
            max_runtime
        }
    }

    // send a topology update to the REST API
    fn send_topology_update(&self, update: &TopologyUpdate) -> Result<(), Box<dyn Error>> {
        // print!("Sending topology update");
        let response = self.client.post(self.url.clone())
            // .json(&update.events)
            .json(&update)
            .send()?;
        // print!("Sent update, response: {:?}", response);
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
        println!("Starting central topology updates. {} updates in list", self.topology_updates.len());
        let mut actual_calls = vec![];

        let abort_time = self.start_time.add(self.max_runtime);
        for update in &self.topology_updates {
            let update_time = update.timestamp.mul_f64(self.speedup);
            let update_time = update_time.add(self.start_time);

            println!("Update time {:?}, abort time {:?}", update_time, abort_time);
            if update_time > abort_time {
                println!("Reached maximum reconnect time before last reconnect was reached. Omitting remaining reconnects in list");
                break
            }

            let mut now = time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
            while now < update_time {
                // println!("{} < {}  Waiting for next update, going to sleep for {} seconds", now.as_secs(), update_time.as_secs(), (update_time - now).as_secs());
                std::thread::sleep(update_time - now);
                now = time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH).unwrap();
            }
            if let Ok(_) = self.send_topology_update(update) {
                println!("Sent update at {:?}", now);
                actual_calls.push(now);
            } else {
                println!("failed to send update, returning");
                return actual_calls
            }
        }
        actual_calls
    }
}
