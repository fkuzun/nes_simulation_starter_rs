use std::error::Error;
use std::io::{self, Write};

use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::output::{OutputTuple, OutputTupleStateless};

const BUCKET_NS: u64 = 100_000_000;

pub struct LiveLatencySink {
    sink: SinkKind,
    current: Option<BucketState>,
    late_tuples: u64,
}

enum SinkKind {
    Stdout,
    Tcp(broadcast::Sender<String>),
}

struct BucketState {
    start_ns: u64,
    count: u64,
    sum_ns: u128,
    min_ns: u64,
    max_ns: u64,
    hist: Histogram<u64>,
}

impl BucketState {
    fn new(start_ns: u64) -> Self {
        Self {
            start_ns,
            count: 0,
            sum_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            // 1 µs..60 s, 3 sig figs.
            hist: Histogram::<u64>::new_with_bounds(1_000, 60_000_000_000, 3).unwrap(),
        }
    }

    fn record(&mut self, lat_ns: u64) {
        self.count += 1;
        self.sum_ns += lat_ns as u128;
        if lat_ns < self.min_ns {
            self.min_ns = lat_ns;
        }
        if lat_ns > self.max_ns {
            self.max_ns = lat_ns;
        }
        let _ = self.hist.record(lat_ns.clamp(1_000, 60_000_000_000));
    }
}

#[derive(Serialize)]
struct LatencyFrame {
    t_ms: u64,
    count: u64,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

impl LiveLatencySink {
    pub fn new_stdout() -> Self {
        Self {
            sink: SinkKind::Stdout,
            current: None,
            late_tuples: 0,
        }
    }

    pub fn new_tcp(tx: broadcast::Sender<String>) -> Self {
        Self {
            sink: SinkKind::Tcp(tx),
            current: None,
            late_tuples: 0,
        }
    }

    pub fn record_stateful(&mut self, t: &OutputTuple) -> io::Result<()> {
        let event_ns = t.event_time_1.max(t.event_time_2);
        if t.emission_time_1 < event_ns {
            return Ok(());
        }
        let lat_ns = t.emission_time_1 - event_ns;
        self.record(t.emission_time_1, lat_ns)
    }

    pub fn record_stateless(&mut self, t: &OutputTupleStateless) -> io::Result<()> {
        if t.emission_time < t.event_time {
            return Ok(());
        }
        let lat_ns = t.emission_time - t.event_time;
        self.record(t.emission_time, lat_ns)
    }

    fn record(&mut self, emission_ns: u64, lat_ns: u64) -> io::Result<()> {
        let bucket_start = (emission_ns / BUCKET_NS) * BUCKET_NS;
        match self.current.as_mut() {
            None => {
                let mut b = BucketState::new(bucket_start);
                b.record(lat_ns);
                self.current = Some(b);
            }
            Some(b) if b.start_ns == bucket_start => b.record(lat_ns),
            Some(b) if bucket_start > b.start_ns => {
                let closed = std::mem::replace(b, BucketState::new(bucket_start));
                self.emit_frame(&closed)?;
                if let Some(nb) = self.current.as_mut() {
                    nb.record(lat_ns);
                }
            }
            Some(_) => {
                self.late_tuples += 1;
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(b) = self.current.take() {
            self.emit_frame(&b)?;
        }
        if self.late_tuples > 0 {
            eprintln!(
                "live_latency: dropped {} out-of-order tuples falling into already-flushed buckets",
                self.late_tuples
            );
        }
        Ok(())
    }

    fn emit_frame(&mut self, b: &BucketState) -> io::Result<()> {
        let frame = LatencyFrame {
            t_ms: b.start_ns / 1_000_000,
            count: b.count,
            min_ms: ns_to_ms(b.min_ns),
            p50_ms: ns_to_ms(b.hist.value_at_quantile(0.50)),
            p95_ms: ns_to_ms(b.hist.value_at_quantile(0.95)),
            p99_ms: ns_to_ms(b.hist.value_at_quantile(0.99)),
            max_ms: ns_to_ms(b.max_ns),
            mean_ms: if b.count == 0 {
                0.0
            } else {
                (b.sum_ns as f64 / b.count as f64) / 1_000_000.0
            },
        };
        let line = serde_json::to_string(&frame).expect("LatencyFrame serialization");
        match &mut self.sink {
            SinkKind::Stdout => {
                let stdout = io::stdout();
                let mut h = stdout.lock();
                h.write_all(line.as_bytes())?;
                h.write_all(b"\n")?;
            }
            SinkKind::Tcp(tx) => {
                let _ = tx.send(format!("{}\n", line));
            }
        }
        Ok(())
    }
}
