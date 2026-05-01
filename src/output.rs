use std::collections::HashSet;
use std::error::Error;
use std::io::Cursor;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use byteorder::LittleEndian;
use serde::{Deserialize, Serialize};

use crate::config::{ExperimentType, OutputType};
use crate::live_latency::LiveLatencySink;

/// Parses a raw output tuple from the NES TCP sink. Each tuple is 14 little-endian u64 fields:
/// win_start, win_end, then left source fields (id, join_id, seq, event_time, processing_time,
/// emission_time), then right source fields.
#[derive(Debug, Serialize, Deserialize)]
pub struct OutputTuple {
    pub(crate) win_start: u64,
    pub(crate) win_end: u64,
    pub(crate) id_1: u64,
    pub(crate) join_id_1: u64,
    pub(crate) sequence_number_1: u64,
    pub(crate) event_time_1: u64,
    pub(crate) processing_time_1: u64,
    pub(crate) emission_time_1: u64,
    pub(crate) id_2: u64,
    pub(crate) join_id_2: u64,
    pub(crate) sequence_number_2: u64,
    pub(crate) event_time_2: u64,
    pub(crate) processing_time_2: u64,
    pub(crate) emission_time_2: u64,
}

impl OutputTuple {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        let win_start = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let win_end = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let id_1 = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let join_id_1 = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let sequence_number_1 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let event_time_1 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let processing_time_1 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let emission_time_1 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let id_2 = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let join_id_2 = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let sequence_number_2 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let event_time_2 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let processing_time_2 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let emission_time_2 =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        Self {
            win_start,
            win_end,
            id_1,
            join_id_1,
            sequence_number_1,
            event_time_1,
            processing_time_1,
            emission_time_1,
            id_2,
            join_id_2,
            sequence_number_2,
            event_time_2,
            processing_time_2,
            emission_time_2,
        }
    }
}

/// Stateless output tuple: id, sequence_number, event_time, processing_time, emission_time
/// (5 little-endian u64 fields, 40 bytes total).
#[derive(Debug, Serialize, Deserialize)]
pub struct OutputTupleStateless {
    pub(crate) id: u64,
    pub(crate) sequence_number: u64,
    pub(crate) event_time: u64,
    pub(crate) processing_time: u64,
    pub(crate) emission_time: u64,
}

impl OutputTupleStateless {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        let id = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let sequence_number =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let event_time = byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let processing_time =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        let emission_time =
            byteorder::ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
        Self {
            id,
            sequence_number,
            event_time,
            processing_time,
            emission_time,
        }
    }
}

/// Receives binary output tuples from a NES TCP sink, parses them as they arrive,
/// computes end-to-end latency, and feeds the shared `LiveLatencySink` for
/// 100 ms-bucketed pre-aggregation. Partial trailing bytes are retained between
/// poll iterations until a full tuple's worth has accumulated.
pub async fn handle_connection(
    stream: tokio::net::TcpStream,
    line_count: Arc<AtomicUsize>,
    desired_line_count: u64,
    desired_line_count_total: u64,
    sink: Arc<Mutex<LiveLatencySink>>,
    shutdown_triggered: Arc<AtomicBool>,
    start_time: SystemTime,
    experiment_duration: Duration,
    output_type: OutputType,
    experiment_type: ExperimentType,
) -> Result<(), Box<dyn Error>> {
    let mut buf = vec![];
    let mut consumed = 0usize;
    let tuple_size = if experiment_type.is_stateful() {
        std::mem::size_of::<OutputTuple>()
    } else {
        std::mem::size_of::<OutputTupleStateless>()
    };
    let mut lines: u64 = 0;
    let mut seen_seq_nunbers = HashSet::new();

    loop {
        if shutdown_triggered.load(SeqCst) {
            println!("shutdown triggered, exiting tuple reader loop");
            break;
        }

        if lines >= desired_line_count {
            println!("All tuples received for thread, exiting tuple reader loop");
            break;
        }

        let current_time = SystemTime::now();
        if let Ok(elapsed_time) = current_time.duration_since(start_time) {
            if elapsed_time > experiment_duration {
                println!("Timeout reached, exiting tuple reader loop");
                break;
            }
        }

        if let Ok(_bytes_read) = stream.try_read_buf(&mut buf) {}

        let available = buf.len() - consumed;
        let complete = available / tuple_size;
        if complete > 0 {
            let mut sink_lock = sink.lock().unwrap();
            for _ in 0..complete {
                line_count.fetch_add(1, Ordering::SeqCst);
                let slice = &buf[consumed..consumed + tuple_size];
                consumed += tuple_size;
                lines += 1;

                if experiment_type.is_stateful() {
                    let output_tuple = OutputTuple::from_bytes(slice);
                    if matches!(output_type, OutputType::AVRO) {
                        // Sanity checks: sequence numbers must match across join sides,
                        // join_id = seq_nr * 1000 (encoding from tcp_input_server),
                        // source IDs must differ (no self-joins).
                        assert_eq!(output_tuple.sequence_number_1, output_tuple.sequence_number_2);
                        assert_eq!(output_tuple.join_id_1, output_tuple.sequence_number_1 * 1000);
                        assert_eq!(output_tuple.join_id_2, output_tuple.sequence_number_2 * 1000);
                        assert_ne!(output_tuple.id_1, output_tuple.id_2);
                        let seen_check = (output_tuple.id_1, output_tuple.sequence_number_1);
                        if seen_seq_nunbers.contains(&seen_check) {
                            println!(
                                "Duplicate sequence number found: {} tuple count {}",
                                output_tuple.sequence_number_1, lines
                            );
                        }
                        seen_seq_nunbers.insert(seen_check);
                    }
                    sink_lock.record_stateful(&output_tuple)?;
                } else {
                    let output_tuple = OutputTupleStateless::from_bytes(slice);
                    sink_lock.record_stateless(&output_tuple)?;
                }
            }
            // Reclaim parsed bytes so the buffer doesn't grow unboundedly.
            if consumed >= 1 << 20 {
                buf.drain(..consumed);
                consumed = 0;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!(
        "Received {} lines of {} ({} of total {})",
        lines,
        desired_line_count,
        line_count.load(SeqCst),
        desired_line_count_total,
    );

    Ok(())
}
