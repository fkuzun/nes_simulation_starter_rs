use std::collections::HashSet;
use std::error::Error;
use std::io::{Cursor, Write};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use avro_rs::{Schema, Writer};
use byteorder::LittleEndian;
use serde::{Deserialize, Serialize};
use std::fs::File;

use crate::config::OutputType;

/// Parses a raw output tuple from the NES TCP sink. Each tuple is 14 little-endian u64 fields:
/// win_start, win_end, then left source fields (id, join_id, seq, event_time, processing_time,
/// emission_time), then right source fields.
#[derive(Debug, Serialize, Deserialize)]
pub struct OutputTuple {
    win_start: u64,
    win_end: u64,
    id_1: u64,
    join_id_1: u64,
    sequence_number_1: u64,
    event_time_1: u64,
    processing_time_1: u64,
    emission_time_1: u64,
    id_2: u64,
    join_id_2: u64,
    sequence_number_2: u64,
    event_time_2: u64,
    processing_time_2: u64,
    emission_time_2: u64,
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

pub trait OutputWriter {
    fn write(&mut self, tuple: OutputTuple) -> Result<(), Box<dyn Error>>;
    fn flush(&mut self) -> Result<(), Box<dyn Error>>;
}

pub struct AvroOutputWriter {
    file: File,
    buffer: Vec<OutputTuple>,
}

impl AvroOutputWriter {
    pub fn new(file: File) -> Self {
        Self {
            file,
            buffer: Vec::new(),
        }
    }
}

impl OutputWriter for AvroOutputWriter {
    fn write(&mut self, tuple: OutputTuple) -> Result<(), Box<dyn Error>> {
        self.buffer.push(tuple);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "experiment_output",
                "fields": [
                    {"name": "win_start", "type": "long"},
                    {"name": "win_end", "type": "long"},
                    {"name": "id_1", "type": "long"},
                    {"name": "join_id_1", "type": "long"},
                    {"name": "sequence_number_1", "type": "long"},
                    {"name": "event_time_1", "type": "long"},
                    {"name": "processing_time_1", "type": "long"},
                    {"name": "emission_time_1", "type": "long"},
                    {"name": "id_2", "type": "long"},
                    {"name": "join_id_2", "type": "long"},
                    {"name": "sequence_number_2", "type": "long"},
                    {"name": "event_time_2", "type": "long"},
                    {"name": "processing_time_2", "type": "long"},
                    {"name": "emission_time_2", "type": "long"}
                ]
            }
            "#;
        let schema = Schema::parse_str(raw_schema).unwrap();

        println!("{:?}", schema);

        let mut writer = Writer::new(&schema, Vec::new());

        for tuple in &self.buffer {
            writer.append_ser(tuple)?;
        }

        let encoded = writer.into_inner().unwrap();
        self.file.write_all(&encoded)?;
        Ok(())
    }
}

/// Receives binary output tuples from a NES TCP sink. Accumulates bytes in a polling loop,
/// then batch-processes complete tuples. Partial trailing bytes are discarded.
pub async fn handle_connection<W: ?Sized + OutputWriter>(
    stream: tokio::net::TcpStream,
    line_count: Arc<AtomicUsize>,
    desired_line_count: u64,
    desired_line_count_total: u64,
    file: Arc<Mutex<W>>,
    shutdown_triggered: Arc<AtomicBool>,
    start_time: SystemTime,
    experiment_duration: Duration,
    output_type: OutputType,
) -> Result<(), Box<dyn Error>> {
    let mut buf = vec![];
    let mut lines = 0;
    let tuple_size = std::mem::size_of::<OutputTuple>();
    loop {
        if shutdown_triggered.load(SeqCst) {
            println!("shutdown triggered, exiting tuple reader loop");
            break;
        }

        if buf.len() / tuple_size >= desired_line_count as usize {
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("Counting tuples an writing file");

    let valid_bytes = buf.len() - (buf.len() % tuple_size);
    let mut lock = file.lock().unwrap();

    let mut seen_seq_nunbers = HashSet::new();

    match output_type {
        OutputType::CSV => {
            for i in (0..valid_bytes).step_by(tuple_size) {
                line_count.fetch_add(1, Ordering::SeqCst);
                let binary_tuple = &buf[i..i + tuple_size];
                let output_tuple = OutputTuple::from_bytes(binary_tuple);
                lock.write(output_tuple)?
            }
        }
        OutputType::AVRO => {
            for i in (0..valid_bytes).step_by(tuple_size) {
                line_count.fetch_add(1, Ordering::SeqCst);
                let binary_tuple = &buf[i..i + tuple_size];

                let output_tuple = OutputTuple::from_bytes(binary_tuple);

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

                lock.write(output_tuple)?;
                lines += 1;
            }
            println!("{} lines converted to avro", lines);
        }
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
