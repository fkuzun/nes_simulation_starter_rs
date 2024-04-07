use std::env;
use std::time::Duration;
use rand::Rng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 6 || args.len() > 7 {
        eprintln!("Usage: {} <min_id> <max_id> <output_file> <experiment_duration> <interval> <amount_of_fixed_nodes_to_place_sources (optional)>", args[0]);
        std::process::exit(1);
    }
    let min_id = args[1].parse::<u64>().expect("Invalid duration");
    //let min_id = 12;
    let max_id = args[2].parse::<u64>().expect("Invalid duration");
    //let max_id = 110;
    let output_file = &args[3][..];
    let duration = Duration::from_millis(args[4].parse::<u64>().expect("Invalid duration"));
    let interval = Duration::from_millis(args[5].parse::<u64>().expect("Invalid interval"));

    // let json_string = std::fs::read_to_string(&self.paths.fixed_topology_nodes)?;
    // let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;

    let mut rng = rand::thread_rng();
    // let fixed_nodes_hosting_sources = if args.len() == 7 {
    //     let source_count = args[6].parse::<u64>().expect("Invalid duration");
    //     let mut numbers: Vec<u64> = vec![];
    //     for _i in 0..10 {
    //         let mut number = rng.gen_range(min_id..max_id);
    //         while numbers.contains(&number) {
    //             number = rng.gen_range(min_id..max_id);
    //         }
    //         numbers.push(number);
    //     }
    //     println!("{:?}", numbers);
    //     numbers
    // } else {
    //     vec![]
    // };
    let fixed_nodes_hosting_sources =  [7, 9, 11, 13, 15, 17, 19, 21, 23, 25];

    let mut timestamp = Duration::new(0, 0);
    let mut csv_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&output_file).unwrap();
    let mut parent= 0;
    while timestamp < duration {
        parent = loop {
            let generated_parent = rng.gen_range(min_id..max_id);
            if generated_parent != parent && !fixed_nodes_hosting_sources.contains(&generated_parent) {
                break generated_parent;
            }
        };
        csv_writer.serialize((parent, timestamp.as_nanos()))?;
        timestamp += interval;
    }
    Ok(())

}