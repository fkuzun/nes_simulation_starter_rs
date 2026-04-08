use std::collections::HashMap;
use std::path::Path;

use itertools::Itertools;

use crate::config::JOIN_QUERY;

/// Parse the source groups JSON file and return a map of node IDs to source names.
/// The JSON file maps u64 node IDs to vectors of u64 source IDs; this function
/// converts both to String representations.
pub fn parse_source_groups(path: &Path) -> HashMap<String, Vec<String>> {
    let content =
        std::fs::read_to_string(path).expect("Failed to read place_default_sources_on_node_ids");
    let raw: HashMap<u64, Vec<u64>> =
        serde_json::from_str(&content).expect("could not parse map of sourcees to nodes");
    raw.iter()
        .map(|(k, v)| {
            (
                k.to_string(),
                v.iter().map(|x| x.to_string()).collect(),
            )
        })
        .collect()
}

/// Build NES query strings for submission to the coordinator.
///
/// Template placeholders:
/// - {INPUT}/{INPUT1}/{INPUT2}: source names
/// - {SINK}: sink descriptor
/// - {OUTPUT}: TCP port (filled at submission time)
/// - {WINDOW_SIZE}: join window size in tuples
pub fn build_query_strings(
    source_groups_path: &Path,
    query_template: &str,
    window_size: u64,
    query_duplication_factor: usize,
) -> Vec<String> {
    let place_default_sources_on_node_ids = parse_source_groups(source_groups_path);
    let mut query_strings = vec![];

    if JOIN_QUERY {
        let mut source_count_map = HashMap::<String, u64>::new();

        for v in place_default_sources_on_node_ids.values().flatten() {
            let source_count = source_count_map.entry(v.clone()).or_insert(0);
            *source_count += 1;
        }

        for (k, c) in source_count_map.iter() {
            assert_eq!(*c % 2, 0);
            let mut joins = String::from("{");
            for i in 0..*c / 2 {
                let join_string = query_template
                    .replace("{INPUT1}", format!("{}s{}", k, i * 2 + 1).as_str())
                    .replace("{INPUT2}", format!("{}s{}", k, i * 2 + 2).as_str());
                joins.push_str(&join_string);
                if i < *c / 2 - 1 {
                    joins.push_str(", ");
                }
            }
            joins.push('}');

            let outer_query = "Query::sink2({SINK}, {JOINS});";
            let outer_query = outer_query.replace("{JOINS}", &joins);

            let input_replaced = outer_query.replace("{WINDOW_SIZE}", &window_size.to_string());
            let sink_string = format!(
                "FileSinkDescriptor::create(\"{}:{{OUTPUT}}\", \"CSV_FORMAT\", \"true\")",
                k
            );
            let tcp_sink = input_replaced.replace("{SINK}", &sink_string);
            println!("--------------");
            println!("Query: {}", tcp_sink);
            println!("--------------");
            query_strings.push(tcp_sink);
        }
    } else {
        for id in place_default_sources_on_node_ids
            .values()
            .flatten()
            .unique()
        {
            let input_replaced = query_template.replace("{INPUT}", &id.to_string());
            let sink_string = format!(
                "FileSinkDescriptor::create(\"{}:{{OUTPUT}}\", \"CSV_FORMAT\", \"true\")",
                id
            );
            let tcp_sink = input_replaced.replace("{SINK}", &sink_string);
            let null_sink =
                input_replaced.replace("{SINK}", "NullOutputSinkDescriptor::create()");
            query_strings.push(tcp_sink);
            for _i in 0..query_duplication_factor {
                query_strings.push(null_sink.clone());
            }
        }
    }
    query_strings
}
