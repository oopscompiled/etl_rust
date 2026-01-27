use crate::model::github::GitHubEvent;
use std::collections::HashMap;

pub fn count_events(events: &[GitHubEvent]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();

    for event in events {
        let type_name = serde_json::to_value(&event.event_type)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "UnknownEvent".to_string());

        *counts.entry(type_name).or_insert(0) += 1;
    }

    counts
}

pub fn print_stats(counts: &HashMap<String, usize>) {
    println!("\n{:=^40}", " EVENT ANALYTICS ");

    let mut sorted_counts: Vec<_> = counts.iter().collect();

    sorted_counts.sort_by(|a, b| b.1.cmp(a.1));

    for (event_type, count) in sorted_counts {
        println!("{:<30} | {:>7}", event_type, count);
    }

    let total: usize = counts.values().sum();
    println!("{:-^40}", "");
    println!("{:<30} | {:>7}", "TOTAL", total);
    println!("{:=^40}\n", "");
}
