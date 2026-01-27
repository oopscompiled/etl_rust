use crate::extract::analysis;
use crate::extract::filters::{is_valid_event_type, save_events, should_include};
use crate::model::github::GitHubEvent;
use rayon::prelude::*;
use std::fs;
use std::fs::File as StdFile;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

pub fn check_folder(
    folder_path: &str,
    dry_run: bool,
    show_stats: bool,
    event_filter: Option<String>,
    output_file: Option<String>,
) -> Result<(), String> {
    if let Some(filter) = &event_filter {
        if !is_valid_event_type(filter) {
            return Err(format!(
                "Invalid event type: '{}'. Valid types are: PushEvent, PullRequestEvent, PullRequestReviewEvent, PullRequestReviewCommentEvent, CreateEvent, DeleteEvent, IssuesEvent, IssueCommentEvent, WatchEvent, ForkEvent, ReleaseEvent, GollumEvent, MemberEvent, PublicEvent, CommitCommentEvent, DiscussionEvent",
                filter
            ));
        }
    }

    let start_total = Instant::now();
    let entries = fs::read_dir(folder_path)
        .map_err(|e| format!("Unable to read folder {}: {}", folder_path, e))?;

    let mut files: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            files.push(path);
        }
    }

    files.sort_by_key(|path| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.split('-').next_back())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0)
    });

    if let Some(output) = &output_file {
        fs::write(output, "").map_err(|e| format!("Failed to create output file: {}", e))?;
    }

    let total_files = files.len();

    if dry_run {
        let mut total_lines = 0usize;
        for path in &files {
            let file_name = path.file_name().unwrap_or_default();
            let line_count = fs::read_to_string(path)
                .map(|s| s.lines().count())
                .unwrap_or(0);
            total_lines += line_count;
            println!(
                "[Dry-run]: Would process file: {:?}, {} lines",
                file_name, line_count
            );
        }
        println!("-------------------------------------------------");
        println!("Summary (Dry-run):");
        println!("Total files: {}", total_files);
        println!("Total lines/events: {}", total_lines);
        if let Some(ref filter) = event_filter {
            println!("Filter applied: {}", filter);
        }
        println!("Total time: {:.2?}", start_total.elapsed());
        Ok(())
    } else {
        let all_events: Vec<GitHubEvent> = files
            .par_iter()
            .filter_map(|path| {
                let file_name = path.file_name().unwrap_or_default();
                println!("File processing: {:?}", file_name);

                let path_str = path.to_str()?;
                match receive_all(path_str, event_filter.clone()) {
                    Ok(events) => {
                        println!(" -> Success: {} events", events.len());
                        Some(events)
                    }
                    Err(e) => {
                        eprintln!(" -> Error in file {:?}: {}", file_name, e);
                        None
                    }
                }
            })
            .flatten()
            .collect();

        let total_lines = all_events.len();

        if show_stats && !all_events.is_empty() {
            let stats = analysis::count_events(&all_events);
            analysis::print_stats(&stats);
        }

        if let Some(output) = &output_file {
            if let Err(e) = save_events(&all_events, output) {
                eprintln!("Warning: Failed to save events to {}: {}", output, e);
            }
        }

        println!("-------------------------------------------------");
        println!("Summary:");
        println!("Total files: {}", total_files);
        println!("Total events processed: {}", total_lines);
        if let Some(ref filter) = event_filter {
            println!("Filter applied: {}", filter);
        }
        if let Some(ref output) = output_file {
            println!("Output saved to: {}", output);
        }
        println!("Total time: {:.2?}", start_total.elapsed());
        Ok(())
    }
}

pub fn receive_all(
    file_path: &str,
    event_filter: Option<String>,
) -> Result<Vec<GitHubEvent>, String> {
    let file = StdFile::open(file_path).map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);
    let mut results = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<GitHubEvent>(&line) {
            Ok(event) => {
                if should_include(&event, &event_filter) {
                    results.push(event);
                }
            }
            Err(err) => {
                eprintln!("Warning at line {}: {}", index + 1, err);
            }
        }
    }
    Ok(results)
}
