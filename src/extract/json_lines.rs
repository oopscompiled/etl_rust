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
    quiet_mode: bool,
) -> Result<(), String> {
    if let Some(filter) = &event_filter {
        if !is_valid_event_type(filter) {
            return Err(format!(
                "Invalid event type: '{}'. Valid types are: PushEvent, PullRequestEvent, PullRequestReviewEvent, PullRequestReviewCommentEvent, CreateEvent, DeleteEvent, IssuesEvent, IssueCommentEvent, WatchEvent, ForkEvent, ReleaseEvent, GollumEvent, MemberEvent, PublicEvent, CommitCommentEvent, DiscussionEvent",
                filter
            ));
        }
    }

    // let start_total = Instant::now();
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

    // let total_files = files.len();

    if dry_run {
        execute_dry_run(&files, &event_filter, quiet_mode)
    } else {
        execute_normal_run(&files, &event_filter, show_stats, &output_file, quiet_mode)
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

fn execute_dry_run(
    files: &[PathBuf],
    event_filter: &Option<String>,
    quiet_mode: bool,
) -> Result<(), String> {
    let start = Instant::now();
    let mut total_lines = 0usize;

    for path in files {
        let line_count = fs::read_to_string(path)
            .map(|s| s.lines().count())
            .unwrap_or(0);
        total_lines += line_count;

        if !quiet_mode {
            println!(
                "[Dry-run]: Would process file: {:?}, {} lines",
                path.file_name().unwrap_or_default(),
                line_count
            );
        }
    }

    print_summary_dry_run(files.len(), total_lines, event_filter, start, quiet_mode);
    Ok(())
}

fn execute_normal_run(
    files: &[PathBuf],
    event_filter: &Option<String>,
    show_stats: bool,
    output_file: &Option<String>,
    quiet_mode: bool,
) -> Result<(), String> {
    let start_total = Instant::now();
    let total_files = files.len();

    let all_events: Vec<GitHubEvent> = files
        .par_iter()
        .filter_map(|path| {
            let file_name = path.file_name().unwrap_or_default();

            if !quiet_mode {
                println!("File processing: {:?}", file_name);
            }

            let path_str = path.to_str()?;
            match receive_all(path_str, event_filter.clone()) {
                Ok(events) => {
                    if !quiet_mode {
                        println!(" -> Success: {} events", events.len());
                    }
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

    if let Some(output) = output_file {
        if let Err(e) = save_events(&all_events, output) {
            eprintln!("Warning: Failed to save events to {}: {}", output, e);
        }
    }

    print_summary_normal_run(
        total_files,
        total_lines,
        event_filter,
        output_file,
        start_total,
        quiet_mode,
    );

    Ok(())
}

fn print_summary_normal_run(
    total_files: usize,
    total_lines: usize,
    event_filter: &Option<String>,
    output_file: &Option<String>,
    elapsed: Instant,
    quiet_mode: bool,
) {
    if quiet_mode {
        return;
    }

    println!("-------------------------------------------------");
    println!("Summary:");
    println!("Total files: {}", total_files);
    println!("Total events processed: {}", total_lines);

    if let Some(filter) = event_filter {
        println!("Filter applied: {}", filter);
    }

    if let Some(output) = output_file {
        println!("Output saved to: {}", output);
    }

    println!("Total time: {:.2?}", elapsed.elapsed());
}

fn print_summary_dry_run(
    total_files: usize,
    total_lines: usize,
    event_filter: &Option<String>,
    elapsed: Instant,
    quiet_mode: bool,
) {
    if quiet_mode {
        return;
    }
    println!("-------------------------------------------------");
    println!("Summary (Dry-run):");
    println!("Total files: {}", total_files);
    println!("Total lines/events: {}", total_lines);

    if let Some(filter) = event_filter {
        println!("Filter applied: {}", filter);
    }
    println!("Total time: {:.2?}", elapsed);
}
