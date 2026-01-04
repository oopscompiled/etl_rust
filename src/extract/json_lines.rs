use crate::model::github::GitHubEvent;
use std::fs;
use std::fs::File as StdFile;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub fn check_folder(folder_path: &str) -> Result<(), String> {
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
            .and_then(|s| s.split('-').last())
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0)
    });

    for path in files {
        println!(
            "File processing: {:?}",
            path.file_name().unwrap_or_default()
        );
        if let Some(path_str) = path.to_str() {
            match receive_all(path_str) {
                Ok(events) => println!(" -> Success: {} events", events.len()),
                Err(e) => eprintln!(" -> Error in file {:?}: {}", path, e),
            }
        }
    }
    Ok(())
}

pub fn receive_all(file_path: &str) -> Result<Vec<GitHubEvent>, String> {
    let file = StdFile::open(file_path).map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);
    let mut results = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<GitHubEvent>(&line) {
            Ok(event) => results.push(event),
            Err(err) => {
                eprintln!("Warning at line {}: {}", index + 1, err);
            }
        }
    }

    Ok(results)
}
