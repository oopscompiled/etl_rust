use serde::Deserialize;
use std::fs;
use std::fs::File as StdFile;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Deserialize, Debug)]
pub struct Actor {
    pub id: u64,
    pub login: String,
    pub display_login: Option<String>,
    pub gravatar_id: String,
    pub url: String,
    pub avatar_url: String,
}

#[derive(Deserialize, Debug)]
pub struct Repo {
    pub id: u64,
    pub name: String,
    pub url: String,
}

#[derive(Deserialize, Debug)]
pub struct Org {
    pub id: u64,
    pub login: String,
    pub gravatar_id: String,
    pub url: String,
    pub avatar_url: String,
}

#[derive(Deserialize, Debug)]
pub enum EventType {
    PushEvent,
}

#[derive(Deserialize, Debug)]
pub struct GitHubEvent {
    pub id: String,
    pub r#type: EventType, // extract only PushEvent
    pub actor: Actor,
    pub repo: Repo,
    pub payload: serde_json::Value, // use Value since the structure cannot be determined.
    pub public: bool,
    pub created_at: String,
    pub org: Option<Org>, // Org might be missing.
}
#[derive(Deserialize, Debug)]
pub struct GitHubPayload {
    // TODO: add fields once the GitHub webhook schema is known.
}

#[derive(Deserialize)]
struct RawEvent {
    #[allow(dead_code)]
    pub id: String,
    pub r#type: String,
}

pub fn check_folder(folder_path: &str) -> Result<(), String> {
    let entries = fs::read_dir(folder_path)
        .map_err(|e| format!("Unable to read folder {}: {}", folder_path, e))?;

    let mut files: Vec<PathBuf> = Vec::new();

    for entry in entries {
        // into_iter() - slow, .iter() better, needs only &self
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

        // First check if it's a PushEvent
        let raw: RawEvent = serde_json::from_str(&line)
            .map_err(|err| format!("Error at line {}: {}", index + 1, err))?;

        if raw.r#type != "PushEvent" {
            continue;
        }

        // Now deserialize as full PushEvent
        let event: GitHubEvent = serde_json::from_str(&line).map_err(|err| {
            format!(
                "Error deserializing PushEvent at line {}: {}",
                index + 1,
                err
            )
        })?;

        results.push(event);
    }

    Ok(results)
}
fn main() {
    let path_to_data = "/Users/pacuk/etl_data";

    let start = Instant::now();

    if let Err(e) = check_folder(path_to_data) {
        eprintln!("Fatal error: {}", e);
    }

    let elapsed = start.elapsed();
    println!("⏱️ Time: {:.2?}", elapsed);
}
