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
#[serde(rename_all = "PascalCase")]
pub enum EventType {
    PushEvent,
    PullRequestEvent,
    PullRequestReviewEvent,
    PullRequestReviewCommentEvent,
    CreateEvent,
    DeleteEvent,
    IssuesEvent,
    IssueCommentEvent,
    WatchEvent,
    ForkEvent,
    ReleaseEvent,
    GollumEvent,
    MemberEvent,
    PublicEvent,
    CommitCommentEvent,
    DiscussionEvent,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PullRequest {
    pub url: Option<String>,
    pub id: Option<u64>,
    pub number: Option<u32>,
    pub head: Option<serde_json::Value>,
    pub base: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Issue {
    pub url: Option<String>,
    pub id: Option<u64>,
    pub number: Option<u32>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub user: Option<serde_json::Value>,
    pub state: Option<String>,
    pub assignee: Option<serde_json::Value>,
    pub assignees: Option<Vec<serde_json::Value>>,
    pub labels: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Comment {
    pub url: Option<String>,
    pub id: Option<u64>,
    pub body: Option<String>,
    pub user: Option<serde_json::Value>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Review {
    pub id: Option<u64>,
    pub user: Option<serde_json::Value>,
    pub body: Option<String>,
    pub state: Option<String>,
    pub submitted_at: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Release {
    pub id: Option<u64>,
    pub tag_name: Option<String>,
    pub name: Option<String>,
    pub body: Option<String>,
    pub draft: Option<bool>,
    pub prerelease: Option<bool>,
    pub created_at: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Forkee {
    pub id: Option<u64>,
    pub name: Option<String>,
    pub full_name: Option<String>,
    pub owner: Option<serde_json::Value>,
    pub description: Option<String>,
    pub url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Label {
    pub id: Option<u64>,
    pub name: Option<String>,
    pub color: Option<String>,
    pub default: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct GitHubPayload {
    pub action: Option<String>,
    pub ref_type: Option<String>,
    pub r#ref: Option<String>,
    pub full_ref: Option<String>,
    pub pusher_type: Option<String>,
    pub master_branch: Option<String>,
    pub description: Option<String>,
    pub repository_id: Option<u64>,
    pub push_id: Option<u64>,
    pub head: Option<String>,
    pub before: Option<String>,
    pub number: Option<u32>,

    // Связанные объекты
    pub pull_request: Option<PullRequest>,
    pub issue: Option<Issue>,
    pub comment: Option<Comment>,
    pub review: Option<Review>,
    pub release: Option<Release>,
    pub forkee: Option<Forkee>,
    pub label: Option<Label>,
    pub assignee: Option<serde_json::Value>,
    pub assignees: Option<Vec<serde_json::Value>>,
    pub labels: Option<Vec<serde_json::Value>>,
    pub member: Option<serde_json::Value>,
    pub pages: Option<Vec<serde_json::Value>>,
    pub discussion: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
pub struct GitHubEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: GitHubPayload,
    pub public: bool,
    pub created_at: String,
    pub org: Option<Org>,
}

// #[derive(Deserialize)]
// struct RawEvent {
//     #[allow(dead_code)]
//     pub id: String,
//     #[serde(rename = "type")]
//     pub event_type: String,
// }

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

fn main() {
    let path_to_data = "etl_data";
    let start = Instant::now();

    if let Err(e) = check_folder(path_to_data) {
        eprintln!("Fatal error: {}", e);
    }

    let elapsed = start.elapsed();
    println!("⏱️ Time: {:.2?}", elapsed);
}
