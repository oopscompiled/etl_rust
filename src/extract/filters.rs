use crate::model::github::{EventType, GitHubEvent};
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::Write;

pub fn is_valid_event_type(event_type_str: &str) -> bool {
    matches!(
        event_type_str,
        "PushEvent"
            | "PullRequestEvent"
            | "PullRequestReviewEvent"
            | "PullRequestReviewCommentEvent"
            | "CreateEvent"
            | "DeleteEvent"
            | "IssuesEvent"
            | "IssueCommentEvent"
            | "WatchEvent"
            | "ForkEvent"
            | "ReleaseEvent"
            | "GollumEvent"
            | "MemberEvent"
            | "PublicEvent"
            | "CommitCommentEvent"
            | "DiscussionEvent"
    )
}

fn event_type_to_str(event_type: &EventType) -> &'static str {
    match event_type {
        EventType::PushEvent => "PushEvent",
        EventType::PullRequestEvent => "PullRequestEvent",
        EventType::PullRequestReviewEvent => "PullRequestReviewEvent",
        EventType::PullRequestReviewCommentEvent => "PullRequestReviewCommentEvent",
        EventType::CreateEvent => "CreateEvent",
        EventType::DeleteEvent => "DeleteEvent",
        EventType::IssuesEvent => "IssuesEvent",
        EventType::IssueCommentEvent => "IssueCommentEvent",
        EventType::WatchEvent => "WatchEvent",
        EventType::ForkEvent => "ForkEvent",
        EventType::ReleaseEvent => "ReleaseEvent",
        EventType::GollumEvent => "GollumEvent",
        EventType::MemberEvent => "MemberEvent",
        EventType::PublicEvent => "PublicEvent",
        EventType::CommitCommentEvent => "CommitCommentEvent",
        EventType::DiscussionEvent => "DiscussionEvent",
    }
}

pub fn matches_actor_filter(actor_login: &str, filter: &str) -> bool {
    actor_login.eq_ignore_ascii_case(filter)
}

pub fn should_include(event: &GitHubEvent, filter: &Option<String>) -> bool {
    match filter {
        None => true,
        Some(filter_str) => event_type_to_str(&event.event_type) == filter_str.as_str(),
    }
}

pub fn save_events(events: &[GitHubEvent], output_path: &str) -> Result<(), String> {
    let raw_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_path)
        .map_err(|e| format!("Failed to open output file: {}", e))?;

    let mut writer = BufWriter::new(raw_file);

    for event in events {
        let json_line = serde_json::to_string(event)
            .map_err(|e| format!("Failed to serialize event: {}", e))?;

        writeln!(writer, "{}", json_line).map_err(|e| format!("Failed to write to file: {}", e))?;
    }

    Ok(())
}
