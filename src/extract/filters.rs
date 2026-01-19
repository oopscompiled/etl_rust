use crate::model::github::GitHubEvent;
use std::fs::OpenOptions;
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

pub fn matches_actor_filter(actor_login: &str, filter: &str) -> bool {
    actor_login.eq_ignore_ascii_case(filter)
}

pub fn should_include(event: &GitHubEvent, filter: &Option<String>) -> bool {
    match filter {
        None => true,
        Some(filter_str) => {
            let event_type_str = format!("{:?}", event.event_type);
            event_type_str == *filter_str
        }
    }
}

pub fn save_events(events: &[GitHubEvent], output_path: &str) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .append(true)
        .open(output_path)
        .map_err(|e| format!("Failed to open output file: {}", e))?;

    for event in events {
        let json_line = serde_json::to_string(event)
            .map_err(|e| format!("Failed to serialize event: {}", e))?;
        writeln!(file, "{}", json_line).map_err(|e| format!("Failed to write to file: {}", e))?;
    }

    Ok(())
}
