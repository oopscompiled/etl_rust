use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Actor {
    pub id: u64,
    pub login: String,
    pub display_login: Option<String>,
    pub gravatar_id: String,
    pub url: String,
    pub avatar_url: String,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Repo {
    pub id: u64,
    pub name: String,
    pub url: String,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Org {
    pub id: u64,
    pub login: String,
    pub gravatar_id: String,
    pub url: String,
    pub avatar_url: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
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
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PullRequest {
    pub url: Option<String>,
    pub id: Option<u64>,
    pub number: Option<u32>,
    pub head: Option<serde_json::Value>,
    pub base: Option<serde_json::Value>,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
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
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Comment {
    pub url: Option<String>,
    pub id: Option<u64>,
    pub body: Option<String>,
    pub user: Option<serde_json::Value>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Review {
    pub id: Option<u64>,
    pub user: Option<serde_json::Value>,
    pub body: Option<String>,
    pub state: Option<String>,
    pub submitted_at: Option<String>,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Release {
    pub id: Option<u64>,
    pub tag_name: Option<String>,
    pub name: Option<String>,
    pub body: Option<String>,
    pub draft: Option<bool>,
    pub prerelease: Option<bool>,
    pub created_at: Option<String>,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Forkee {
    pub id: Option<u64>,
    pub name: Option<String>,
    pub full_name: Option<String>,
    pub owner: Option<serde_json::Value>,
    pub description: Option<String>,
    pub url: Option<String>,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Label {
    pub id: Option<u64>,
    pub name: Option<String>,
    pub color: Option<String>,
    pub default: Option<bool>,
}
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
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
#[allow(dead_code)]
#[derive(Deserialize, Serialize, Debug, Clone)]
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
