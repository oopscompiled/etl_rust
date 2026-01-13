#[cfg(test)]
mod tests {
    use etl_rust::extract::json_lines::{check_folder, receive_all};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_check_folder_with_invalid_path() {
        let result = check_folder("/non/existent/path", false, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unable to read folder"));
    }

    #[test]
    fn test_check_folder_with_empty_folder() {
        let tmp_dir = tempdir().unwrap();
        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_single_json_file() {
        let tmp_dir = tempdir().unwrap();
        fs::write(
            tmp_dir.path().join("1.json"),
            r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#,
        )
        .unwrap();

        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_multiple_json_files() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;

        for i in 1..=4 {
            fs::write(tmp_dir.path().join(format!("file-{}.json", i)), json_data).unwrap();
        }

        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_invalid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), "{}").unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            Some("InvalidEventType".to_string()),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid event type"));
    }

    #[test]
    fn test_check_folder_with_valid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), json_data).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            Some("PushEvent".to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_dry_run_mode() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;

        for i in 1..=3 {
            fs::write(tmp_dir.path().join(format!("dry-{}.json", i)), json_data).unwrap();
        }

        let result = check_folder(tmp_dir.path().to_str().unwrap(), true, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_ignores_non_json_files() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("file.txt"), "some text").unwrap();
        fs::write(tmp_dir.path().join("file.yaml"), "key: value").unwrap();

        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None);
        assert!(result.is_ok());
    }

    // Tests for receive_all function
    #[test]
    fn test_receive_all_with_valid_json_file() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_receive_all_with_multiple_lines() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}
{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_empty_lines() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}

{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_invalid_json_line() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}
{"invalid json":
{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        // Should succeed but skip the invalid line
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let pr_event = r#"{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, format!("{}\n{}", push_event, pr_event)).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), Some("PushEvent".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_receive_all_with_non_matching_filter() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, push_event).unwrap();

        let result = receive_all(
            file_path.to_str().unwrap(),
            Some("PullRequestEvent".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_receive_all_with_nonexistent_file() {
        let result = receive_all("/nonexistent/file.json", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_receive_all_with_empty_file() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("empty.json");
        fs::write(&file_path, "").unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_receive_all_with_only_whitespace_and_empty_lines() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("whitespace.json");
        fs::write(&file_path, "\n  \n\t\n").unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }
}
