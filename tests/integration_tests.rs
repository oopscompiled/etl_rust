use etl_rust::{Config, run};

#[test]
fn test_run_with_invalid_path() {
    let config = Config {
        path_to_data: String::from("/non/existent/path"),
    };

    let result = run(config);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unable to read folder"));
}
