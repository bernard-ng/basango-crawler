use super::*;

#[test]
fn missing_external_path_borrows_the_bundled_configuration() {
    assert!(matches!(read(None).unwrap(), Cow::Borrowed(_)));
}

#[test]
fn structural_decode_allows_environment_to_complete_secrets() {
    let raw = r#"{
        "ingestion": { "endpoint": "https://api.example.com" },
        "sources": [{ "kind": "wordpress", "id": "example", "url": "https://example.com" }]
    }"#;

    assert!(decode(raw).is_ok());
    assert!(parse(raw).is_err());
}
