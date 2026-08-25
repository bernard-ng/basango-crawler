use super::*;

#[test]
fn extracts_open_graph_and_resolves_relative_urls() {
    let html = r#"
        <html><head>
          <meta property="og:title" content="A title">
          <meta property="og:image" content="/image.jpg">
          <link rel="canonical" href="/story">
        </head></html>
    "#;
    let base = Url::parse("https://example.com/news/page").unwrap();
    let metadata = consume_html(html, &base).unwrap();
    assert_eq!(metadata.title.as_deref(), Some("A title"));
    assert_eq!(
        metadata.image.unwrap().as_str(),
        "https://example.com/image.jpg"
    );
    assert_eq!(metadata.url.unwrap().as_str(), "https://example.com/story");
}
