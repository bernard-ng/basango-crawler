use crate::config::{CommonSourceConfig, HtmlSelectors};

use super::*;

fn source() -> HtmlSourceConfig {
    HtmlSourceConfig {
        common: CommonSourceConfig {
            id: crate::domain::SourceId::new("example").unwrap(),
            url: Url::parse("https://example.com").unwrap(),
            ..CommonSourceConfig::default()
        },
        fetch_details: false,
        indexed_categories: Vec::new(),
        pagination_template: "news/{page}".into(),
        selectors: HtmlSelectors {
            body: ".body".into(),
            categories: Some(".category".into()),
            date: "time".into(),
            link: "a".into(),
            list: ".article".into(),
            title: "h1".into(),
            pagination: ".pages a".into(),
        },
    }
}

#[test]
fn parses_an_html_article_without_network_access() {
    let http = HttpClient::new(&Default::default()).unwrap();
    let crawler = HtmlCrawler::new(source(), http);
    let html = r#"
        <html><head><meta property="og:title" content="Metadata title"></head>
        <body><h1>Article title</h1><time datetime="2025-02-01T12:00:00Z"></time>
        <div class="body"><p>Hello <strong>world</strong></p></div>
        <span class="category">Politics</span></body></html>
    "#;
    let url = Url::parse("https://example.com/story").unwrap();
    let article = crawler.parse_article(html, Some(&url), None).unwrap();
    assert_eq!(article.title, "Article title");
    assert!(article.body.contains("Hello"));
    assert_eq!(article.categories, vec!["politics"]);
}

#[test]
fn parses_mediacongo_semantic_publication_date() {
    let mut source = source();
    source.common.date_format = "yyyy-LL-dd".into();
    source.selectors.date = "span[itemprop=\"datePublished\"]".into();
    let crawler = HtmlCrawler::new(source, HttpClient::new(&Default::default()).unwrap());
    let html = r#"
        <html><body>
        <h1>Kongo Central : 11 morts dans un nouveau drame</h1>
        <span class="schema" itemprop="datePublished">2026-08-25</span>
        <div class="adate">25.08.2026</div>
        <div class="body"><p>Article body</p></div>
        </body></html>
    "#;
    let url = Url::parse("https://www.mediacongo.net/article-actualite-166762.html").unwrap();

    let article = crawler.parse_article(html, Some(&url), None).unwrap();

    assert_eq!(
        article.published_at.to_rfc3339(),
        "2026-08-25T00:00:00+00:00"
    );
}

#[test]
fn substitutes_category_and_page_in_endpoint() {
    let mut source = source();
    source.pagination_template = "category/{category}/page/{page}".into();
    let crawler = HtmlCrawler::new(source, HttpClient::new(&Default::default()).unwrap());
    assert_eq!(
        crawler.endpoint_url(3, Some("news")).unwrap().as_str(),
        "https://example.com/category/news/page/3"
    );
}

#[test]
fn estimates_html_archives_from_first_page_density_and_page_count() {
    let crawler = HtmlCrawler::new(source(), HttpClient::new(&Default::default()).unwrap());
    let html = r#"
        <div class="article"></div>
        <div class="article"></div>
        <div class="article"></div>
        <nav class="pages"><a href="/news?page=4">Last</a></nav>
    "#;

    let page_range = crawler.pagination_from_html(html).unwrap();
    let articles_per_page = crawler.listing_entries(html).unwrap().len();

    assert_eq!(page_range, PageRange::new(0, 4).unwrap());
    assert_eq!(
        estimate_archive_size(articles_per_page, page_range).unwrap(),
        15
    );
}

#[test]
fn estimates_a_listing_without_pagination_as_one_page() {
    let crawler = HtmlCrawler::new(source(), HttpClient::new(&Default::default()).unwrap());
    let html = r#"<div class="article"></div><div class="article"></div>"#;

    let page_range = crawler.pagination_from_html(html).unwrap();

    assert_eq!(page_range, PageRange::new(0, 0).unwrap());
    assert_eq!(estimate_archive_size(2, page_range).unwrap(), 2);
}
