use clap::Parser;

use super::*;

#[test]
fn clap_parses_typed_crawl_ranges() {
    let cli = Cli::try_parse_from([
        "crawler",
        "crawl",
        "--source-id",
        "example",
        "--page-range",
        "1:3",
        "--date-range",
        "2025-01-01:2025-01-31",
        "--direction",
        "backward",
    ])
    .unwrap();
    let Command::Crawl(arguments) = cli.command else {
        panic!("expected crawl command")
    };
    assert_eq!(arguments.page_range.unwrap(), PageRange::new(1, 3).unwrap());
    assert_eq!(arguments.direction, Some(UpdateDirection::Backward));
}

#[test]
fn status_is_a_standalone_command() {
    let cli = Cli::try_parse_from(["crawler", "status"]).unwrap();
    assert!(matches!(cli.command, Command::Status));
}

#[test]
fn source_is_a_standalone_command() {
    let cli = Cli::try_parse_from(["crawler", "source"]).unwrap();
    assert!(matches!(cli.command, Command::Source));
}

#[test]
fn schedule_requires_an_explicit_source() {
    assert!(Cli::try_parse_from(["crawler", "schedule"]).is_err());
}
