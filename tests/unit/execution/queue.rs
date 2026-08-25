use super::*;

#[test]
fn stable_ids_are_deterministic() {
    let value = ("example", "https://example.com/story");
    assert_eq!(
        stable_job_id("article", &value).unwrap(),
        stable_job_id("article", &value).unwrap()
    );
}

#[test]
fn zero_retention_removes_jobs_immediately() {
    assert!(matches!(retention(0), RemoveOnFinish::Bool(true)));
}

#[test]
fn queue_names_are_scoped_by_agent_id() {
    assert_eq!(
        scoped_queue_name(&encode_agent_id("basango-pi-01"), "articles"),
        "basango-pi-01-articles"
    );
    assert_eq!(encode_agent_id("pi:west_1"), "pi_3awest_5f1");
}

#[test]
fn queue_snapshot_preserves_all_job_counts() {
    let snapshot = snapshot_from_counts(
        "agent-discovery",
        1,
        JobCounts {
            waiting: 2,
            active: 3,
            delayed: 4,
            prioritized: 5,
            completed: 6,
            failed: 7,
            waiting_children: 8,
            paused: 9,
        },
    );

    assert_eq!(snapshot.name, "agent-discovery");
    assert_eq!(snapshot.workers, 1);
    assert_eq!(snapshot.waiting, 2);
    assert_eq!(snapshot.active, 3);
    assert_eq!(snapshot.delayed, 4);
    assert_eq!(snapshot.prioritized, 5);
    assert_eq!(snapshot.completed, 6);
    assert_eq!(snapshot.failed, 7);
    assert_eq!(snapshot.waiting_children, 8);
    assert_eq!(snapshot.paused, 9);
}
