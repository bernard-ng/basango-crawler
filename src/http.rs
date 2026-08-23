//! HTTP infrastructure shared by source crawlers and backend forwarding.
//!
//! This module hides retries, timeouts, user-agent selection, and status
//! handling behind a small client. Callers focus on their protocol instead of
//! rebuilding transport policy for every request.

mod client;
mod open_graph;
mod user_agent;

pub use client::{HttpClient, HttpResponse};
pub use open_graph::{
    consume_html as consume_open_graph_html, consume_url as consume_open_graph_url,
};
