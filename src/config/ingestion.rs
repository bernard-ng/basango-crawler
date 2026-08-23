use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestionApiConfig {
    pub endpoint: Option<Url>,
    pub token: String,
}
