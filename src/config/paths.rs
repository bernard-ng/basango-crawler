use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PathsConfig {
    pub root: PathBuf,
    pub data: PathBuf,
    pub sqlite: Option<PathBuf>,
}

impl PathsConfig {
    pub(super) fn data_path(&self) -> PathBuf {
        if self.data.as_os_str().is_empty() {
            self.root.join("data")
        } else {
            self.data.clone()
        }
    }

    pub(super) fn sqlite_path(&self) -> PathBuf {
        self.sqlite
            .clone()
            .unwrap_or_else(|| self.data_path().join("crawler.db"))
    }
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("."),
            data: PathBuf::new(),
            sqlite: None,
        }
    }
}
