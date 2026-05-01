pub mod partner;
pub mod public;
pub mod wasapi;

use std::collections::HashMap;

pub type Metadata = HashMap<String, Vec<MetadataEntry>>;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MetadataEntry {
    pub id: u64,
    pub value: String,
}
