use anyhow::Result;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

pub type SymbolId = String;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VenueType {
    Spot,
    Futures,
    Options,
    Unknown,
}

impl Default for VenueType {
    fn default() -> Self {
        VenueType::Unknown
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ContractSpec {
    pub venue: VenueType,
    pub base: String,
    pub quote: String,
    #[serde(default)]
    pub lot_step: Option<f64>,
    #[serde(default)]
    pub price_step: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct SymbolConfig {
    pub id: SymbolId,
    #[serde(default)]
    pub spec: ContractSpec,
    #[serde(default)]
    pub aliases: HashMap<String, Vec<String>>, // exchange -> raw symbols
}

#[derive(Default)]
struct SymbolTable {
    aliases: HashMap<(String, String), SymbolId>,
    specs: HashMap<SymbolId, ContractSpec>,
}

static TABLE: OnceCell<SymbolTable> = OnceCell::new();

fn table() -> &'static SymbolTable {
    TABLE.get_or_init(|| SymbolTable::default())
}

pub fn load_from_path(path: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    if buf.trim().is_empty() {
        return Ok(());
    }

    // Try JSON first, then TOML
    let entries: Vec<SymbolConfig> = if let Ok(v) = serde_json::from_str(&buf) {
        v
    } else {
        toml::from_str(&buf)?
    };

    let mut tbl = SymbolTable::default();
    for entry in entries {
        let id = entry.id.clone();
        tbl.specs.insert(id.clone(), entry.spec);
        for (ex, raws) in entry.aliases {
            for raw in raws {
                tbl.aliases.insert((ex.clone(), raw), id.clone());
            }
        }
    }
    TABLE.set(tbl).ok();
    Ok(())
}

pub fn normalize_symbol(exchange: &str, raw: &str) -> SymbolId {
    let tbl = table();
    if let Some(id) = tbl.aliases.get(&(exchange.to_string(), raw.to_string())) {
        return id.clone();
    }
    // fallback: standard formatting
    raw.to_uppercase().replace('_', "-")
}

pub fn get_spec(id: &str) -> Option<ContractSpec> {
    table().specs.get(id).cloned()
}
