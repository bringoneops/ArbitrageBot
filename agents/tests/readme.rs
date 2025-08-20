use std::collections::BTreeSet;

use regex::Regex;

#[test]
fn readme_exchange_ids_match_registry() {
    // Register all adapters so registry knows about every supported exchange.
    agents::adapter::binance::register();
    agents::adapter::gateio::register();
    agents::adapter::mexc::register();
    agents::adapter::bingx::register();
    agents::adapter::kucoin::register();
    agents::adapter::xt::register();
    agents::adapter::bitmart::register();
    agents::adapter::coinex::register();
    agents::adapter::latoken::register();
    agents::adapter::lbank::register();
    agents::adapter::bitget::register();

    let registered: BTreeSet<String> =
        agents::registry::registered_ids().into_iter().map(|s| s.to_string()).collect();

    let readme_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../README.md");
    let readme = std::fs::read_to_string(readme_path).unwrap();
    let re = Regex::new(r"(?s)Supported IDs:\s*((`[^`]+`,?\s*)+)\.").unwrap();
    let caps = re.captures(&readme).expect("EXCHANGES section not found");
    let list = caps.get(1).unwrap().as_str();
    let id_re = Regex::new(r"`([^`]+)`").unwrap();
    let readme_ids: BTreeSet<_> = id_re
        .captures_iter(list)
        .map(|c| c[1].to_string())
        .collect();

    assert_eq!(registered, readme_ids);
}
