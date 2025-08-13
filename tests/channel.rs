use serde::Deserialize;

#[derive(Deserialize)]
struct EventConfig {
    event_buffer_size: usize,
}

#[test]
fn default_buffer_size_is_exposed() {
    let cfg: EventConfig =
        toml::from_str(include_str!("../config/default.toml")).expect("valid config");
    assert_eq!(cfg.event_buffer_size, 1024);
}
