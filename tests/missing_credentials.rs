use binance_us_and_global::config::Config;
use serial_test::serial;

#[test]
#[serial]
fn missing_api_key_fails() {
    std::env::remove_var("API_KEY");
    std::env::set_var("API_SECRET", "secret");
    std::env::set_var("SPOT_SYMBOLS", "BTCUSDT");
    std::env::set_var("FUTURES_SYMBOLS", "BTCUSDT");
    assert!(Config::from_env().is_err());
}

#[test]
#[serial]
fn missing_api_secret_fails() {
    std::env::set_var("API_KEY", "key");
    std::env::remove_var("API_SECRET");
    std::env::set_var("SPOT_SYMBOLS", "BTCUSDT");
    std::env::set_var("FUTURES_SYMBOLS", "BTCUSDT");
    assert!(Config::from_env().is_err());
}
