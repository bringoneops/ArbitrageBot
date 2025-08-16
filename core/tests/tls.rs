use arb_core::tls::build_tls_config;

#[test]
fn invalid_hex_pin_returns_error() {
    let pins = vec!["zz".to_string()];
    let err = build_tls_config(None, &pins).unwrap_err();
    assert!(err.to_string().contains("decoding pin"));
}
