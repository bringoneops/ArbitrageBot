use anyhow::{Context, Result};
use tokio_rustls::rustls::{client::WebPkiVerifier, Certificate, ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use rustls_pemfile::certs;
use sha2::{Digest, Sha256};
use std::{fs::File, io::BufReader, sync::Arc, time::SystemTime};
use subtle::ConstantTimeEq;

struct PinnedVerifier {
    inner: WebPkiVerifier,
    pins: Vec<Vec<u8>>,
}

impl PinnedVerifier {
    fn new(inner: WebPkiVerifier, pins: Vec<Vec<u8>>) -> Self {
        Self { inner, pins }
    }
}

impl tokio_rustls::rustls::client::ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &tokio_rustls::rustls::client::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp: &[u8],
        now: SystemTime,
    ) -> std::result::Result<tokio_rustls::rustls::client::ServerCertVerified, tokio_rustls::rustls::Error> {
        self.inner
            .verify_server_cert(end_entity, intermediates, server_name, scts, ocsp, now)?;
        let fingerprint = Sha256::digest(&end_entity.0);
        if self
            .pins
            .iter()
            .any(|p| p.as_slice().ct_eq(fingerprint.as_slice()).into())
        {
            Ok(tokio_rustls::rustls::client::ServerCertVerified::assertion())
        } else {
            Err(tokio_rustls::rustls::Error::General("certificate pin mismatch".into()))
        }
    }
}

/// Build a TLS configuration. `cert_pins` must be SHA-256 certificate pins
/// encoded as hexadecimal strings.
pub fn build_tls_config(
    ca_bundle: Option<&str>,
    cert_pins: &[String],
) -> Result<Arc<ClientConfig>> {
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().context("loading native certs")? {
        root_store.add(&Certificate(cert.0))?;
    }
    if let Some(path) = ca_bundle {
        let mut reader = BufReader::new(File::open(path).context("opening CA bundle")?);
        for cert in certs(&mut reader).context("reading CA bundle")? {
            root_store.add(&Certificate(cert))?;
        }
    }
    let verifier = WebPkiVerifier::new(root_store.clone(), None);
    let mut config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    if !cert_pins.is_empty() {
        let pins: Vec<Vec<u8>> = cert_pins
            .iter()
            .map(|p| hex::decode(p).with_context(|| format!("decoding pin: {p}")))
            .collect::<Result<_>>()?;
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(PinnedVerifier::new(verifier, pins)));
    }
    Ok(Arc::new(config))
}
