use std::{sync::Arc, time::SystemTime};

use rustls::{server::ClientCertVerified, Certificate, DistinguishedName};

pub struct ServerSkipClientVerification;

impl ServerSkipClientVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::server::ClientCertVerifier for ServerSkipClientVerification {
    fn client_auth_root_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _now: SystemTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(rustls::server::ClientCertVerified::assertion())
    }
}

pub struct ClientSkipServerVerification;

impl ClientSkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for ClientSkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
