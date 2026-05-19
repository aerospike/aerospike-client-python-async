// Copyright 2023-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::IoError;

    ////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  TlsConfig
    //
    ////////////////////////////////////////////////////////////////////////////////////////////

    #[cfg(feature = "tls")]
    #[gen_stub_pyclass(module = "_aerospike_async_native")]
    #[pyclass(from_py_object, 
        name = "TlsConfig",
        module = "_aerospike_async_native",
        subclass,
        freelist = 100
    )]
    #[derive(Clone)]
    pub struct TlsConfig {
        pub(crate) _as: rustls::ClientConfig,
    }

    // Type alias to allow function signatures to compile when TLS is disabled
    #[cfg(not(feature = "tls"))]
    type TlsConfig = ();

    #[cfg(feature = "tls")]
    #[gen_stub_pymethods]
    #[pymethods]
    impl TlsConfig {
        /// Create a new TlsConfig from CA certificate file.
        ///
        /// Args:
        ///     cafile: Path to the CA certificate file (PEM format)
        ///
        /// Returns:
        ///     TlsConfig instance configured with the CA certificate
        #[new]
        #[pyo3(signature = (cafile))]
        pub fn new(cafile: String) -> PyResult<Self> {
            use rustls::{ClientConfig, RootCertStore};
            use std::fs::File;
            use std::io::BufReader;

            // Build root cert store with webpki roots and custom CA
            let mut root_store = RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            };

            // Add custom CA certificates
            let ca_file = File::open(&cafile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open CA file {}: {}", cafile, e)))?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut ca_reader).collect();
            let certs = certs.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse CA file {}: {}", cafile, e)))?;

            for cert in certs {
                root_store.add(cert).map_err(|e| PyErr::new::<IoError, _>(format!("Cannot add CA certificate: {}", e)))?;
            }

            // Build client config with root certificates
            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            Ok(TlsConfig { _as: config })
        }

        /// Create a TlsConfig with client authentication from certificate and key files.
        ///
        /// Args:
        ///     cafile: Path to the CA certificate file (PEM format)
        ///     certfile: Path to the client certificate file (PEM format)
        ///     keyfile: Path to the client private key file (PEM format)
        ///
        /// Returns:
        ///     TlsConfig instance configured with CA and client certificates
        #[staticmethod]
        pub fn with_client_auth(cafile: String, certfile: String, keyfile: String) -> PyResult<Self> {
            use rustls::{ClientConfig, RootCertStore};
            use rustls::pki_types::{CertificateDer, PrivateKeyDer};
            use std::fs::File;
            use std::io::BufReader;

            // Build root cert store with webpki roots and custom CA
            let mut root_store = RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.into(),
            };

            // Add custom CA certificates
            let ca_file = File::open(&cafile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open CA file {}: {}", cafile, e)))?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut ca_reader).collect();
            let certs = certs.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse CA file {}: {}", cafile, e)))?;

            for cert in certs {
                root_store.add(cert).map_err(|e| PyErr::new::<IoError, _>(format!("Cannot add CA certificate: {}", e)))?;
            }

            // Load client certificate
            let client_cert_file = File::open(&certfile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open client cert file {}: {}", certfile, e)))?;
            let mut client_cert_reader = BufReader::new(client_cert_file);
            let client_certs: Result<Vec<CertificateDer>, _> = rustls_pemfile::certs(&mut client_cert_reader).collect();
            let client_certs = client_certs.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse client cert file {}: {}", certfile, e)))?;

            // Load client private key
            let key_file = File::open(&keyfile)
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot open key file {}: {}", keyfile, e)))?;
            let mut key_reader = BufReader::new(key_file);
            let keys: Result<Vec<_>, _> = rustls_pemfile::pkcs8_private_keys(&mut key_reader).collect();
            let mut keys = keys.map_err(|e| PyErr::new::<IoError, _>(format!("Cannot parse key file {}: {}", keyfile, e)))?;

            let client_key = keys.pop()
                .ok_or_else(|| PyErr::new::<IoError, _>(format!("No private key found in {}", keyfile)))?;

            // Build client config with root certificates and client auth
            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(client_certs, PrivateKeyDer::Pkcs8(client_key))
                .map_err(|e| PyErr::new::<IoError, _>(format!("Cannot build TLS config with client auth: {}", e)))?;

            Ok(TlsConfig { _as: config })
        }
    }
