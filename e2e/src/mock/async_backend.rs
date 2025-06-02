use std::{
        io::{ErrorKind, Read, Write as IoWrite}, // Renamed Write to IoWrite to avoid conflict
    net::{SocketAddr, TcpListener, TcpStream},
    str::from_utf8_unchecked,
        sync::Arc, // For Arc<TlsAcceptor>
    thread,
        path::PathBuf, // To work with temp file paths
};

use futures::channel::mpsc;
    use native_tls::{Identity, TlsAcceptor, TlsStream};
    use rcgen::{CertificateParams, KeyPair, SanType, DistinguishedName, Certificate}; // Added Certificate
    use tempfile::NamedTempFile; // For temporary certificate files

use crate::{
    http_utils::http_ok_response,
    mock::aggregator::{Aggregator, SimpleAggregator},
    BUFFER_SIZE,
};

// Helper to generate cert/key PEM strings
fn internal_generate_cert_key_pems(subject_alt_names: Vec<String>) -> Result<(String, String), Box<dyn std::error::Error>> {
    let mut params = CertificateParams::new(subject_alt_names.clone());
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(rcgen::DnType::CommonName, subject_alt_names.get(0).map_or("mockserver.test", |s| s).to_string()); // Use first SAN as CN or default
    // Add all provided SANs
    for san in subject_alt_names {
        // rcgen figures out if it's IP or DNS name based on parsing
        params.subject_alt_names.push(SanType::GeneralName(rcgen::GeneralName::from(san)));
    }


    let cert = Certificate::from_params(params)?;
    let cert_pem = cert.pem()?;
    let key_pem = cert.key_pair().serialize_pem();
    Ok((cert_pem, key_pem))
}

/// Generates a self-signed certificate and private key, saving them to temporary files.
/// Returns paths to the temporary cert and key files.
pub fn generate_temp_cert_key_files(subject_alt_names: Vec<String>) -> Result<(NamedTempFile, NamedTempFile), Box<dyn std::error::Error>> {
    let (cert_pem, key_pem) = internal_generate_cert_key_pems(subject_alt_names)?;

    let mut cert_file = NamedTempFile::new()?;
    cert_file.write_all(cert_pem.as_bytes())?;

    let mut key_file = NamedTempFile::new()?;
    key_file.write_all(key_pem.as_bytes())?;

    Ok((cert_file, key_file))
}


/// Handle to a detached thread where a Backend runs
/// (a thin wrapper around a TcpListener)
pub struct BackendHandle<T> {
    pub name: String,
    /// Allows to stop the backend within the thread
    pub stop_tx: mpsc::Sender<()>,
    /// Receives data from the backend on the thread
    pub aggregator_rx: mpsc::Receiver<T>,
}

type RequestHandler<A> = Box<dyn Fn(&TcpStream, &str, A) -> A + Send + Sync>;

impl<A: Aggregator + Send + Sync + 'static> BackendHandle<A> {
    pub fn spawn_detached_backend<S: Into<String>>(
        name: S,
        address: SocketAddr,
        mut aggregator: A,
        handler: RequestHandler<A>,
        specific_address: Option<SocketAddr>, // Added
    ) -> Self {
        let name = name.into();
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        let (mut aggregator_tx, aggregator_rx) = mpsc::channel::<A>(1);

        // Use specific_address if provided, otherwise use the dynamically assigned one from setup.
        // The 'address' parameter in this function signature is usually the one from sozu config,
        // which might be dynamic for backends. For a mock *target* server, we need a fixed one.
        let bind_addr = specific_address.unwrap_or(address);

        let listener = TcpListener::bind(bind_addr).expect(&format!("could not bind to address: {}", bind_addr));
        let actual_address = listener.local_addr().expect("Failed to get local address from listener");
        println!("Mock backend '{}' attempting to listen on {}, actually listening on {}", name, bind_addr, actual_address);

        let mut clients = Vec::new();
        let thread_name = name.to_owned();

        // The backend runs on this detached thread:
        // - accepts tcp connections
        // - calls handler on each live connections
        // - monitors stop_rx to stop itself
        thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("could not set nonblocking on listener");
            loop {
                let stream = listener.accept();
                match stream {
                    Ok(stream) => {
                        println!("{thread_name}: new connection");
                        stream
                            .0
                            .set_nonblocking(true)
                            .expect("cound not set nonblocking on client");
                        clients.push(stream.0);
                    }
                    Err(error) => {
                        if error.kind() != ErrorKind::WouldBlock {
                            println!("IO Error: {error:?}");
                        }
                    }
                }
                for client in &clients {
                    aggregator = handler(client, &thread_name, aggregator);
                }
                match stop_rx.try_next() {
                    Ok(Some(_)) => break,
                    _ => continue,
                }
            }
            drop(listener);
            aggregator_tx
                .try_send(aggregator)
                .expect("could not send aggregator");
        });
        Self {
            name,
            stop_tx,
            aggregator_rx,
        }
    }

    pub fn stop_and_get_aggregator(&mut self) -> Option<A> {
        self.stop_tx.try_send(()).expect("could not stop backend");
        loop {
            match self.aggregator_rx.try_next() {
                Ok(Some(aggregator)) => return Some(aggregator),
                _ => continue,
            }
        }
    }
}

impl BackendHandle<SimpleAggregator> {
    /// This creates a callback that listens on a TcpStream
    /// and returns HTTP OK responses with the given content in the body
    /// it returns an updated aggregator
    pub fn http_handler<S: Into<String>>(content: S) -> RequestHandler<SimpleAggregator> {
        let content = content.into();
        Box::new(move |mut stream, backend_name, mut aggregator| {
            let mut buf = [0u8; BUFFER_SIZE];
            match stream.read(&mut buf) {
                Ok(0) => { /* Connection closed by peer */ return aggregator; }
                Ok(n) => {
                    // println!("{backend_name} received {n} bytes: {}", unsafe { from_utf8_unchecked(&buf[..n]) });
                    aggregator.requests_received += 1;
                    let response_str = http_ok_response(&content);
                    if let Err(e) = stream.write_all(response_str.as_bytes()) {
                        // Error writing response, maybe client closed connection
                        if e.kind() != ErrorKind::WouldBlock {
                             println!("{}: Error writing response: {}", backend_name, e);
                        }
                        return aggregator; // Don't increment responses_sent if write fails or would block
                    }
                    aggregator.responses_sent += 1;
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    // No data to read right now
                }
                Err(e) => {
                    // Other read error
                    println!("{}: Error reading from stream: {}", backend_name, e);
                    return aggregator; // Stop processing this stream on error
                }
            }
            aggregator
        })
    }

    /// This creates a callback that listens on a TcpStream
    /// and returns the given content as raw bytes
    /// it returns an updated aggregator
    pub fn tcp_handler<S: Into<String>>(content: S) -> RequestHandler<SimpleAggregator> {
        let content: String = content.into();
        Box::new(move |mut stream, backend_name, mut aggregator| {
            let mut buf = [0u8; BUFFER_SIZE];
            match stream.read(&mut buf) {
                Ok(0) => { /* Connection closed by peer */ return aggregator; }
                Ok(n) => {
                    // println!("{backend_name} received {n} TCP bytes: {}", unsafe { from_utf8_unchecked(&buf[..n]) });
                    aggregator.requests_received += 1;
                    if let Err(e) = stream.write_all(content.as_bytes()) {
                        if e.kind() != ErrorKind::WouldBlock {
                            println!("{backend_name}: Error writing TCP response: {}", e);
                        }
                        return aggregator;
                    }
                    aggregator.responses_sent += 1;
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    // No data to read right now
                }
                Err(e) => {
                    println!("{backend_name} TCP read error: {e}");
                    return aggregator; // Stop processing this stream on error
                }
            }
            aggregator
        })
    }
}

// New VerifyingAggregator and handler for more detailed tests
use std::collections::HashMap; // Added for hit_counts

#[derive(Debug, Clone, Default)]
pub struct VerifyingAggregator {
    pub requests_received: usize,
    pub responses_sent: usize,
    pub last_method: Option<String>,
    pub last_path: Option<String>, // This will be the key for hit_counts
    pub last_host_header: Option<String>,
    pub last_body: Option<String>,
    pub hit_counts: HashMap<String, usize>, // Tracks hits per path
    // Add other fields to verify as needed, e.g., specific headers
}

impl Aggregator for VerifyingAggregator {
    fn new() -> Self { Default::default() }
    fn add_request(&mut self) { self.requests_received += 1; }
    fn add_response(&mut self) { self.responses_sent += 1; }
}

impl BackendHandle<VerifyingAggregator> {
    pub fn verifying_http_handler(
        expected_method: String,
        expected_path_prefix: String, // Expect path to start with this
        expected_host_header: String,
        response_body: String,
        response_status: u16,
        // Added custom_response_headers
        custom_response_headers: Option<Vec<(String, String)>>,
    ) -> RequestHandler<VerifyingAggregator> {
        Box::new(move |stream, backend_name, mut aggregator| {
            // For verifying_http_handler, stream is &TcpStream, not mut
            let mut stream_clone = stream.try_clone().expect("Failed to clone stream for verifying_http_handler");
            let mut buf = [0u8; BUFFER_SIZE];
            match stream_clone.read(&mut buf) {
                Ok(0) => return aggregator, // Connection closed
                Ok(n) => {
                    let request_str = String::from_utf8_lossy(&buf[..n]).to_string();
                    // println!("{} VerifyingHandler received request:\n{}", backend_name, request_str);

                    // Basic HTTP parsing (very naive, for test purposes)
                    let mut lines = request_str.lines();
                    if let Some(request_line) = lines.next() {
                        let parts: Vec<&str> = request_line.split_whitespace().collect();
                        if parts.len() >= 2 { // Should be 3 (METHOD PATH VERSION)
                            aggregator.last_method = Some(parts[0].to_string());
                            aggregator.last_path = Some(parts[1].to_string());
                        }
                    }
                    // Reset host header for each new request check
                    aggregator.last_host_header = None;
                    let header_lines = request_str.lines().skip(1); // Skip request line
                    for line in header_lines {
                        if line.to_lowercase().starts_with("host:") {
                            aggregator.last_host_header = Some(line.split_at(5).1.trim().to_string());
                        }
                        if line.is_empty() { // End of headers
                            break;
                        }
                    }

                    let body_parts: Vec<&str> = request_str.split("\r\n\r\n").collect();
                    if body_parts.len() > 1 && !body_parts[1].is_empty() {
                        aggregator.last_body = Some(body_parts[1..].join("\r\n\r\n"));
                    } else {
                        aggregator.last_body = None;
                    }

                    // Perform assertions
                    if let Some(ref method) = aggregator.last_method {
                        assert_eq!(method, &expected_method, "{}: Method mismatch", backend_name);
                    } else {
                        panic!("{}: Failed to parse method from request", backend_name);
                    }

                    if let Some(ref path) = aggregator.last_path {
                         assert!(path.starts_with(&expected_path_prefix), "{}: Path prefix mismatch. Expected prefix: '{}', Got: '{}'", backend_name, expected_path_prefix, path);
                        // Increment hit count for this path
                        *aggregator.hit_counts.entry(path.clone()).or_insert(0) += 1;
                    } else {
                        panic!("{}: Failed to parse path from request", backend_name);
                    }

                    if !expected_host_header.is_empty() { // Allow skipping host check if empty expected_host_header
                        assert_eq!(aggregator.last_host_header.as_ref(), Some(&expected_host_header), "{}: Host header mismatch", backend_name);
                    }

                    aggregator.requests_received += 1;

                    let mut response_headers_str = format!("Content-Length: {}\r\nConnection: close\r\n", response_body.len());
                    if let Some(ref custom_headers) = custom_response_headers {
                        for (name, value) in custom_headers {
                            response_headers_str.push_str(&format!("{}: {}\r\n", name, value));
                        }
                    }

                    let response = format!(
                        "HTTP/1.1 {}\r\n{}{}\r\n{}",
                        response_status,
                        response_headers_str, // All headers including Content-Length and custom ones
                        // No extra \r\n needed here as headers_str includes trailing \r\n for each
                        response_body
                    );

                    if stream_clone.write_all(response.as_bytes()).is_err() {
                        // println!("{}: Error writing verifying response.", backend_name);
                        return aggregator;
                    }
                    aggregator.responses_sent += 1;
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => {
                    // println!("{}: Error reading for verifying_http_handler: {}", backend_name, e);
                    return aggregator;
                }
            }
            aggregator
        })
    }

    // Handler for CONNECT: performs TLS handshake then uses verifying_http_handler logic
    pub fn tls_http_handler(
        acceptor: Arc<TlsAcceptor>,
        expected_method_inner: String,
        expected_path_prefix_inner: String,
        expected_host_header_inner: String, // Host header *inside* the TLS tunnel
        response_body_inner: String,
        response_status_inner: u16,
    ) -> RequestHandler<VerifyingAggregator> {
        Box::new(move |tcp_stream, backend_name, mut aggregator| {
            // Do not set non-blocking on tcp_stream here, TlsAcceptor might need blocking.
            // Or handle WouldBlock from acceptor.accept() if tcp_stream is non-blocking.
            // For simplicity in test, let's assume it might block for handshake.
            // Clone the stream as acceptor.accept takes ownership.
            let stream_clone = match tcp_stream.try_clone() {
                Ok(s) => s,
                Err(e) => {
                    println!("{}: Failed to clone TCP stream for TLS handshake: {}", backend_name, e);
                    return aggregator;
                }
            };

            match acceptor.accept(stream_clone) {
                Ok(mut tls_stream) => {
                    println!("{}: TLS handshake successful with client.", backend_name);
                    let mut buf = [0u8; BUFFER_SIZE];
                    match tls_stream.read(&mut buf) {
                        Ok(0) => { /* Connection closed by peer post-handshake */ }
                        Ok(n) => {
                            let request_str = String::from_utf8_lossy(&buf[..n]).to_string();
                            println!("{} TLS VerifyingHandler received request:\n{}", backend_name, request_str);

                            let mut lines = request_str.lines();
                            if let Some(request_line) = lines.next() {
                                let parts: Vec<&str> = request_line.split_whitespace().collect();
                                if parts.len() >= 2 {
                                    aggregator.last_method = Some(parts[0].to_string());
                                    aggregator.last_path = Some(parts[1].to_string());
                                }
                            }
                            aggregator.last_host_header = None;
                            let header_lines = request_str.lines().skip(1);
                            for line in header_lines {
                                if line.to_lowercase().starts_with("host:") {
                                    aggregator.last_host_header = Some(line.split_at(5).1.trim().to_string());
                                }
                                if line.is_empty() { break; }
                            }
                            let body_parts: Vec<&str> = request_str.split("\r\n\r\n").collect();
                            if body_parts.len() > 1 && !body_parts[1].is_empty() {
                                aggregator.last_body = Some(body_parts[1..].join("\r\n\r\n"));
                            } else {
                                aggregator.last_body = None;
                            }

                            if let Some(ref method) = aggregator.last_method {
                                assert_eq!(method, &expected_method_inner, "{}: Inner method mismatch", backend_name);
                            } else {
                                panic!("{}: Failed to parse inner method", backend_name);
                            }
                            if let Some(ref path) = aggregator.last_path {
                                assert!(path.starts_with(&expected_path_prefix_inner), "{}: Inner path prefix mismatch. Expected: '{}', Got: '{}'", backend_name, expected_path_prefix_inner, path);
                            } else {
                                panic!("{}: Failed to parse inner path", backend_name);
                            }
                             if !expected_host_header_inner.is_empty() {
                                assert_eq!(aggregator.last_host_header.as_ref(), Some(&expected_host_header_inner), "{}: Inner host header mismatch", backend_name);
                            }

                            aggregator.requests_received += 1;

                            let response = format!(
                                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                response_status_inner,
                                response_body_inner.len(),
                                response_body_inner
                            );
                            if tls_stream.write_all(response.as_bytes()).is_err() {
                                // Error writing response
                            } else {
                                aggregator.responses_sent += 1;
                            }
                            let _ = tls_stream.shutdown(); // Close TLS session
                        }
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => { /* Read would block */ }
                        Err(e) => {
                            println!("{}: Error reading from TLS stream: {}", backend_name, e);
                        }
                    }
                }
                Err(e) => {
                    println!("{}: TLS handshake failed: {}", backend_name, e);
                    // This is tricky: the client might still be connected via TCP.
                    // The handler is per-connection attempt on the listener.
                    // If handshake fails, this particular handler invocation is done.
                }
            }
            aggregator
        })
    }
}

// Helper to load PKCS#8 key and PEM certificate for native-tls using temporary file paths.
// The NamedTempFile objects must be kept in scope by the caller for the duration these paths are needed.
pub fn load_server_identity_from_temp_files(
    cert_path: &std::path::Path,
    key_path: &std::path::Path
) -> Result<Identity, Box<dyn std::error::Error>> {
    println!("Loading TLS identity from temp files: cert='{}', key='{}'", cert_path.display(), key_path.display());

    let mut cert_file = std::fs::File::open(cert_path)?;
    let mut cert_pem_bytes = Vec::new();
    cert_file.read_to_end(&mut cert_pem_bytes)?;

    let mut key_file = std::fs::File::open(key_path)?;
    let mut key_pem_bytes = Vec::new();
    key_file.read_to_end(&mut key_pem_bytes)?;

    // Convert PEM to DER for Identity::from_pkcs8
    // Certificate part
    let cert_pem_str = String::from_utf8(cert_pem_bytes)?;
    let cert_der = rcgen::Certificate::from_pem(&cert_pem_str)?.der().to_vec();

    // Key part (PKCS#8 PEM to PKCS#8 DER)
    let key_pem_str = String::from_utf8(key_pem_bytes)?;
    let key_der = KeyPair::from_pem(&key_pem_str)?.serialize_pkcs8_der();

    Identity::from_pkcs8(&cert_der, &key_der).map_err(|e| e.into())
}

// Renamed the original function that returned PEM strings.
// This is now an internal helper.
// pub fn generate_cert_key_pem(subject_alt_names: Vec<String>) -> Result<(String, String), Box<dyn std::error::Error>> {
// ... this is now internal_generate_cert_key_pems ...
// }
