use std::{
        fs::File,
        io::{BufReader, ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    str::from_utf8_unchecked,
        sync::Arc, // For Arc<TlsAcceptor>
    thread,
};

use futures::channel::mpsc;
    // Assuming native_tls is added to e2e/Cargo.toml
    use native_tls::{Identity, TlsAcceptor, TlsStream}; 

use crate::{
    http_utils::http_ok_response,
    mock::aggregator::{Aggregator, SimpleAggregator},
    BUFFER_SIZE,
};

    // Placeholder for where certs might be. User needs to ensure these exist.
    const MOCK_CERT_PATH: &str = "e2e/mock_cert.pem"; // Or a path accessible by the test runner
    const MOCK_KEY_PATH: &str = "e2e/mock_key.p8";   // PKCS#8 format typically

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

// Helper to load PKCS#8 key and PEM certificate for native-tls
// This would ideally be in a shared test utils module.
pub fn load_server_identity() -> Result<Identity, Box<dyn std::error::Error>> {
    // This is a placeholder for actual key loading.
    // In a real test environment, you'd read these from files.
    // For now, to make it somewhat runnable without external files,
    // I'll use dummy byte arrays. These WON'T WORK for real TLS.
    // Replace with actual file loading:
    // let key_file = File::open(MOCK_KEY_PATH)?;
    // let mut key_reader = BufReader::new(key_file);
    // let mut key_bytes = Vec::new();
    // key_reader.read_to_end(&mut key_bytes)?;
    //
    // let cert_file = File::open(MOCK_CERT_PATH)?;
    // let mut cert_reader = BufReader::new(cert_file);
    // let mut cert_bytes = Vec::new();
    // cert_reader.read_to_end(&mut cert_bytes)?;
    // Identity::from_pkcs8(&cert_bytes, &key_bytes)
    
    // Using hardcoded dummy identity for placeholder. This will fail handshake.
    // User must replace MOCK_CERT_PATH and MOCK_KEY_PATH and use file loading code above.
    println!("WARN: Using dummy TLS identity for mock server. Real cert/key needed for CONNECT tests to pass TLS handshake.");
    println!("Please ensure '{}' (PEM cert) and '{}' (PKCS#8 key) exist or update paths.", MOCK_CERT_PATH, MOCK_KEY_PATH);
    
    // Attempt to load real files if they exist, otherwise use a dummy that will likely fail.
    match (File::open(MOCK_CERT_PATH), File::open(MOCK_KEY_PATH)) {
        (Ok(cert_file), Ok(key_file)) => {
            let mut cert_reader = BufReader::new(cert_file);
            let mut cert_bytes = Vec::new();
            cert_reader.read_to_end(&mut cert_bytes)?;

            let mut key_reader = BufReader::new(key_file);
            let mut key_bytes = Vec::new();
            key_reader.read_to_end(&mut key_bytes)?;
            
            println!("Successfully loaded mock certificate and key from files.");
            Identity::from_pkcs8(&cert_bytes, &key_bytes).map_err(|e| e.into())
        }
        _ => {
            // Fallback dummy identity if files are not found - this will cause TLS errors.
            // Generate a temporary self-signed cert/key for basic structure testing if possible,
            // but native-tls Identity::from_pkcs8 needs valid data.
            // This dummy identity is invalid.
            let dummy_key = b"-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC7g+kYj4N2\n-----END PRIVATE KEY-----"; // Invalid dummy
            let dummy_cert = b"-----BEGIN CERTIFICATE-----\nMIIEajCCAuKgAwIBAgIJAOsrGj+P74d5MA0GCSqGSIb3DQEBCwUAMFExCzAJ\n-----END CERTIFICATE-----"; // Invalid dummy
            Identity::from_pkcs8(dummy_cert, dummy_key).map_err(|e| e.into())
        }
    }
}
