pub mod answers;
pub mod diagnostics;
pub mod editor;
pub mod parser;

use std::{
    cell::RefCell,
    io::ErrorKind,
    net::{Shutdown, SocketAddr},
    rc::{Rc, Weak},
    time::{Duration, Instant},
};

use mio::{net::TcpStream, Interest, Token};
use rusty_ulid::Ulid;
use url::Url; // Added for forward proxy URI parsing

// --- BEGIN CACHING ADDITIONS ---
use std::sync::Mutex;
use lru::LruCache;
use std::num::NonZeroUsize;
use lazy_static::lazy_static;
// --- END CACHING ADDITIONS ---

use sozu_command::{
    config::MAX_LOOP_ITERATIONS,
    logging::EndpointRecord,
    proto::command::{Event, EventKind, ListenerType},
};
// use time::{Duration, Instant};

use crate::{
    backends::{Backend, BackendError},
    pool::{Checkout, Pool},
    protocol::{
        http::{
            answers::DefaultAnswerStream,
            diagnostics::{diagnostic_400_502, diagnostic_413_507},
            editor::HttpContext,
            parser::Method,
        },
        pipe::WebSocketContext,
        SessionState,
    },
    retry::RetryPolicy,
    router::Route,
    server::{push_event, CONN_RETRIES},
    socket::{stats::socket_rtt, SocketHandler, SocketResult, TransportProtocol},
    sozu_command::{logging::LogContext, ready::Ready},
    timer::TimeoutContainer,
    AcceptError, BackendConnectAction, BackendConnectionError, BackendConnectionStatus,
    L7ListenerHandler, L7Proxy, ListenerHandler, Protocol, ProxySession, Readiness,
    RetrieveClusterError, SessionIsToBeClosed, SessionMetrics, SessionResult, StateResult,
};

/// This macro is defined uniquely in this module to help the tracking of kawa h1
/// issues inside Sōzu
macro_rules! log_context {
    ($self:expr) => {
        format!(
            "KAWA-H1\t{}\tSession(public={}, session={}, frontend={}, readiness={}, backend={}, readiness={})\t >>>",
            $self.context.log_context(),
            $self.context.public_address.to_string(),
            $self.context.session_address.map(|addr| addr.to_string()).unwrap_or_else(|| "<none>".to_string()),
            $self.frontend_token.0,
            $self.frontend_readiness,
            $self.backend_token.map(|token| token.0.to_string()).unwrap_or_else(|| "<none>".to_string()),
            $self.backend_readiness,
        )
    };
}

/// Generic Http representation using the Kawa crate using the Checkout of Sozu as buffer
type GenericHttpStream = kawa::Kawa<Checkout>;

impl kawa::AsBuffer for Checkout {
    fn as_buffer(&self) -> &[u8] {
        self.inner.extra()
    }
    fn as_mut_buffer(&mut self) -> &mut [u8] {
        self.inner.extra_mut()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultAnswer {
    Answer301 {
        location: String,
    },
    Answer400 {
        message: String,
        phase: kawa::ParsingPhaseMarker,
        successfully_parsed: String,
        partially_parsed: String,
        invalid: String,
    },
    Answer401 {},
    Answer404 {},
    Answer408 {
        duration: String,
    },
    Answer413 {
        message: String,
        phase: kawa::ParsingPhaseMarker,
        capacity: usize,
    },
    Answer502 {
        message: String,
        phase: kawa::ParsingPhaseMarker,
        successfully_parsed: String,
        partially_parsed: String,
        invalid: String,
    },
    Answer503 {
        message: String,
    },
    Answer504 {
        duration: String,
    },
    Answer507 {
        phase: kawa::ParsingPhaseMarker,
        message: String,
        capacity: usize,
    },
}

impl From<&DefaultAnswer> for u16 {
    fn from(answer: &DefaultAnswer) -> u16 {
        match answer {
            DefaultAnswer::Answer301 { .. } => 301,
            DefaultAnswer::Answer400 { .. } => 400,
            DefaultAnswer::Answer401 { .. } => 401,
            DefaultAnswer::Answer404 { .. } => 404,
            DefaultAnswer::Answer408 { .. } => 408,
            DefaultAnswer::Answer413 { .. } => 413,
            DefaultAnswer::Answer502 { .. } => 502,
            DefaultAnswer::Answer503 { .. } => 503,
            DefaultAnswer::Answer504 { .. } => 504,
            DefaultAnswer::Answer507 { .. } => 507,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutStatus {
    Request,
    Response,
    WaitingForNewRequest,
    WaitingForResponse,
}

pub enum ResponseStream {
    BackendAnswer(GenericHttpStream),
    DefaultAnswer(u16, DefaultAnswerStream),
}

/// Http will be contained in State which itself is contained by Session
pub struct Http<Front: SocketHandler, L: ListenerHandler + L7ListenerHandler> {
    answers: Rc<RefCell<answers::HttpAnswers>>,
    pub backend: Option<Rc<RefCell<Backend>>>,
    backend_connection_status: BackendConnectionStatus,
    pub backend_readiness: Readiness,
    pub backend_socket: Option<TcpStream>,
    backend_stop: Option<Instant>,
    pub backend_token: Option<Token>,
    pub container_backend_timeout: TimeoutContainer,
    pub container_frontend_timeout: TimeoutContainer,
    configured_backend_timeout: Duration,
    configured_connect_timeout: Duration,
    configured_frontend_timeout: Duration,
    /// attempts to connect to the backends during the session
    connection_attempts: u8,
    pub frontend_readiness: Readiness,
    pub frontend_socket: Front,
    frontend_token: Token,
    keepalive_count: usize,
    listener: Rc<RefCell<L>>,
    pub request_stream: GenericHttpStream,
    pub response_stream: ResponseStream,
    /// The HTTP context was separated from the State for borrowing reasons.
    /// Calling a kawa parser mutably borrows the State through request_stream or response_stream,
    /// so Http can't be borrowed again to be used in callbacks. HttContext is an independant
    /// subsection of Http that can be mutably borrowed for parser callbacks.
    pub context: HttpContext,

    // --- Fields for Forward Proxy support ---
    /// True if the current request is a forward proxy request (absolute URI or CONNECT).
    is_forward_proxy_request: bool,
    /// Scheme for the forward proxy target (e.g., "http" or "https").
    forward_target_scheme: Option<String>,
    /// Host for the forward proxy target.
    forward_target_host: Option<String>,
    /// Port for the forward proxy target.
    forward_target_port: Option<u16>,
    /// True if the session is currently in CONNECT tunnel mode.
    is_connect_tunnel: bool,
    /// Buffer for sending "HTTP/1.1 200 Connection established" for CONNECT.
    connect_response_buffer: Option<Vec<u8>>,
    // --- End of Fields for Forward Proxy support ---

    // --- Fields for Caching ---
    serving_from_cache: Option<CachedResponse>,
    // Store the cache key for the current request if it's cacheable,
    // so we can use it when populating the cache after fetching from origin.
    current_cache_key: Option<CacheKey>, 
    // --- End of Fields for Caching ---
}

impl<Front: SocketHandler, L: ListenerHandler + L7ListenerHandler> Http<Front, L> {
    /// Instantiate a new HTTP SessionState with:
    ///
    /// - frontend_interest: READABLE | HUP | ERROR
    /// - frontend_event: EMPTY
    /// - backend_interest: EMPTY
    /// - backend_event: EMPTY
    ///
    /// Remember to set the events from the previous State!
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        answers: Rc<RefCell<answers::HttpAnswers>>,
        configured_backend_timeout: Duration,
        configured_connect_timeout: Duration,
        configured_frontend_timeout: Duration,
        container_frontend_timeout: TimeoutContainer,
        frontend_socket: Front,
        frontend_token: Token,
        listener: Rc<RefCell<L>>,
        pool: Weak<RefCell<Pool>>,
        protocol: Protocol,
        public_address: SocketAddr,
        request_id: Ulid,
        session_address: Option<SocketAddr>,
        sticky_name: String,
    ) -> Result<Http<Front, L>, AcceptError> {
        let (front_buffer, back_buffer) = match pool.upgrade() {
            Some(pool) => {
                let mut pool = pool.borrow_mut();
                match (pool.checkout(), pool.checkout()) {
                    (Some(front_buffer), Some(back_buffer)) => (front_buffer, back_buffer),
                    _ => return Err(AcceptError::BufferCapacityReached),
                }
            }
            None => return Err(AcceptError::BufferCapacityReached),
        };
        Ok(Http {
            answers,
            backend_connection_status: BackendConnectionStatus::NotConnected,
            backend_readiness: Readiness::new(),
            backend_socket: None,
            backend_stop: None,
            backend_token: None,
            backend: None,
            configured_backend_timeout,
            configured_connect_timeout,
            configured_frontend_timeout,
            connection_attempts: 0,
            container_backend_timeout: TimeoutContainer::new_empty(configured_connect_timeout),
            container_frontend_timeout,
            frontend_readiness: Readiness {
                interest: Ready::READABLE | Ready::HUP | Ready::ERROR,
                event: Ready::EMPTY,
            },
            frontend_socket,
            frontend_token,
            keepalive_count: 0,
            listener,
            request_stream: GenericHttpStream::new(
                kawa::Kind::Request,
                kawa::Buffer::new(front_buffer),
            ),
            response_stream: ResponseStream::BackendAnswer(GenericHttpStream::new(
                kawa::Kind::Response,
                kawa::Buffer::new(back_buffer),
            )),
            context: HttpContext {
                id: request_id,
                backend_id: None,
                cluster_id: None,

                closing: false,
                keep_alive_backend: true,
                keep_alive_frontend: true,
                protocol,
                public_address,
                session_address,
                sticky_name,
                sticky_session: None,
                sticky_session_found: None,

                method: None,
                authority: None,
                path: None,
                status: None,
                reason: None,
                user_agent: None,
            },
            // Initialize forward proxy fields
            is_forward_proxy_request: false,
            forward_target_scheme: None,
            forward_target_host: None,
            forward_target_port: None,
            is_connect_tunnel: false,
            connect_response_buffer: None,
            // Init cache fields
            serving_from_cache: None,
            current_cache_key: None,
        })
    }

    /// Reset the connection in case of keep-alive to be ready for the next request
    pub fn reset(&mut self) {
        trace!("{} ============== reset", log_context!(self));
        let response_stream = match &mut self.response_stream {
            ResponseStream::BackendAnswer(response_stream) => response_stream,
            _ => return,
        };

        self.context.id = Ulid::generate();
        self.context.reset();

        // Reset forward proxy specific fields for keep-alive
        self.is_forward_proxy_request = false;
        self.forward_target_scheme = None;
        self.forward_target_host = None;
        self.forward_target_port = None;
        self.is_connect_tunnel = false; // Should already be false if not in a tunnel
        self.connect_response_buffer = None;
        // Reset cache fields
        self.serving_from_cache = None;
        self.current_cache_key = None;


        self.request_stream.clear();
        response_stream.clear();
        self.keepalive_count += 1;
        gauge_add!("http.active_requests", -1);

        if let Some(backend) = &mut self.backend {
            let mut backend = backend.borrow_mut();
            backend.active_requests = backend.active_requests.saturating_sub(1);
        }

        // reset the front timeout and cancel the back timeout while we are
        // waiting for a new request
        self.container_backend_timeout.cancel();
        self.container_frontend_timeout
            .set_duration(self.configured_frontend_timeout);
        self.frontend_readiness.interest = Ready::READABLE | Ready::HUP | Ready::ERROR;
        self.backend_readiness.interest = Ready::HUP | Ready::ERROR;

        // We are resetting the offset of request and response stream buffers
        // We do have to keep cursor position on the request, if there is data
        // in the request stream to preserve http pipelining.

        // Print the left-over response buffer output to track in which case it
        // may happens
        let response_storage = &mut response_stream.storage;
        if !response_storage.is_empty() {
            warn!(
                "{} Leftover fragment from response: {}",
                log_context!(self),
                parser::view(
                    response_storage.used(),
                    16,
                    &[response_storage.start, response_storage.end,],
                )
            );
        }

        response_storage.clear();
        if !self.request_stream.storage.is_empty() {
            self.frontend_readiness.event.insert(Ready::READABLE);
        } else {
            self.request_stream.storage.clear();
        }
    }

    pub fn readable(&mut self, metrics: &mut SessionMetrics) -> StateResult {
        trace!("{} ============== readable", log_context!(self));
        if !self.container_frontend_timeout.reset() {
            error!(
                "could not reset front timeout {:?}",
                self.configured_frontend_timeout
            );
            self.print_state(self.protocol_string());
        }

        let response_stream = match &mut self.response_stream {
            ResponseStream::BackendAnswer(response_stream) => response_stream,
            ResponseStream::DefaultAnswer(..) => {
                error!(
                    "{} Sending default answer, should not read from frontend socket",
                    log_context!(self)
                );

                self.frontend_readiness.interest.remove(Ready::READABLE);
                self.frontend_readiness.interest.insert(Ready::WRITABLE);
                return StateResult::Continue;
            }
        };

        if self.request_stream.storage.is_full() {
            self.frontend_readiness.interest.remove(Ready::READABLE);
            if self.request_stream.is_main_phase() {
                self.backend_readiness.interest.insert(Ready::WRITABLE);
            } else {
                // client has filled its buffer and we can't empty it
                self.set_answer(DefaultAnswer::Answer413 {
                    capacity: self.request_stream.storage.capacity(),
                    phase: self.request_stream.parsing_phase.marker(),
                    message: diagnostic_413_507(self.request_stream.parsing_phase),
                });
            }
            return StateResult::Continue;
        }

        let (size, socket_state) = self
            .frontend_socket
            .socket_read(self.request_stream.storage.space());

        debug!("{} Read {} bytes", log_context!(self), size);

        if size > 0 {
            self.request_stream.storage.fill(size);
            count!("bytes_in", size as i64);
            metrics.bin += size;
            // if self.kawa_request.storage.is_full() {
            //     self.frontend_readiness.interest.remove(Ready::READABLE);
            // }
        } else {
            self.frontend_readiness.event.remove(Ready::READABLE);
        }

        match socket_state {
            SocketResult::Error | SocketResult::Closed => {
                if self.request_stream.is_initial() {
                    // count an error if we were waiting for the first request
                    // otherwise, if we already had one completed request and response,
                    // and are waiting for the next one, we do not count a socket
                    // closing abruptly as an error
                    if self.keepalive_count == 0 {
                        self.frontend_socket.read_error();
                    }
                } else {
                    self.frontend_socket.read_error();
                    self.log_request_error(
                        metrics,
                        &format!(
                            "front socket {socket_state:?}, closing the session. Readiness: {:?} -> {:?}, read {size} bytes",
                            self.frontend_readiness,
                            self.backend_readiness,
                        )
                    );
                }
                return StateResult::CloseSession;
            }
            SocketResult::WouldBlock => {
                self.frontend_readiness.event.remove(Ready::READABLE);
            }
            SocketResult::Continue => {}
        };

        trace!("{} ============== readable_parse", log_context!(self));
        let was_initial = self.request_stream.is_initial();
        let was_not_proxying = !self.request_stream.is_main_phase();

        kawa::h1::parse(&mut self.request_stream, &mut self.context);
        // kawa::debug_kawa(&self.request_stream);

        // --- BEGIN CACHING LOOKUP ---
        // Only attempt cache lookup if not already serving from cache, not in tunnel mode, and is GET.
        if self.serving_from_cache.is_none() && !self.is_connect_tunnel && self.context.method == Some(Method::Get) {
            // Simplified Cache-Control check (client headers)
            let mut bypass_cache = false;
            for (name, value) in self.request_stream.detached.headers.iter() {
                if name.eq_ignore_ascii_case(b"Cache-Control") && value.eq_ignore_ascii_case(b"no-cache") {
                    bypass_cache = true;
                    info!("{} Client sent Cache-Control: no-cache. Bypassing cache lookup.", log_context!(self));
                    break;
                }
                if name.eq_ignore_ascii_case(b"Pragma") && value.eq_ignore_ascii_case(b"no-cache") {
                    bypass_cache = true;
                    info!("{} Client sent Pragma: no-cache. Bypassing cache lookup.", log_context!(self));
                    break;
                }
            }

            if !bypass_cache {
                // Construct cache key based on the potentially *original* (pre-normalization) path/authority from context,
                // as this is before the forward proxy logic modifies them for upstream request.
                // However, for consistency, it's better to form the key *after* normalization if possible,
                // or ensure normalization is idempotent for cache key generation.
                // For now, using potentially original values from context.path (if absolute) or context.authority + context.path.

                let key_uri_str = if self.context.path.as_deref().unwrap_or("").contains("://") {
                    self.context.path.as_deref().unwrap_or("").to_string()
                } else if let Some(authority) = self.context.authority.as_deref() {
                    // Attempt to reconstruct. This assumes "http" if no scheme was part of absolute URI path.
                    // This part is tricky because self.forward_target_scheme is set later.
                    // For now, we rely on the absolute URI detection to have set self.is_forward_proxy_request
                    // and the forward_target_scheme if path was absolute.
                    // If path was not absolute, this key might be less effective or incorrect for caching.
                    // Let's refine this: the key for lookup should ideally be what the *client sent* as target.
                    let scheme_prefix = if self.context.protocol == Protocol::HTTPS { "https://" } else { "http://" };
                    format!("{}{}{}", scheme_prefix, authority, self.context.path.as_deref().unwrap_or("/"))
                } else {
                    String::new() // Cannot form a good key
                };

                if !key_uri_str.is_empty() {
                    let key = format!("GET::{}", key_uri_str);
                    // self.current_cache_key is set *after* full URI normalization, before connecting to origin.
                    // Here we just use the generated key for lookup.

                    if let Ok(mut cache) = HTTP_FORWARD_CACHE.lock() {
                        if let Some(cached_item) = cache.get(&key) {
                            info!("{} Cache HIT for key: {}", log_context!(self), key);
                            self.serving_from_cache = Some(cached_item.clone());
                            // Bypassing further request processing and backend connection.
                            // Ensure writable() will be called to serve this.
                            self.frontend_readiness.interest.insert(Ready::WRITABLE);
                            // No need to read further from client for this request.
                            self.frontend_readiness.interest.remove(Ready::READABLE); 
                            // Clear events that might have been processed before this point in the loop
                            self.frontend_readiness.event = Ready::EMPTY; 
                            self.backend_readiness.event = Ready::EMPTY;
                            return StateResult::Continue; // Let writable() handle it.
                        } else {
                            info!("{} Cache MISS for key: {}", log_context!(self), key);
                        }
                    }
                }
            }
        }
        // --- END CACHING LOOKUP ---

        // --- BEGIN FORWARD PROXY LOGIC (Absolute URI and CONNECT detection) ---
        // This includes URI normalization which is important for generating the definitive cache key later.
        // This runs after kawa::h1::parse, so self.context fields are populated.
        // We only do this if it's not already marked as a forward request (e.g., by CONNECT later)
        // and if we are in a state where headers are parsed or being parsed.
        if !self.is_forward_proxy_request && // Not already marked by CONNECT
           (self.request_stream.is_header_phase() || self.request_stream.is_main_phase()) {

            let can_be_absolute_uri_forward = 
                // Condition 1: No cluster was assigned by router (implicit forward proxy mode)
                self.context.cluster_id.is_none() || 
                // Condition 2: Or, it's an HTTP/0.9-like simple request where authority might be missing
                // and path IS the absolute URI. (This part is more speculative for Sozu's typical use)
                (self.context.authority.is_none() && self.context.path.as_ref().map_or(false, |p| p.contains("://")));

            info!(
                "{} Post-parse context. Authority: {:?}, Path: {:?}",
                log_context!(self),
                self.context.authority,
                self.context.path
            );

            if let Some(request_path_str) = self.context.path.as_deref() {
                // Basic check for "scheme://" to identify potential absolute URIs.
                // Act on it if it's a potential forward proxy scenario (no cluster OR path is absolute URI and no authority)
                if request_path_str.contains("://") && can_be_absolute_uri_forward {
                    match Url::parse(request_path_str) {
                        Ok(parsed_url) => {
                            let scheme = parsed_url.scheme().to_lowercase();
                            if (scheme == "http" || scheme == "https") && parsed_url.host_str().is_some() {
                                info!("{} Possible absolute URI forward detected (cluster_id: {:?}, authority: {:?}). Original path: {}", 
                                    log_context!(self), self.context.cluster_id, self.context.authority, request_path_str);

                                self.is_forward_proxy_request = true; // Mark as forward proxy request
                                self.forward_target_scheme = Some(scheme.clone());
                                self.forward_target_host = parsed_url.host_str().map(|s| s.to_string());
                                self.forward_target_port = parsed_url.port_or_known_default();

                                // Update self.context.path to be the path & query part for the upstream.
                                let upstream_path = if parsed_url.path().is_empty() { "/".to_string() } else { parsed_url.path().to_string() };
                                let final_upstream_path = if let Some(query) = parsed_url.query() {
                                    format!("{}?{}", upstream_path, query)
                                } else {
                                    upstream_path
                                };
                                self.context.path = Some(final_upstream_path);

                                // Update self.context.authority to the host:port from the absolute URI.
                                // This is critical for correct Host header generation if Sozu modifies/sends one,
                                // and for consistent logging.
                                let host_from_url = self.forward_target_host.as_ref().unwrap(); // Safe due to host_str().is_some()
                                self.context.authority = if let Some(port) = self.forward_target_port {
                                    if (scheme == "http" && port != 80) || (scheme == "https" && port != 443) {
                                        Some(format!("{}:{}", host_from_url, port))
                                    } else {
                                        Some(host_from_url.clone())
                                    }
                                } else {
                                    Some(host_from_url.clone())
                                };

                                info!(
                                    "{} Absolute URI forward request. Original: '{}'. Parsed Target -> Scheme: {:?}, Host: {:?}, Port: {:?}. Context Path set to: {:?}. Context Authority set to: {:?}",
                                    log_context!(self),
                                    request_path_str,
                                    self.forward_target_scheme,
                                    self.forward_target_host,
                                    self.forward_target_port,
                                    self.context.path,
                                    self.context.authority
                                );
                            } else {
                                // Parsed but not http/https or no host, not a valid forward target.
                                info!("{} Parsed URI '{}' but scheme ('{}') is not http/https or host is missing. Not treating as forward proxy.", log_context!(self), request_path_str, scheme);
                            }
                        }
                        Err(e) => {
                            // Looked like an absolute URI but failed to parse.
                            warn!("{} Failed to parse suspected absolute URI '{}': {}. Proceeding with normal reverse-proxy/backend logic.", log_context!(self), request_path_str, e);
                        }
                    }
                }
            }
        }
        // --- END FORWARD PROXY LOGIC (Absolute URI detection) ---

        // --- BEGIN FORWARD PROXY LOGIC (CONNECT detection) ---
        if !self.is_forward_proxy_request && // Not already identified as absolute URI forward
           (self.request_stream.is_header_phase() || self.request_stream.is_main_phase()) &&
           self.context.method == Some(Method::Connect) {
            
            info!("{} CONNECT request detected.", log_context!(self));
            self.is_forward_proxy_request = true; // Mark that we are handling a forward proxy style request

            if let Some(authority_str) = self.context.authority.as_deref() {
                let mut parts = authority_str.splitn(2, ':');
                if let Some(host) = parts.next() {
                    self.forward_target_host = Some(host.to_string());
                    if let Some(port_str) = parts.next() {
                        match port_str.parse::<u16>() {
                            Ok(port) => self.forward_target_port = Some(port),
                            Err(_) => {
                                warn!("{} Invalid port in CONNECT authority: {}", log_context!(self), authority_str);
                                self.set_answer(DefaultAnswer::Answer400 { message: "Invalid port in CONNECT authority".into(), phase: self.request_stream.parsing_phase.marker(), successfully_parsed: "".into(), partially_parsed: "".into(), invalid: authority_str.to_string() });
                                return StateResult::Continue; // Proceed to send 400
                            }
                        }
                    } else {
                        // Default to 443 for CONNECT if no port is specified
                        self.forward_target_port = Some(443); 
                        info!("{} No port in CONNECT authority '{}', defaulting to 443.", log_context!(self), authority_str);
                    }
                    self.forward_target_scheme = Some("tcp".to_string()); // Scheme is implicitly tcp for the tunnel

                    info!(
                        "{} CONNECT target parsed. Host: {:?}, Port: {:?}",
                        log_context!(self),
                        self.forward_target_host,
                        self.forward_target_port
                    );
                    // Proceed to connection phase via ConnectBackend state
                } else {
                    warn!("{} Invalid CONNECT authority: {}", log_context!(self), authority_str);
                    self.set_answer(DefaultAnswer::Answer400 { message: "Invalid CONNECT authority".into(), phase: self.request_stream.parsing_phase.marker(), successfully_parsed: "".into(), partially_parsed: "".into(), invalid: authority_str.to_string() });
                    return StateResult::Continue; // Proceed to send 400
                }
            } else {
                warn!("{} CONNECT request missing authority.", log_context!(self));
                self.set_answer(DefaultAnswer::Answer400 { message: "CONNECT request missing authority".into(), phase: self.request_stream.parsing_phase.marker(), successfully_parsed: "".into(), partially_parsed: "".into(), invalid: "".to_string() });
                return StateResult::Continue; // Proceed to send 400
            }
        }
        // --- END FORWARD PROXY LOGIC (CONNECT detection) ---

        if was_initial && !self.request_stream.is_initial() {
            // if it was the first request, the front timeout duration
            // was set to request_timeout, which is much lower. For future
            // requests on this connection, we can wait a bit more
            self.container_frontend_timeout
                .set_duration(self.configured_frontend_timeout);
            gauge_add!("http.active_requests", 1);
            incr!("http.requests");
        }

        if let kawa::ParsingPhase::Error { marker, kind } = self.request_stream.parsing_phase {
            incr!("http.frontend_parse_errors");
            warn!(
                "{} Parsing request error in {:?}: {}",
                log_context!(self),
                marker,
                match kind {
                    kawa::ParsingErrorKind::Consuming { index } => {
                        let kawa = &self.request_stream;
                        parser::view(
                            kawa.storage.used(),
                            16,
                            &[
                                kawa.storage.start,
                                kawa.storage.head,
                                index as usize,
                                kawa.storage.end,
                            ],
                        )
                    }
                    kawa::ParsingErrorKind::Processing { message } => message.to_owned(),
                }
            );
            if response_stream.consumed {
                self.log_request_error(metrics, "Parsing error on the request");
                return StateResult::CloseSession;
            } else {
                let (message, successfully_parsed, partially_parsed, invalid) =
                    diagnostic_400_502(marker, kind, &self.request_stream);
                self.set_answer(DefaultAnswer::Answer400 {
                    message,
                    phase: marker,
                    successfully_parsed,
                    partially_parsed,
                    invalid,
                });
                return StateResult::Continue;
            }
        }

        if self.request_stream.is_main_phase() {
            self.backend_readiness.interest.insert(Ready::WRITABLE);
            if was_not_proxying {
                // Sozu tries to connect only once all the headers were gathered and edited
                // this could be improved
                // If serving from cache, we don't connect to backend.
                if self.serving_from_cache.is_some() {
                    trace!("{} Serving from cache, skipping backend connection.", log_context!(self));
                            self.frontend_readiness.interest.insert(Ready::WRITABLE); // Ensure writable to serve from cache
                } else {
                            // Not serving from cache, proceed to connect to backend.
                            // Now is the time to set the definitive current_cache_key for potential storage later.
                            if self.is_forward_proxy_request && self.context.method == Some(Method::Get) && !self.is_connect_tunnel {
                                if let (Some(scheme), Some(host), Some(port), Some(path)) = (
                                    self.forward_target_scheme.as_deref(),
                                    self.forward_target_host.as_deref(),
                                    self.forward_target_port,
                                    self.context.path.as_deref(), // Use the normalized path
                                ) {
                                    let full_uri_for_cache = format!("{}://{}:{}{}", scheme, host, port, path);
                                    self.current_cache_key = Some(format!("GET::{}", full_uri_for_cache));
                                    info!("{} Cache key set for potential storage: {:?}", log_context!(self), self.current_cache_key);
                                }
                            } else if self.context.method == Some(Method::Get) && !self.is_connect_tunnel {
                                // Fallback for non-absolute URI but potentially cacheable reverse proxy GETs (if desired later)
                                // For now, this path means current_cache_key might remain None if not absolute forward.
                            }


                    trace!("{} ============== HANDLE CONNECTION!", log_context!(self));
                    return StateResult::ConnectBackend;
                }
            }
        }
        if self.request_stream.is_terminated() {
            self.frontend_readiness.interest.remove(Ready::READABLE);
        }

        StateResult::Continue
    }

    pub fn writable(&mut self, metrics: &mut SessionMetrics) -> StateResult {
        trace!("{} ============== writable", log_context!(self));
        let response_stream = match &mut self.response_stream {
            ResponseStream::BackendAnswer(response_stream) => {
                // --- BEGIN CACHING (Population) ---
                // This logic should run when the response from origin is complete.
                if response_stream.is_terminated() && response_stream.is_completed() {
                    if let Some(key_to_cache) = self.current_cache_key.take() { // .take() to only attempt cache once per request
                        if self.context.status == Some(200) { // Only cache 200 OK for now
                            let mut can_cache_response = true;
                            let mut extracted_headers = Vec::new();

                            // Check server response headers for Cache-Control directives
                            for header in response_stream.detached.headers.iter() {
                                let name_str = String::from_utf8_lossy(header.name());
                                let value_str = String::from_utf8_lossy(header.value());

                                if name_str.eq_ignore_ascii_case("Cache-Control") {
                                    if value_str.contains("no-store") || value_str.contains("private") {
                                        can_cache_response = false;
                                        info!("{} Origin response for key '{}' contains Cache-Control: no-store/private. Not caching.", log_context!(self), key_to_cache);
                                        break;
                                    }
                                    // Basic max-age=0 check (more complex parsing needed for full support)
                                    if value_str.contains("max-age=0") {
                                         can_cache_response = false;
                                         info!("{} Origin response for key '{}' contains Cache-Control: max-age=0. Not caching.", log_context!(self), key_to_cache);
                                         break;
                                    }
                                }
                                // TODO: Could also check 'Pragma: no-cache' from origin, though less common for responses.
                                // TODO: Could check 'Expires' header for very old dates.

                                extracted_headers.push((name_str.into_owned(), value_str.into_owned()));
                            }

                            if can_cache_response {
                                let body_to_cache = response_stream.storage.used().to_vec();
                                let item_to_cache = CachedResponse {
                                    status: 200, // We already checked self.context.status == Some(200)
                                    headers: extracted_headers,
                                    body: body_to_cache,
                                };
                                if let Ok(mut cache) = HTTP_FORWARD_CACHE.lock() {
                                    cache.put(key_to_cache.clone(), item_to_cache);
                                    info!("{} Response for key {} cached.", log_context!(self), key_to_cache);
                                } else {
                                    error!("{} Failed to lock cache for writing.", log_context!(self));
                                }
                            }
                        } else {
                             info!("{} Response status {:?} for key {:?} not 200 OK. Not caching.", log_context!(self), self.context.status, key_to_cache);
                        }
                    }
                }
                // --- END CACHING (Population) ---
                response_stream // return for outer match
            },
            _ => return self.writable_default_answer(metrics),
        };

        response_stream.prepare(&mut kawa::h1::BlockConverter);

        let bufs = response_stream.as_io_slice();
        if bufs.is_empty() && !self.frontend_socket.socket_wants_write() {
            self.frontend_readiness.interest.remove(Ready::WRITABLE);
            // do not shortcut, response might have been terminated without anything more to send
        }

        let (size, socket_state) = self.frontend_socket.socket_write_vectored(&bufs);

        debug!("{} Wrote {} bytes", log_context!(self), size);

        if size > 0 {
            response_stream.consume(size);
            count!("bytes_out", size as i64);
            metrics.bout += size;
            self.backend_readiness.interest.insert(Ready::READABLE);
        }

        match socket_state {
            SocketResult::Error | SocketResult::Closed => {
                self.frontend_socket.write_error();
                self.log_request_error(
                    metrics,
                    &format!(
                        "front socket {socket_state:?}, closing session.  Readiness: {:?} -> {:?}, read {size} bytes",
                        self.frontend_readiness,
                        self.backend_readiness,
                    ),
                );
                return StateResult::CloseSession;
            }
            SocketResult::WouldBlock => {
                self.frontend_readiness.event.remove(Ready::WRITABLE);
            }
            SocketResult::Continue => {}
        }

        if self.frontend_socket.socket_wants_write() {
            return StateResult::Continue;
        }

        if response_stream.is_terminated() && response_stream.is_completed() {
            if self.context.closing {
                debug!("{} closing proxy, no keep alive", log_context!(self));
                self.log_request_success(metrics);
                return StateResult::CloseSession;
            }

            match response_stream.detached.status_line {
                kawa::StatusLine::Response { code: 101, .. } => {
                    trace!("{} ============== HANDLE UPGRADE!", log_context!(self));
                    self.log_request_success(metrics);
                    return StateResult::Upgrade;
                }
                kawa::StatusLine::Response { code: 100, .. } => {
                    trace!("{} ============== HANDLE CONTINUE!", log_context!(self));
                    response_stream.clear();
                    self.log_request_success(metrics);
                    return StateResult::Continue;
                }
                kawa::StatusLine::Response { code: 103, .. } => {
                    self.backend_readiness.event.insert(Ready::READABLE);
                    trace!("{} ============== HANDLE EARLY HINT!", log_context!(self));
                    response_stream.clear();
                    self.log_request_success(metrics);
                    return StateResult::Continue;
                }
                _ => (),
            }

            let response_length_known = response_stream.body_size != kawa::BodySize::Empty;
            let request_length_known = self.request_stream.body_size != kawa::BodySize::Empty;
            if !(self.request_stream.is_terminated() && self.request_stream.is_completed())
                && request_length_known
            {
                error!(
                    "{} Response terminated before request, this case is not handled properly yet",
                    log_context!(self)
                );
                incr!("http.early_response_close");
                // FIXME: this will cause problems with pipelining
                // return StateResult::CloseSession;
            }

            // FIXME: we could get smarter about this
            // with no keepalive on backend, we could open a new backend ConnectionError
            // with no keepalive on front but keepalive on backend, we could have
            // a pool of connections
            trace!(
                "{} ============== HANDLE KEEP-ALIVE: {} {} {}",
                log_context!(self),
                self.context.keep_alive_frontend,
                self.context.keep_alive_backend,
                response_length_known
            );

            self.log_request_success(metrics);
            return match (
                self.context.keep_alive_frontend,
                self.context.keep_alive_backend,
                response_length_known,
            ) {
                (true, true, true) => {
                    debug!("{} Keep alive frontend/backend", log_context!(self));
                    metrics.reset();
                    self.reset();
                    StateResult::Continue
                }
                (true, false, true) => {
                    debug!("{} Keep alive frontend", log_context!(self));
                    metrics.reset();
                    self.reset();
                    StateResult::CloseBackend
                }
                _ => {
                    debug!("{} No keep alive", log_context!(self));
                    StateResult::CloseSession
                }
            };
        }
        StateResult::Continue
    }

    fn writable_default_answer(&mut self, metrics: &mut SessionMetrics) -> StateResult {
        trace!(
            "{} ============== writable_default_answer",
            log_context!(self)
        );
        let response_stream = match &mut self.response_stream {
            ResponseStream::DefaultAnswer(_, response_stream) => response_stream,
            _ => return StateResult::CloseSession,
        };
        let bufs = response_stream.as_io_slice();
        let (size, socket_state) = self.frontend_socket.socket_write_vectored(&bufs);

        count!("bytes_out", size as i64);
        metrics.bout += size;
        response_stream.consume(size);

        if size == 0 || socket_state != SocketResult::Continue {
            self.frontend_readiness.event.remove(Ready::WRITABLE);
        }

        if response_stream.is_completed() {
            save_http_status_metric(self.context.status, self.context.log_context());
            self.log_default_answer_success(metrics);
            self.frontend_readiness.reset();
            self.backend_readiness.reset();
            return StateResult::CloseSession;
        }

        if socket_state == SocketResult::Error {
            self.frontend_socket.write_error();
            self.log_request_error(
                metrics,
                "error writing default answer to front socket, closing",
            );
            StateResult::CloseSession
        } else {
            StateResult::Continue
        }
    }

    pub fn backend_writable(&mut self, metrics: &mut SessionMetrics) -> SessionResult {
        trace!("{} ============== backend_writable", log_context!(self));
        if let ResponseStream::DefaultAnswer(..) = self.response_stream {
            error!(
                "{}\tsending default answer, should not write to back",
                log_context!(self)
            );
            self.backend_readiness.interest.remove(Ready::WRITABLE);
            self.frontend_readiness.interest.insert(Ready::WRITABLE);
            return SessionResult::Continue;
        }

        let backend_socket = if let Some(backend_socket) = &mut self.backend_socket {
            backend_socket
        } else {
            self.log_request_error(metrics, "back socket not found, closing session");
            return SessionResult::Close;
        };

        self.request_stream.prepare(&mut kawa::h1::BlockConverter);

        let bufs = self.request_stream.as_io_slice();
        if bufs.is_empty() {
            self.backend_readiness.interest.remove(Ready::WRITABLE);
            return SessionResult::Continue;
        }

        let (size, socket_state) = backend_socket.socket_write_vectored(&bufs);
        debug!("{} Wrote {} bytes", log_context!(self), size);

        if size > 0 {
            self.request_stream.consume(size);
            count!("back_bytes_out", size as i64);
            metrics.backend_bout += size;
            self.frontend_readiness.interest.insert(Ready::READABLE);
            self.backend_readiness.interest.insert(Ready::READABLE);
        } else {
            self.backend_readiness.event.remove(Ready::WRITABLE);
        }

        match socket_state {
            // the back socket is not writable anymore, so we can drop
            // the front buffer, no more data can be transmitted.
            // But the socket might still be readable, or if it is
            // closed, we might still have some data in the buffer.
            // As an example, we can get an early response for a large
            // POST request to refuse it and prevent all of the data
            // from being transmitted, with the backend server closing
            // the socket right after sending the response
            // FIXME: shouldn't we test for response_state then?
            SocketResult::Error | SocketResult::Closed => {
                self.frontend_readiness.interest.remove(Ready::READABLE);
                self.backend_readiness.interest.remove(Ready::WRITABLE);
                return SessionResult::Continue;
            }
            SocketResult::WouldBlock => {
                self.backend_readiness.event.remove(Ready::WRITABLE);
            }
            SocketResult::Continue => {}
        }

        if self.request_stream.is_terminated() && self.request_stream.is_completed() {
            self.backend_readiness.interest.remove(Ready::WRITABLE);

            // cancel the front timeout while we are waiting for the server to answer
            self.container_frontend_timeout.cancel();
            self.container_backend_timeout.reset();
        }
        SessionResult::Continue
    }

    // Read content from cluster
    pub fn backend_readable(&mut self, metrics: &mut SessionMetrics) -> SessionResult {
        trace!("{} ============== backend_readable", log_context!(self));
        if !self.container_backend_timeout.reset() {
            error!(
                "{} Could not reset back timeout {:?}",
                log_context!(self),
                self.configured_backend_timeout
            );
            self.print_state(self.protocol_string());
        }

        let response_stream = match &mut self.response_stream {
            ResponseStream::BackendAnswer(response_stream) => response_stream,
            _ => {
                error!(
                    "{} Sending default answer, should not read from backend socket",
                    log_context!(self),
                );

                self.backend_readiness.interest.remove(Ready::READABLE);
                self.frontend_readiness.interest.insert(Ready::WRITABLE);
                return SessionResult::Continue;
            }
        };

        let backend_socket = if let Some(backend_socket) = &mut self.backend_socket {
            backend_socket
        } else {
            self.log_request_error(metrics, "back socket not found, closing session");
            return SessionResult::Close;
        };

        if response_stream.storage.is_full() {
            self.backend_readiness.interest.remove(Ready::READABLE);
            if response_stream.is_main_phase() {
                self.frontend_readiness.interest.insert(Ready::WRITABLE);
            } else {
                // server has filled its buffer and we can't empty it
                let capacity = response_stream.storage.capacity();
                let phase = response_stream.parsing_phase.marker();
                let message = diagnostic_413_507(response_stream.parsing_phase);
                self.set_answer(DefaultAnswer::Answer507 {
                    capacity,
                    phase,
                    message,
                });
            }
            return SessionResult::Continue;
        }

        let (size, socket_state) = backend_socket.socket_read(response_stream.storage.space());
        debug!("{} Read {} bytes", log_context!(self), size);

        if size > 0 {
            response_stream.storage.fill(size);
            count!("back_bytes_in", size as i64);
            metrics.backend_bin += size;
            // if self.kawa_response.storage.is_full() {
            //     self.backend_readiness.interest.remove(Ready::READABLE);
            // }

            // In case the response starts while the request is not tagged as "terminated",
            // we place the timeout responsibility on the backend.
            // This can happen when:
            // - the request is malformed and doesn't have length information
            // - kawa fails to detect a properly terminated request (e.g. a GET request with no body and no length)
            // - the response can start before the end of the request (e.g. stream processing like compression)
            self.container_frontend_timeout.cancel();
        } else {
            self.backend_readiness.event.remove(Ready::READABLE);
        }

        // TODO: close delimited and backend_hup should be handled better
        match socket_state {
            SocketResult::Error => {
                backend_socket.read_error();
                self.log_request_error(
                    metrics,
                    &format!(
                        "back socket {socket_state:?}, closing session.  Readiness: {:?} -> {:?}, read {size} bytes",
                        self.frontend_readiness,
                        self.backend_readiness,
                    ),
                );
                return SessionResult::Close;
            }
            SocketResult::WouldBlock | SocketResult::Closed => {
                self.backend_readiness.event.remove(Ready::READABLE);
            }
            SocketResult::Continue => {}
        }

        trace!(
            "{} ============== backend_readable_parse",
            log_context!(self)
        );
        kawa::h1::parse(response_stream, &mut self.context);
        // kawa::debug_kawa(&self.response_stream);

        if let kawa::ParsingPhase::Error { marker, kind } = response_stream.parsing_phase {
            incr!("http.backend_parse_errors");
            warn!(
                "{} Parsing response error in {:?}: {}",
                log_context!(self),
                marker,
                match kind {
                    kawa::ParsingErrorKind::Consuming { index } => {
                        parser::view(
                            response_stream.storage.used(),
                            16,
                            &[
                                response_stream.storage.start,
                                response_stream.storage.head,
                                index as usize,
                                response_stream.storage.end,
                            ],
                        )
                    }
                    kawa::ParsingErrorKind::Processing { message } => message.to_owned(),
                }
            );
            if response_stream.consumed {
                return SessionResult::Close;
            } else {
                let (message, successfully_parsed, partially_parsed, invalid) =
                    diagnostic_400_502(marker, kind, response_stream);
                self.set_answer(DefaultAnswer::Answer502 {
                    message,
                    phase: marker,
                    successfully_parsed,
                    partially_parsed,
                    invalid,
                });
                return SessionResult::Continue;
            }
        }

        if response_stream.is_main_phase() {
            self.frontend_readiness.interest.insert(Ready::WRITABLE);
        }
        if response_stream.is_terminated() {
            metrics.backend_stop();
            self.backend_stop = Some(Instant::now());
            self.backend_readiness.interest.remove(Ready::READABLE);
        }
        SessionResult::Continue
    }
}

impl<Front: SocketHandler, L: ListenerHandler + L7ListenerHandler> Http<Front, L> {
    fn log_endpoint(&self) -> EndpointRecord {
        EndpointRecord::Http {
            method: self.context.method.as_deref(),
            authority: self.context.authority.as_deref(),
            path: self.context.path.as_deref(),
            reason: self.context.reason.as_deref(),
            status: self.context.status,
        }
    }

    pub fn get_session_address(&self) -> Option<SocketAddr> {
        self.context
            .session_address
            .or_else(|| self.frontend_socket.socket_ref().peer_addr().ok())
    }

    pub fn get_backend_address(&self) -> Option<SocketAddr> {
        self.backend
            .as_ref()
            .map(|backend| backend.borrow().address)
            .or_else(|| {
                self.backend_socket
                    .as_ref()
                    .and_then(|backend| backend.peer_addr().ok())
            })
    }

    // The protocol name used in the access logs
    fn protocol_string(&self) -> &'static str {
        match self.context.protocol {
            Protocol::HTTP => "HTTP",
            Protocol::HTTPS => match self.frontend_socket.protocol() {
                TransportProtocol::Ssl2 => "HTTPS-SSL2",
                TransportProtocol::Ssl3 => "HTTPS-SSL3",
                TransportProtocol::Tls1_0 => "HTTPS-TLS1.0",
                TransportProtocol::Tls1_1 => "HTTPS-TLS1.1",
                TransportProtocol::Tls1_2 => "HTTPS-TLS1.2",
                TransportProtocol::Tls1_3 => "HTTPS-TLS1.3",
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    /// Format the context of the websocket into a loggable String
    pub fn websocket_context(&self) -> WebSocketContext {
        WebSocketContext::Http {
            method: self.context.method.clone(),
            authority: self.context.authority.clone(),
            path: self.context.path.clone(),
            reason: self.context.reason.clone(),
            status: self.context.status,
        }
    }

    pub fn log_request(&self, metrics: &SessionMetrics, error: bool, message: Option<&str>) {
        let listener = self.listener.borrow();
        let tags = self.context.authority.as_ref().and_then(|host| {
            let hostname = match host.split_once(':') {
                None => host,
                Some((hostname, _)) => hostname,
            };
            listener.get_tags(hostname)
        });

        let context = self.context.log_context();
        metrics.register_end_of_session(&context);

        log_access! {
            error,
            on_failure: { incr!("unsent-access-logs") },
            message: message,
            context,
            session_address: self.get_session_address(),
            backend_address: self.get_backend_address(),
            protocol: self.protocol_string(),
            endpoint: self.log_endpoint(),
            tags,
            client_rtt: socket_rtt(self.front_socket()),
            server_rtt: self.backend_socket.as_ref().and_then(socket_rtt),
            service_time: metrics.service_time(),
            response_time: metrics.backend_response_time(),
            request_time: metrics.request_time(),
            bytes_in: metrics.bin,
            bytes_out: metrics.bout,
            user_agent: self.context.user_agent.as_deref(),
        };
    }

    pub fn log_request_success(&self, metrics: &SessionMetrics) {
        save_http_status_metric(self.context.status, self.context.log_context());
        self.log_request(metrics, false, None);
    }

    pub fn log_default_answer_success(&self, metrics: &SessionMetrics) {
        self.log_request(metrics, false, None);
    }
    pub fn log_request_error(&self, metrics: &mut SessionMetrics, message: &str) {
        incr!("http.errors");
        error!(
            "{} Could not process request properly got: {}",
            log_context!(self),
            message
        );
        self.print_state(self.protocol_string());
        self.log_request(metrics, true, Some(message));
    }

    pub fn set_answer(&mut self, answer: DefaultAnswer) {
        let status = u16::from(&answer);
        if let ResponseStream::DefaultAnswer(old_status, ..) = self.response_stream {
            error!(
                "already set the default answer to {}, trying to set to {}",
                old_status, status
            );
        } else {
            match answer {
                DefaultAnswer::Answer301 { .. } => incr!(
                    "http.301.redirection",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer400 { .. } => incr!("http.400.errors"),
                DefaultAnswer::Answer401 { .. } => incr!(
                    "http.401.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer404 { .. } => incr!("http.404.errors"),
                DefaultAnswer::Answer408 { .. } => incr!(
                    "http.408.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer413 { .. } => incr!(
                    "http.413.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer502 { .. } => incr!(
                    "http.502.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer503 { .. } => incr!(
                    "http.503.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer504 { .. } => incr!(
                    "http.504.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
                DefaultAnswer::Answer507 { .. } => incr!(
                    "http.507.errors",
                    self.context.cluster_id.as_deref(),
                    self.context.backend_id.as_deref()
                ),
            };
        }

        let mut kawa = self.answers.borrow().get(
            answer,
            self.context.id.to_string(),
            self.context.cluster_id.as_deref(),
            self.context.backend_id.as_deref(),
            self.get_route(),
        );
        kawa.prepare(&mut kawa::h1::BlockConverter);
        self.context.status = Some(status);
        self.context.reason = None;
        self.context.keep_alive_frontend = false;
        self.response_stream = ResponseStream::DefaultAnswer(status, kawa);
        self.frontend_readiness.interest = Ready::WRITABLE | Ready::HUP | Ready::ERROR;
        self.backend_readiness.interest = Ready::HUP | Ready::ERROR;
    }

    pub fn test_backend_socket(&self) -> bool {
        match self.backend_socket {
            Some(ref s) => {
                let mut tmp = [0u8; 1];
                let res = s.peek(&mut tmp[..]);

                match res {
                    // if the socket is half open, it will report 0 bytes read (EOF)
                    Ok(0) => false,
                    Ok(_) => true,
                    Err(e) => matches!(e.kind(), std::io::ErrorKind::WouldBlock),
                }
            }
            None => false,
        }
    }

    pub fn is_valid_backend_socket(&self) -> bool {
        // if socket was last used in the last second, test it
        match self.backend_stop.as_ref() {
            Some(stop_instant) => {
                let now = Instant::now();
                let dur = now - *stop_instant;
                if dur > Duration::from_secs(1) {
                    return self.test_backend_socket();
                }
            }
            None => return self.test_backend_socket(),
        }

        true
    }

    pub fn set_backend_socket(&mut self, socket: TcpStream, backend: Option<Rc<RefCell<Backend>>>) {
        self.backend_socket = Some(socket);
        self.backend = backend;
    }

    pub fn set_cluster_id(&mut self, cluster_id: String) {
        self.context.cluster_id = Some(cluster_id);
    }

    pub fn set_backend_id(&mut self, backend_id: String) {
        self.context.backend_id = Some(backend_id);
    }

    pub fn set_backend_token(&mut self, token: Token) {
        self.backend_token = Some(token);
    }

    pub fn clear_backend_token(&mut self) {
        self.backend_token = None;
    }

    pub fn set_backend_timeout(&mut self, dur: Duration) {
        if let Some(token) = self.backend_token.as_ref() {
            self.container_backend_timeout.set_duration(dur);
            self.container_backend_timeout.set(*token);
        }
    }

    pub fn front_socket(&self) -> &TcpStream {
        self.frontend_socket.socket_ref()
    }

    /// WARNING: this function removes the backend entry in the session manager
    /// IF the backend_token is set, so that entry can be reused for new backend.
    /// I don't think this is a good idea, but it is a quick fix
    fn close_backend(&mut self, proxy: Rc<RefCell<dyn L7Proxy>>, metrics: &mut SessionMetrics) {
        self.container_backend_timeout.cancel();
        debug!(
            "{}\tPROXY [{}->{}] CLOSED BACKEND",
            log_context!(self),
            self.frontend_token.0,
            self.backend_token
                .map(|t| format!("{}", t.0))
                .unwrap_or_else(|| "-".to_string())
        );

        let proxy = proxy.borrow();
        if let Some(socket) = &mut self.backend_socket.take() {
            if let Err(e) = proxy.deregister_socket(socket) {
                error!(
                    "{} Error deregistering back socket({:?}): {:?}",
                    log_context!(self),
                    socket,
                    e
                );
            }
            if let Err(e) = socket.shutdown(Shutdown::Both) {
                if e.kind() != ErrorKind::NotConnected {
                    error!(
                        "{} Error shutting down back socket({:?}): {:?}",
                        log_context!(self),
                        socket,
                        e
                    );
                }
            }
        }

        if let Some(token) = self.backend_token.take() {
            proxy.remove_session(token);

            if self.backend_connection_status != BackendConnectionStatus::NotConnected {
                self.backend_readiness.event = Ready::EMPTY;
            }

            if self.backend_connection_status == BackendConnectionStatus::Connected {
                gauge_add!("backend.connections", -1);
                gauge_add!(
                    "connections_per_backend",
                    -1,
                    self.context.cluster_id.as_deref(),
                    metrics.backend_id.as_deref()
                );
            }

            self.set_backend_connected(BackendConnectionStatus::NotConnected, metrics);

            if let Some(backend) = self.backend.take() {
                backend.borrow_mut().dec_connections();
            }
        }
    }

    /// Check the number of connection attempts against authorized connection retries
    fn check_circuit_breaker(&mut self) -> Result<(), BackendConnectionError> {
        if self.connection_attempts >= CONN_RETRIES {
            error!(
                "{} Max connection attempt reached ({})",
                log_context!(self),
                self.connection_attempts,
            );

            self.set_answer(DefaultAnswer::Answer503 {
                message: format!(
                    "Max connection attempt reached: {}",
                    self.connection_attempts
                ),
            });
            return Err(BackendConnectionError::MaxConnectionRetries(None));
        }
        Ok(())
    }

    fn check_backend_connection(&mut self, metrics: &mut SessionMetrics) -> bool {
        let is_valid_backend_socket = self.is_valid_backend_socket();

        if !is_valid_backend_socket {
            return false;
        }

        //matched on keepalive
        metrics.backend_id = self.backend.as_ref().map(|i| i.borrow().backend_id.clone());

        metrics.backend_start();
        if let Some(b) = self.backend.as_mut() {
            b.borrow_mut().active_requests += 1;
        }
        true
    }

    // -> host, path, method
    pub fn extract_route(&self) -> Result<(&str, &str, &Method), RetrieveClusterError> {
        let given_method = self
            .context
            .method
            .as_ref()
            .ok_or(RetrieveClusterError::NoMethod)?;
        let given_authority = self
            .context
            .authority
            .as_deref()
            .ok_or(RetrieveClusterError::NoHost)?;
        let given_path = self
            .context
            .path
            .as_deref()
            .ok_or(RetrieveClusterError::NoPath)?;

        Ok((given_authority, given_path, given_method))
    }

    pub fn get_route(&self) -> String {
        if let Some(method) = &self.context.method {
            if let Some(authority) = &self.context.authority {
                if let Some(path) = &self.context.path {
                    return format!("{method} {authority}{path}");
                }
                return format!("{method} {authority}");
            }
            return format!("{method}");
        }
        String::new()
    }

    fn cluster_id_from_request(
        &mut self,
        proxy: Rc<RefCell<dyn L7Proxy>>,
    ) -> Result<String, RetrieveClusterError> {
        let (host, uri, method) = match self.extract_route() {
            Ok(tuple) => tuple,
            Err(cluster_error) => {
                self.set_answer(DefaultAnswer::Answer400 {
                    message: "Could not extract the route after connection started, this should not happen.".into(),
                    phase: self.request_stream.parsing_phase.marker(),
                    successfully_parsed: "null".into(),
                    partially_parsed: "null".into(),
                    invalid: "null".into(),
                });
                return Err(cluster_error);
            }
        };

        let route_result = self
            .listener
            .borrow()
            .frontend_from_request(host, uri, method);

        let route = match route_result {
            Ok(route) => route,
            Err(frontend_error) => {
                self.set_answer(DefaultAnswer::Answer404 {});
                return Err(RetrieveClusterError::RetrieveFrontend(frontend_error));
            }
        };

        let cluster_id = match route {
            Route::ClusterId(cluster_id) => cluster_id,
            Route::Deny => {
                self.set_answer(DefaultAnswer::Answer401 {});
                return Err(RetrieveClusterError::UnauthorizedRoute);
            }
        };

        let frontend_should_redirect_https = matches!(proxy.borrow().kind(), ListenerType::Http)
            && proxy
                .borrow()
                .clusters()
                .get(&cluster_id)
                .map(|cluster| cluster.https_redirect)
                .unwrap_or(false);

        if frontend_should_redirect_https {
            self.set_answer(DefaultAnswer::Answer301 {
                location: format!("https://{host}{uri}"),
            });
            return Err(RetrieveClusterError::UnauthorizedRoute);
        }

        Ok(cluster_id)
    }

    pub fn backend_from_request(
        &mut self,
        cluster_id: &str,
        frontend_should_stick: bool,
        proxy: Rc<RefCell<dyn L7Proxy>>,
        metrics: &mut SessionMetrics,
    ) -> Result<TcpStream, BackendConnectionError> {
        let (backend, conn) = self
            .get_backend_for_sticky_session(
                frontend_should_stick,
                self.context.sticky_session_found.as_deref(),
                cluster_id,
                proxy,
            )
            .map_err(|backend_error| {
                // some backend errors are actually retryable
                // TODO: maybe retry or return a different default answer
                self.set_answer(DefaultAnswer::Answer503 {
                    message: backend_error.to_string(),
                });
                BackendConnectionError::Backend(backend_error)
            })?;

        if frontend_should_stick {
            // update sticky name in case it changed I guess?
            self.context.sticky_name = self.listener.borrow().get_sticky_name().to_string();

            self.context.sticky_session = Some(
                backend
                    .borrow()
                    .sticky_id
                    .clone()
                    .unwrap_or_else(|| backend.borrow().backend_id.clone()),
            );
        }

        metrics.backend_id = Some(backend.borrow().backend_id.clone());
        metrics.backend_start();
        self.set_backend_id(backend.borrow().backend_id.clone());

        self.backend = Some(backend);
        Ok(conn)
    }

    fn get_backend_for_sticky_session(
        &self,
        frontend_should_stick: bool,
        sticky_session: Option<&str>,
        cluster_id: &str,
        proxy: Rc<RefCell<dyn L7Proxy>>,
    ) -> Result<(Rc<RefCell<Backend>>, TcpStream), BackendError> {
        match (frontend_should_stick, sticky_session) {
            (true, Some(sticky_session)) => proxy
                .borrow()
                .backends()
                .borrow_mut()
                .backend_from_sticky_session(cluster_id, sticky_session),
            _ => proxy
                .borrow()
                .backends()
                .borrow_mut()
                .backend_from_cluster_id(cluster_id),
        }
    }

    /// Helper to get mutable access to response_stream's storage, if it's BackendAnswer.
    /// Needed for `transfer_data` because `ResponseStream` is an enum.
    fn response_stream_storage_mut(&mut self) -> Result<&mut kawa::Buffer<Checkout>, ()> {
        match &mut self.response_stream {
            ResponseStream::BackendAnswer(rs) => Ok(&mut rs.storage),
            ResponseStream::DefaultAnswer(_, _) => {
                error!("{} Attempted to get response_stream_storage_mut during DefaultAnswer state (likely in tunnel mode error).", log_context!(self));
                Err(())
            }
        }
    }
    
    /// Generic helper for transferring data in tunnel mode.
    /// Returns `Ok(true)` if EOF or error occurred on source/destination, indicating session should close.
    /// Returns `Ok(false)` if data was transferred or a socket would block (session continues).
    /// Returns `Err(_)` for critical internal errors (e.g., trying to use DefaultAnswer stream).
    fn transfer_data<S1, S2>(&mut self, 
        source_socket_handler: &mut S1, // Renamed to avoid conflict with Http.source_socket
        dest_socket_handler: &mut S2,   // Renamed to avoid conflict
        buffer_checkout: &mut kawa::Buffer<Checkout>, 
        metrics: &mut SessionMetrics, 
        is_front_to_back: bool
    ) -> Result<bool, ()> 
    where
        S1: SocketHandler + std::fmt::Debug,
        S2: SocketHandler + std::fmt::Debug,
    {
        let mut eof_or_error_occurred = false;

        // Step 1: Try to write any pending data from the buffer to the destination socket.
        if !buffer_checkout.is_empty() {
            let (write_size, write_state) = dest_socket_handler.socket_write_vectored(&[std::io::IoSlice::new(buffer_checkout.used())]);
            if write_size > 0 {
                buffer_checkout.consume(write_size);
                if is_front_to_back { 
                    metrics.backend_bout += write_size; count!("connect.front_to_back_bytes_written", write_size as i64); 
                    trace!("{} CONNECT tunnel: Wrote {} bytes from client to target.", log_context!(self), write_size);
                } else { 
                    metrics.bout += write_size; count!("connect.back_to_front_bytes_written", write_size as i64); 
                    trace!("{} CONNECT tunnel: Wrote {} bytes from target to client.", log_context!(self), write_size);
                }
            }
            match write_state {
                SocketResult::WouldBlock => { /* Destination would block, can't write more now. */ }
                SocketResult::Error | SocketResult::Closed => {
                    let direction = if is_front_to_back { "dest (target)" } else { "dest (client)" };
                    info!("{} CONNECT tunnel: {} socket error/closed on write. Marking for session closure.", log_context!(self), direction);
                    eof_or_error_occurred = true; // Mark to close session after attempting read from source.
                }
                SocketResult::Continue => {}
            }
        }

        // Step 2: If the buffer has space AND no error occurred on write, try to read from the source socket.
        if !eof_or_error_occurred && !buffer_checkout.is_full() {
             let (read_size, read_state) = source_socket_handler.socket_read(buffer_checkout.space());
             if read_size > 0 {
                buffer_checkout.fill(read_size);
                if is_front_to_back { 
                    metrics.bin += read_size; count!("connect.front_to_back_bytes_read", read_size as i64); 
                    trace!("{} CONNECT tunnel: Read {} bytes from client.", log_context!(self), read_size);
                } else { 
                    metrics.backend_bin += read_size; count!("connect.back_to_front_bytes_read", read_size as i64); 
                    trace!("{} CONNECT tunnel: Read {} bytes from target.", log_context!(self), read_size);
                }
             }
            match read_state {
                SocketResult::WouldBlock => { /* Source would block, can't read more now. */ }
                SocketResult::Error | SocketResult::Closed => { // EOF or error on source
                    let direction = if is_front_to_back { "source (client)" } else { "source (target)" };
                    info!("{} CONNECT tunnel: {} socket EOF/error/closed on read. Marking for session closure.", log_context!(self), direction);
                    eof_or_error_occurred = true; 
                }
                SocketResult::Continue => {}
            }
        }
        
        Ok(eof_or_error_occurred)
    }

    fn handle_connect_tunnel(&mut self, metrics: &mut SessionMetrics) -> SessionResult {
        let mut close_session = false;
        let mut activity_occured = true; // Assume activity to enter loop

        // Loop to shuttle data as long as there's activity or events suggesting potential activity
        for _ in 0..MAX_LOOP_ITERATIONS { // Protect against potential spin loops
            if !activity_occured { // No data moved in the last full pass, and no new events processed by now.
                break;
            }
            activity_occured = false; // Reset for this pass

            // 1. Data from client (frontend) to target (backend)
            // Try to transfer if frontend was readable OR if there's pending data in request_stream buffer.
            if self.frontend_readiness.event.is_readable() || !self.request_stream.storage.is_empty() {
                if self.backend_socket.is_some() { // Ensure backend socket exists
                    match self.transfer_data(&mut self.frontend_socket, self.backend_socket.as_mut().unwrap(), &mut self.request_stream.storage, metrics, true) {
                        Ok(should_close) => {
                            if should_close { close_session = true; break; }
                            activity_occured = true; // transfer_data would have returned false if it just blocked
                        } 
                        Err(_) => { close_session = true; break; } // Internal error from transfer_data itself
                    }
                } else { 
                    info!("{} CONNECT tunnel: Backend socket missing for front->back transfer. Closing.", log_context!(self));
                    close_session = true; break; 
                }
            }
            if close_session { break; }


            // 2. Data from target (backend) to client (frontend)
            // Try to transfer if backend was readable OR if there's pending data in response_stream buffer.
            let response_storage_checkout = match self.response_stream_storage_mut() {
                Ok(s) => s,
                Err(_) => { close_session = true; break; } // Should not happen if tunnel is properly established
            };
            if self.backend_readiness.event.is_readable() || !response_storage_checkout.is_empty() {
                 if self.backend_socket.is_some() { // Ensure backend socket exists (already checked for write, but good for read source)
                    match self.transfer_data(self.backend_socket.as_mut().unwrap(), &mut self.frontend_socket, response_storage_checkout, metrics, false) {
                        Ok(should_close) => {
                            if should_close { close_session = true; break; }
                            activity_occured = true;
                        }
                        Err(_) => { close_session = true; break; }
                    }
                } else { 
                     info!("{} CONNECT tunnel: Backend socket missing for back->front transfer. Closing.", log_context!(self));
                    close_session = true; break; 
                }
            }
            if close_session { break; }
            
            // If after trying both directions, no new events were processed by this iteration of the loop for readable,
            // and no data could be moved (e.g. both directions would block on write or read with empty buffers), then break.
            if !self.frontend_readiness.event.is_readable() && !self.backend_readiness.event.is_readable() && !activity_occured {
                break;
            }
        }

        // Final check for HUP/Error events after data shuttling attempts
        if self.frontend_readiness.event.is_hup() || self.frontend_readiness.event.is_error() {
            info!("{} CONNECT tunnel: Frontend HUP/Error detected post-transfer. Closing session.", log_context!(self));
            close_session = true;
        }
        if self.backend_readiness.event.is_hup() || self.backend_readiness.event.is_error() {
            info!("{} CONNECT tunnel: Backend HUP/Error detected post-transfer. Closing session.", log_context!(self));
            close_session = true;
        }
        
        // Clear Mio events as they've been processed for this tunnel iteration
        self.frontend_readiness.event = Ready::EMPTY;
        self.backend_readiness.event = Ready::EMPTY;

        if close_session {
            info!("{} CONNECT tunnel: Closing session.", log_context!(self));
            SessionResult::Close
        } else {
            SessionResult::Continue
        }
    }

    fn connect_to_backend(
        &mut self,
        session_rc: Rc<RefCell<dyn ProxySession>>,
        proxy: Rc<RefCell<dyn L7Proxy>>,
        metrics: &mut SessionMetrics,
    ) -> Result<BackendConnectAction, BackendConnectionError> {
        // --- BEGIN FORWARD PROXY MODIFICATION for connect_to_backend ---
        if self.is_forward_proxy_request && !self.is_connect_tunnel {
            debug!("{} Attempting direct connection for HTTP forward proxy.", log_context!(self));

            let target_host = self.forward_target_host.as_ref().ok_or_else(|| {
                self.set_answer(DefaultAnswer::Answer503 { message: "Forward proxy host not set.".into() });
                BackendConnectionError::Backend(BackendError::NoHealthyBackends("Forward proxy host not set.".into()))
            })?;
            let target_port = self.forward_target_port.ok_or_else(|| {
                self.set_answer(DefaultAnswer::Answer503 { message: "Forward proxy port not set.".into() });
                BackendConnectionError::Backend(BackendError::NoHealthyBackends("Forward proxy port not set.".into()))
            })?;

            let target_addr_str = format!("{}:{}", target_host, target_port);
            
            // Close any existing backend connection if we're switching to a direct forward.
            // This mirrors logic for when cluster_id changes.
            if self.backend_token.take().is_some() {
                 self.close_backend(proxy.clone(), metrics);
            }
            // Reset parts of context that might be tied to a previous backend/cluster.
            self.context.cluster_id = Some(format!("forward_http->{}", target_addr_str)); // Synthetic cluster_id for logging
            self.context.backend_id = Some(target_addr_str.clone()); // Synthetic backend_id
            self.backend = None; // No traditional Backend object for direct forwards.

            match TcpStream::connect(target_addr_str.parse().map_err(|_| {
                self.set_answer(DefaultAnswer::Answer503 { message: "Forward proxy address invalid.".into() });
                BackendConnectionError::Backend(BackendError::NoHealthyBackends("Forward proxy address invalid.".into()))
            })?) {
                Ok(mut stream) => {
                    info!("{} Successfully connected to forward target: {}", log_context!(self), target_addr_str);
                    if let Err(e) = stream.set_nodelay(true) {
                        error!("{} Error setting nodelay on forward target socket({:?}): {:?}", log_context!(self), stream, e);
                    }

                    self.backend_readiness.interest = Ready::WRITABLE | Ready::HUP | Ready::ERROR;
                    self.backend_connection_status = BackendConnectionStatus::Connecting(Instant::now());
                    self.connection_attempts = 0; // Reset connection attempts for this new target.

                    // Handle backend token (reuse or new)
                    let old_backend_token = self.backend_token.take(); // Take to ensure it's processed once
                    match old_backend_token {
                        Some(backend_token) => {
                            self.set_backend_token(backend_token);
                            if let Err(e) = proxy.borrow().register_socket(&mut stream, backend_token, Interest::READABLE | Interest::WRITABLE) {
                                error!("{} Error re-registering back socket for forward proxy({:?}): {:?}", log_context!(self), stream, e);
                                // This is problematic, might need to close.
                                return Err(BackendConnectionError::Backend(BackendError::InternalError(e.to_string())));
                            }
                            self.set_backend_socket(stream, None); // No traditional Backend object
                            self.set_backend_timeout(self.configured_connect_timeout);
                            return Ok(BackendConnectAction::Replace);
                        }
                        None => {
                            let backend_token = proxy.borrow().add_session(session_rc);
                            if let Err(e) = proxy.borrow().register_socket(&mut stream, backend_token, Interest::READABLE | Interest::WRITABLE) {
                                error!("{} Error registering new back socket for forward proxy({:?}): {:?}", log_context!(self), stream, e);
                                proxy.borrow().remove_session(backend_token); // Clean up session entry
                                return Err(BackendConnectionError::Backend(BackendError::InternalError(e.to_string())));
                            }
                            self.set_backend_socket(stream, None); // No traditional Backend object
                            self.set_backend_token(backend_token);
                            self.set_backend_timeout(self.configured_connect_timeout);
                            return Ok(BackendConnectAction::New);
                        }
                    }
                }
                Err(e) => {
                    error!("{} Failed to connect to forward target {}: {}", log_context!(self), target_addr_str, e);
                    self.fail_backend_connection(metrics); // Uses self.backend, which is None. This might need adjustment or be a no-op.
                    self.set_answer(DefaultAnswer::Answer503 { message: format!("Failed to connect to upstream server: {}", target_host) });
                    return Err(BackendConnectionError::Backend(BackendError::ConnectionRefusedOrTimedOut));
                }
            }
        }
        // --- END FORWARD PROXY MODIFICATION for connect_to_backend ---

        let old_cluster_id = self.context.cluster_id.clone();
        let old_backend_token = self.backend_token;

        self.check_circuit_breaker()?;

        // This part is skipped for forward proxy requests handled above.
        let cluster_id = self
            .cluster_id_from_request(proxy.clone())
            .map_err(BackendConnectionError::RetrieveClusterError)?;

        trace!(
            "{} Connect_to_backend (reverse proxy): {:?} {:?} {:?}",
            log_context!(self),
            self.context.cluster_id,
            cluster_id,
            self.backend_connection_status
        );
        // check if we can reuse the backend connection
        if (self.context.cluster_id.as_ref()) == Some(&cluster_id)
            && self.backend_connection_status == BackendConnectionStatus::Connected
        {
            let has_backend = self
                .backend
                .as_ref()
                .map(|backend| {
                    let backend = backend.borrow();
                    proxy
                        .borrow()
                        .backends()
                        .borrow()
                        .has_backend(&cluster_id, &backend)
                })
                .unwrap_or(false);

            if has_backend && self.check_backend_connection(metrics) {
                return Ok(BackendConnectAction::Reuse);
            } else if self.backend_token.take().is_some() {
                self.close_backend(proxy.clone(), metrics);
            }
        }

        //replacing with a connection to another cluster
        if old_cluster_id.is_some()
            && old_cluster_id.as_ref() != Some(&cluster_id)
            && self.backend_token.take().is_some()
        {
            self.close_backend(proxy.clone(), metrics);
        }

        self.context.cluster_id = Some(cluster_id.clone());

        let frontend_should_stick = proxy
            .borrow()
            .clusters()
            .get(&cluster_id)
            .map(|cluster| cluster.sticky_session)
            .unwrap_or(false);

        let mut socket =
            self.backend_from_request(&cluster_id, frontend_should_stick, proxy.clone(), metrics)?;
        if let Err(e) = socket.set_nodelay(true) {
            error!(
                "{} Error setting nodelay on backend socket({:?}): {:?}",
                log_context!(self),
                socket,
                e
            );
        }

        self.backend_readiness.interest = Ready::WRITABLE | Ready::HUP | Ready::ERROR;
        self.backend_connection_status = BackendConnectionStatus::Connecting(Instant::now());

        match old_backend_token {
            Some(backend_token) => {
                self.set_backend_token(backend_token);
                if let Err(e) = proxy.borrow().register_socket(
                    &mut socket,
                    backend_token,
                    Interest::READABLE | Interest::WRITABLE,
                ) {
                    error!(
                        "{} Error registering back socket({:?}): {:?}",
                        log_context!(self),
                        socket,
                        e
                    );
                }

                self.set_backend_socket(socket, self.backend.clone());
                self.set_backend_timeout(self.configured_connect_timeout);

                Ok(BackendConnectAction::Replace)
            }
            None => {
                let backend_token = proxy.borrow().add_session(session_rc);

                if let Err(e) = proxy.borrow().register_socket(
                    &mut socket,
                    backend_token,
                    Interest::READABLE | Interest::WRITABLE,
                ) {
                    error!(
                        "{} Error registering back socket({:?}): {:?}",
                        log_context!(self),
                        socket,
                        e
                    );
                }

                self.set_backend_socket(socket, self.backend.clone());
                self.set_backend_token(backend_token);
                self.set_backend_timeout(self.configured_connect_timeout);

                Ok(BackendConnectAction::New)
            }
        }
    }

    fn set_backend_connected(
        &mut self,
        connected: BackendConnectionStatus,
        metrics: &mut SessionMetrics,
    ) {
        let last = self.backend_connection_status;
        self.backend_connection_status = connected;

        if connected == BackendConnectionStatus::Connected {
            gauge_add!("backend.connections", 1);
            gauge_add!(
                "connections_per_backend",
                1,
                self.context.cluster_id.as_deref(),
                metrics.backend_id.as_deref()
            );

            // the back timeout was of connect_timeout duration before,
            // now that we're connected, move to backend_timeout duration
            self.set_backend_timeout(self.configured_backend_timeout);
            // if we are not waiting for the backend response, its timeout is concelled
            // it should be set when the request has been entirely transmitted
            if !self.backend_readiness.interest.is_readable() {
                self.container_backend_timeout.cancel();
            }

            if let Some(backend) = &self.backend {
                let mut backend = backend.borrow_mut();

                if backend.retry_policy.is_down() {
                    incr!(
                        "backend.up",
                        self.context.cluster_id.as_deref(),
                        metrics.backend_id.as_deref()
                    );

                    info!(
                        "backend server {} at {} is up",
                        backend.backend_id, backend.address
                    );

                    push_event(Event {
                        kind: EventKind::BackendUp as i32,
                        backend_id: Some(backend.backend_id.to_owned()),
                        address: Some(backend.address.into()),
                        cluster_id: None,
                    });
                }

                if let BackendConnectionStatus::Connecting(start) = last {
                    backend.set_connection_time(Instant::now() - start);
                }

                //successful connection, reset failure counter
                backend.failures = 0;
                backend.active_requests += 1;
                backend.retry_policy.succeed();
            }
        }
    }

    fn fail_backend_connection(&mut self, metrics: &SessionMetrics) {
        if let Some(backend) = &self.backend {
            let mut backend = backend.borrow_mut();
            backend.failures += 1;

            let already_unavailable = backend.retry_policy.is_down();
            backend.retry_policy.fail();
            incr!(
                "backend.connections.error",
                self.context.cluster_id.as_deref(),
                metrics.backend_id.as_deref()
            );

            if !already_unavailable && backend.retry_policy.is_down() {
                error!(
                    "{} backend server {} at {} is down",
                    log_context!(self),
                    backend.backend_id,
                    backend.address
                );

                incr!(
                    "backend.down",
                    self.context.cluster_id.as_deref(),
                    metrics.backend_id.as_deref()
                );

                push_event(Event {
                    kind: EventKind::BackendDown as i32,
                    backend_id: Some(backend.backend_id.to_owned()),
                    address: Some(backend.address.into()),
                    cluster_id: None,
                });
            }
        }
    }

    pub fn backend_hup(&mut self, metrics: &mut SessionMetrics) -> StateResult {
        let response_stream = match &mut self.response_stream {
            ResponseStream::BackendAnswer(response_stream) => response_stream,
            _ => return StateResult::CloseBackend,
        };

        // there might still data we can read on the socket
        if self.backend_readiness.event.is_readable()
            && self.backend_readiness.interest.is_readable()
        {
            return StateResult::Continue;
        }

        // the backend finished to answer we can close
        if response_stream.is_terminated() {
            return StateResult::CloseBackend;
        }
        match (
            self.request_stream.is_initial(),
            response_stream.is_initial(),
        ) {
            // backend stopped before response is finished,
            // or maybe it was malformed in the first place (no Content-Length)
            (_, false) => {
                error!(
                    "{} Backend closed before session is over",
                    log_context!(self),
                );

                trace!("{} Backend hang-up, setting the parsing phase of the response stream to terminated, this also takes care of responses that lack length information.", log_context!(self));

                response_stream.parsing_phase = kawa::ParsingPhase::Terminated;

                // writable() will be called again and finish the session properly
                // for this reason, writable must not short cut
                self.frontend_readiness.interest.insert(Ready::WRITABLE);
                StateResult::Continue
            }
            // probably backend hup between keep alive request, change backend
            (true, true) => {
                trace!(
                    "{} Backend hanged up in between requests",
                    log_context!(self)
                );
                StateResult::CloseBackend
            }
            // the frontend already transmitted data so we can't redirect
            (false, true) => {
                error!(
                    "{}  Frontend transmitted data but the back closed",
                    log_context!(self)
                );

                self.set_answer(DefaultAnswer::Answer503 {
                    message: "Backend closed after consuming part of the request".into(),
                });

                self.backend_readiness.interest = Ready::EMPTY;
                StateResult::Continue
            }
        }
    }

    /// The main session loop, processing all events triggered by mio since last time
    /// and proxying http traffic. The main flow can be summed up by:
    ///
    /// - if connecting an back has event:
    ///   - if backend hanged up, try again
    ///   - else, set as connected
    /// - while front or back has event:
    ///   - read request on front
    ///   - write request to back
    ///   - read response on back
    ///   - write response to front
    fn ready_inner(
        &mut self,
        session: Rc<RefCell<dyn crate::ProxySession>>,
        proxy: Rc<RefCell<dyn L7Proxy>>,
        metrics: &mut SessionMetrics,
    ) -> SessionResult {
        // --- BEGIN FORWARD PROXY (CONNECT Tunnel main dispatch) ---
        if self.is_connect_tunnel {
            // We are in CONNECT tunnel mode. Bypass normal HTTP processing.
            return self.handle_connect_tunnel(metrics);
        }
        // --- END FORWARD PROXY (CONNECT Tunnel main dispatch) ---

        // --- BEGIN FORWARD PROXY (CONNECT Tunnel Handling Setup) ---
        // Check if we've just successfully connected to the target for a CONNECT request.
        if self.is_forward_proxy_request && // It's a forward proxy operation (CONNECT or absolute URI)
           self.context.method == Some(Method::Connect) && // Specifically a CONNECT method
           !self.is_connect_tunnel && // Not yet in tunnel mode
           self.connect_response_buffer.is_none() && // We haven't prepared the 200 OK yet
           self.backend_connection_status.is_connecting() && // We were attempting to connect
           !self.backend_readiness.event.is_empty() && // There's an event for the backend socket
           !self.backend_readiness.event.is_hup() && // And it's not a hangup
           !self.backend_readiness.event.is_error() // And it's not an error
           // This implies the backend socket is now writable or readable, meaning TCP connect succeeded.
        {
            info!("{} TCP connection to target for CONNECT successful. Preparing '200 Connection established'.", log_context!(self));
            self.connect_response_buffer = Some(b"HTTP/1.1 200 Connection established\r\n\r\n".to_vec());
            self.frontend_readiness.interest.insert(Ready::WRITABLE); // Ensure writable() is called to send this.
            
            // Mark backend as "connected" for internal state management (e.g. timeouts)
            // This doesn't mean it's an HTTP backend in the traditional sense for CONNECT.
            metrics.backend_connected(); 
            self.connection_attempts = 0; // Reset for this connection
            self.set_backend_connected(BackendConnectionStatus::Connected, metrics); // Update status
            
            // For CONNECT, after 200 OK, we don't do further HTTP processing on this connection.
            // So, remove interest in reading more from client (until tunnel starts) or writing to backend (HTTP-wise).
            self.frontend_readiness.interest.remove(Ready::READABLE);
            self.backend_readiness.interest.remove(Ready::WRITABLE | Ready::READABLE); 
            
            // Clear current events as we've handled the successful connection event for CONNECT.
            self.frontend_readiness.event = Ready::EMPTY;
            self.backend_readiness.event = Ready::EMPTY;
            
            return SessionResult::Continue; // Let Http::writable send the 200 OK.
        }
        // --- END FORWARD PROXY (CONNECT Tunnel Handling Setup) ---

        let mut counter = 0;

        if self.backend_connection_status.is_connecting()
            && !self.backend_readiness.event.is_empty()
            // Add check to ensure this block is not entered if we just handled CONNECT success above
            && !(self.context.method == Some(Method::Connect) && self.is_forward_proxy_request && self.connect_response_buffer.is_some())
        {
            if self.backend_readiness.event.is_hup() && !self.test_backend_socket() {
                //retry connecting the backend
                error!(
                    "{} Error connecting to backend, trying again, attempt {}",
                    log_context!(self),
                    self.connection_attempts
                );

                self.connection_attempts += 1;
                self.fail_backend_connection(metrics);

                self.backend_connection_status =
                    BackendConnectionStatus::Connecting(Instant::now());

                // trigger a backend reconnection
                self.close_backend(proxy.clone(), metrics);

                let connection_result =
                    self.connect_to_backend(session.clone(), proxy.clone(), metrics);
                if let Err(err) = &connection_result {
                    error!(
                        "{} Error connecting to backend: {}",
                        log_context!(self),
                        err
                    );
                }

                if let Some(session_result) = handle_connection_result(connection_result) {
                    return session_result;
                }
            } else {
                metrics.backend_connected();
                self.connection_attempts = 0;
                self.set_backend_connected(BackendConnectionStatus::Connected, metrics);
                // we might get an early response from the backend, so we want to look
                // at readable events
                self.backend_readiness.interest.insert(Ready::READABLE);
            }
        }

        if self.frontend_readiness.event.is_hup() {
            if !self.request_stream.is_initial() {
                self.log_request_error(metrics, "Client disconnected abruptly");
            }
            return SessionResult::Close;
        }

        while counter < MAX_LOOP_ITERATIONS {
            let frontend_interest = self.frontend_readiness.filter_interest();
            let backend_interest = self.backend_readiness.filter_interest();

            trace!(
                "{} Frontend interest({:?}) and backend interest({:?})",
                log_context!(self),
                frontend_interest,
                backend_interest,
            );

            if frontend_interest.is_empty() && backend_interest.is_empty() {
                break;
            }

            if self.backend_readiness.event.is_hup()
                && self.frontend_readiness.interest.is_writable()
                && !self.frontend_readiness.event.is_writable()
            {
                break;
            }

            if frontend_interest.is_readable() {
                let state_result = self.readable(metrics);
                trace!(
                    "{} frontend_readable: {:?}",
                    log_context!(self),
                    state_result
                );

                match state_result {
                    StateResult::Continue => {}
                    StateResult::ConnectBackend => {
                        let connection_result =
                            self.connect_to_backend(session.clone(), proxy.clone(), metrics);
                        if let Err(err) = &connection_result {
                            error!(
                                "{} Error connecting to backend: {}",
                                log_context!(self),
                                err
                            );
                        }

                        if let Some(session_result) = handle_connection_result(connection_result) {
                            return session_result;
                        }
                    }
                    StateResult::CloseBackend => unreachable!(),
                    StateResult::CloseSession => return SessionResult::Close,
                    StateResult::Upgrade => return SessionResult::Upgrade,
                }
            }

            if backend_interest.is_writable() {
                let session_result = self.backend_writable(metrics);
                trace!(
                    "{} backend_writable: {:?}",
                    log_context!(self),
                    session_result
                );
                if session_result != SessionResult::Continue {
                    return session_result;
                }
            }

            if backend_interest.is_readable() {
                let session_result = self.backend_readable(metrics);
                trace!(
                    "{} backend_readable: {:?}",
                    log_context!(self),
                    session_result
                );
                if session_result != SessionResult::Continue {
                    return session_result;
                }
            }

            if frontend_interest.is_writable() {
                let state_result = self.writable(metrics);
                trace!(
                    "{} frontend_writable: {:?}",
                    log_context!(self),
                    state_result
                );
                match state_result {
                    StateResult::CloseBackend => self.close_backend(proxy.clone(), metrics),
                    StateResult::CloseSession => return SessionResult::Close,
                    StateResult::Upgrade => return SessionResult::Upgrade,
                    StateResult::Continue => {}
                    StateResult::ConnectBackend => unreachable!(),
                }
            }

            if frontend_interest.is_error() {
                error!(
                    "{} frontend socket error, disconnecting",
                    log_context!(self)
                );

                return SessionResult::Close;
            }

            if backend_interest.is_hup() || backend_interest.is_error() {
                let state_result = self.backend_hup(metrics);

                trace!("{} backend_hup: {:?}", log_context!(self), state_result);
                match state_result {
                    StateResult::Continue => {}
                    StateResult::CloseBackend => self.close_backend(proxy.clone(), metrics),
                    StateResult::CloseSession => return SessionResult::Close,
                    StateResult::ConnectBackend | StateResult::Upgrade => unreachable!(),
                }
            }

            counter += 1;
        }

        if counter >= MAX_LOOP_ITERATIONS {
            error!(
                "{}\tHandling session went through {} iterations, there's a probable infinite loop bug, closing the connection",
                log_context!(self), MAX_LOOP_ITERATIONS
            );

            incr!("http.infinite_loop.error");
            self.print_state(self.protocol_string());

            return SessionResult::Close;
        }

        SessionResult::Continue
    }

    pub fn timeout_status(&self) -> TimeoutStatus {
        if self.request_stream.is_main_phase() {
            match &self.response_stream {
                ResponseStream::BackendAnswer(kawa) if kawa.is_initial() => {
                    TimeoutStatus::WaitingForResponse
                }
                _ => TimeoutStatus::Response,
            }
        } else if self.keepalive_count > 0 {
            TimeoutStatus::WaitingForNewRequest
        } else {
            TimeoutStatus::Request
        }
    }
}

impl<Front: SocketHandler, L: ListenerHandler + L7ListenerHandler> SessionState for Http<Front, L> {
    fn ready(
        &mut self,
        session: Rc<RefCell<dyn crate::ProxySession>>,
        proxy: Rc<RefCell<dyn L7Proxy>>,
        metrics: &mut SessionMetrics,
    ) -> SessionResult {
        let session_result = self.ready_inner(session, proxy, metrics);
        if session_result == SessionResult::Upgrade {
            let response_storage = match &mut self.response_stream {
                ResponseStream::BackendAnswer(response_stream) => &mut response_stream.storage,
                _ => return SessionResult::Close,
            };

            // sync the underlying Checkout buffers, if they contain remaining data
            // it will be processed once upgraded to websocket
            self.request_stream.storage.buffer.sync(
                self.request_stream.storage.end,
                self.request_stream.storage.head,
            );
            response_storage
                .buffer
                .sync(response_storage.end, response_storage.head);
        }
        session_result
    }

    fn update_readiness(&mut self, token: Token, events: Ready) {
        if self.frontend_token == token {
            self.frontend_readiness.event |= events;
        } else if self.backend_token == Some(token) {
            self.backend_readiness.event |= events;
        }
    }

    fn close(&mut self, proxy: Rc<RefCell<dyn L7Proxy>>, metrics: &mut SessionMetrics) {
        self.close_backend(proxy, metrics);
        self.frontend_socket.socket_close();
        let _ = self.frontend_socket.socket_write_vectored(&[]);

        //if the state was initial, the connection was already reset
        if !self.request_stream.is_initial() {
            gauge_add!("http.active_requests", -1);

            if let Some(b) = self.backend.as_mut() {
                let mut backend = b.borrow_mut();
                backend.active_requests = backend.active_requests.saturating_sub(1);
            }
        }
    }

    fn timeout(&mut self, token: Token, metrics: &mut SessionMetrics) -> StateResult {
        //info!("got timeout for token: {:?}", token);
        if self.frontend_token == token {
            self.container_frontend_timeout.triggered();
            return match self.timeout_status() {
                // we do not have a complete answer
                TimeoutStatus::Request => {
                    self.set_answer(DefaultAnswer::Answer408 {
                        duration: self.container_frontend_timeout.to_string(),
                    });
                    self.writable(metrics)
                }
                // we have a complete answer but the response did not start
                TimeoutStatus::WaitingForResponse => {
                    // this case is ambiguous, as it is the frontend timeout that triggers while we were waiting for response
                    // the timeout responsibility should have switched before
                    self.set_answer(DefaultAnswer::Answer504 {
                        duration: self.container_backend_timeout.to_string(),
                    });
                    self.writable(metrics)
                }
                // we have a complete answer and the start of a response, but the request was not tagged as terminated
                // for now we place responsibility of timeout on the backend in those cases, so we ignore this
                TimeoutStatus::Response => StateResult::Continue,
                // timeout in keep-alive, simply close the connection
                TimeoutStatus::WaitingForNewRequest => StateResult::CloseSession,
            };
        }

        if self.backend_token == Some(token) {
            //info!("backend timeout triggered for token {:?}", token);
            self.container_backend_timeout.triggered();
            return match self.timeout_status() {
                TimeoutStatus::Request => {
                    error!(
                        "got backend timeout while waiting for a request, this should not happen"
                    );
                    self.set_answer(DefaultAnswer::Answer504 {
                        duration: self.container_backend_timeout.to_string(),
                    });
                    self.writable(metrics)
                }
                TimeoutStatus::WaitingForResponse => {
                    self.set_answer(DefaultAnswer::Answer504 {
                        duration: self.container_backend_timeout.to_string(),
                    });
                    self.writable(metrics)
                }
                TimeoutStatus::Response => {
                    error!(
                        "backend {:?} timeout while receiving response (cluster {:?})",
                        self.context.backend_id, self.context.cluster_id
                    );
                    StateResult::CloseSession
                }
                // in keep-alive, we place responsibility of timeout on the frontend, so we ignore this
                TimeoutStatus::WaitingForNewRequest => StateResult::Continue,
            };
        }

        error!("{} Got timeout for an invalid token", log_context!(self));
        StateResult::CloseSession
    }

    fn cancel_timeouts(&mut self) {
        self.container_backend_timeout.cancel();
        self.container_frontend_timeout.cancel();
    }

    fn print_state(&self, context: &str) {
        error!(
            "\
{} {} Session(Kawa)
\tFrontend:
\t\ttoken: {:?}\treadiness: {:?}\tstate: {:?}
\tBackend:
\t\ttoken: {:?}\treadiness: {:?}",
            log_context!(self),
            context,
            self.frontend_token,
            self.frontend_readiness,
            self.request_stream.parsing_phase,
            self.backend_token,
            self.backend_readiness,
            // self.response_stream.parsing_phase
        );
    }

    fn shutting_down(&mut self) -> SessionIsToBeClosed {
        if self.request_stream.is_initial() && self.request_stream.storage.is_empty()
        // && self.response_stream.storage.is_empty()
        {
            true
        } else {
            self.context.closing = true;
            false
        }
    }
}

fn handle_connection_result(
    connection_result: Result<BackendConnectAction, BackendConnectionError>,
) -> Option<SessionResult> {
    match connection_result {
        // reuse connection or send a default answer, we can continue
        Ok(BackendConnectAction::Reuse) => None,
        Ok(BackendConnectAction::New) | Ok(BackendConnectAction::Replace) => {
            // we must wait for an event
            Some(SessionResult::Continue)
        }
        Err(_) => {
            // All BackendConnectionError already set a default answer
            // the session must continue to serve it
            // - NotFound: not used for http (only tcp)
            // - RetrieveClusterError: 301/400/401/404,
            // - MaxConnectionRetries: 503,
            // - Backend: 503,
            // - MaxSessionsMemory: not checked in connect_to_backend (TODO: check it?)
            None
        }
    }
}

/// Save the HTTP status code of the backend response
fn save_http_status_metric(status: Option<u16>, context: LogContext) {
    if let Some(status) = status {
        match status {
            100..=199 => {
                incr!("http.status.1xx", context.cluster_id, context.backend_id);
            }
            200..=299 => {
                incr!("http.status.2xx", context.cluster_id, context.backend_id);
            }
            300..=399 => {
                incr!("http.status.3xx", context.cluster_id, context.backend_id);
            }
            400..=499 => {
                incr!("http.status.4xx", context.cluster_id, context.backend_id);
            }
            500..=599 => {
                incr!("http.status.5xx", context.cluster_id, context.backend_id);
            }
            _ => {
                // http responses with other codes (protocol error)
                incr!("http.status.other");
            }
        }
    }
}
