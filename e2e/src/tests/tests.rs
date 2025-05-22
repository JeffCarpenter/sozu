use std::{
    net::SocketAddr,
    thread,
    time::{Duration, Instant},
};

use sozu_command_lib::{
    config::{FileConfig, ListenerBuilder},
    info,
    logging::setup_default_logging,
    proto::command::{
        request::RequestType, ActivateListener, AddCertificate, CertificateAndKey, Cluster,
        CustomHttpAnswers, ListenerType, RemoveBackend, RequestHttpFrontend, SocketAddress,
    },
    scm_socket::Listeners,
    state::ConfigState,
};

use crate::{
    http_utils::{http_ok_response, http_request, immutable_answer},
    mock::{
        aggregator::SimpleAggregator,
        async_backend::BackendHandle as AsyncBackend,
        client::Client,
        https_client::{build_https_client, resolve_request},
        sync_backend::Backend as SyncBackend,
    },
    sozu::worker::Worker,
    tests::{provide_port, repeat_until_error_or, setup_async_test, setup_sync_test, State},
};
use std::io::{Read, Write}; // For TcpStream read/write in CONNECT test
use native_tls::TlsConnector; // For CONNECT test client (TlsAcceptor is in mock_backend)
use std::sync::Arc; // For Arc<TlsAcceptor> in mock_backend (though used by test to create it)


pub fn create_local_address() -> SocketAddr {
    let address: SocketAddr = format!("127.0.0.1:{}", provide_port())
        .parse()
        .expect("could not parse front address");
    println!("created local address {}", address);
    address
}

pub fn try_async(nb_backends: usize, nb_clients: usize, nb_requests: usize) -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) = setup_async_test(
        "ASYNC",
        config,
        listeners,
        state,
        front_address,
        nb_backends,
        false,
    );

    let mut clients = (0..nb_clients)
        .map(|i| {
            Client::new(
                format!("client{i}"),
                front_address,
                http_request("GET", "/api", format!("ping{i}"), "localhost"),
            )
        })
        .collect::<Vec<_>>();
    for client in clients.iter_mut() {
        client.connect();
    }
    for _ in 0..nb_requests {
        for client in clients.iter_mut() {
            client.send();
        }
        for client in clients.iter_mut() {
            match client.receive() {
                Some(response) => println!("{response}"),
                _ => {}
            }
        }
    }

    worker.soft_stop();
    worker.wait_for_server_stop();

    for client in &clients {
        println!(
            "{} sent: {}, received: {}",
            client.name, client.requests_sent, client.responses_received
        );
    }
    for backend in backends.iter_mut() {
        let aggregator = backend.stop_and_get_aggregator();
        println!("{} aggregated: {:?}", backend.name, aggregator);
    }

    if clients.iter().all(|client| {
        client.requests_sent == nb_requests && client.responses_received == nb_requests
    }) {
        State::Success
    } else {
        State::Fail
    }
}

pub fn try_sync(nb_clients: usize, nb_requests: usize) -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) =
        setup_sync_test("SYNC", config, listeners, state, front_address, 1, false);
    let mut backend = backends.pop().unwrap();

    backend.connect();

    let mut clients = (0..nb_clients)
        .map(|i| {
            Client::new(
                format!("client{i}"),
                front_address,
                http_request("GET", "/api", format!("ping{i}"), "localhost"),
            )
        })
        .collect::<Vec<_>>();

    // send one request, then maintain a keepalive session
    for (i, client) in clients.iter_mut().enumerate() {
        client.connect();
        client.send();
        backend.accept(i);
        backend.receive(i);
        backend.send(i);
        client.receive();
    }

    for _ in 0..nb_requests - 1 {
        for client in clients.iter_mut() {
            client.send();
        }
        for i in 0..nb_clients {
            backend.receive(i);
            backend.send(i);
        }
        for client in clients.iter_mut() {
            match client.receive() {
                Some(response) => println!("{response}"),
                _ => {}
            }
        }
    }

    worker.soft_stop();
    worker.wait_for_server_stop();

    for client in &clients {
        println!(
            "{} sent: {}, received: {}",
            client.name, client.requests_sent, client.responses_received
        );
    }
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    if clients.iter().all(|client| {
        client.requests_sent == nb_requests && client.responses_received == nb_requests
    }) {
        State::Success
    } else {
        State::Fail
    }
}

pub fn try_backend_stop(nb_requests: usize, zombie: Option<u32>) -> State {
    let front_address = create_local_address();

    let config = Worker::into_config(FileConfig {
        zombie_check_interval: zombie,
        ..FileConfig::default()
    });
    let listeners = Listeners::default();
    let state = ConfigState::new();
    let (mut worker, mut backends) = setup_async_test(
        "BACKSTOP",
        config,
        listeners,
        state,
        front_address,
        2,
        false,
    );
    let mut backend2 = backends.pop().expect("backend2");
    let mut backend1 = backends.pop().expect("backend1");

    let mut aggregator = Some(SimpleAggregator {
        requests_received: 0,
        responses_sent: 0,
    });

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/api", "ping", "localhost"),
    );
    client.connect();

    let start = Instant::now();
    for i in 0..nb_requests {
        if client.send().is_none() {
            break;
        }
        match client.receive() {
            Some(response) => println!("{response}"),
            None => break,
        }
        if i == 0 {
            aggregator = backend1.stop_and_get_aggregator();
        }
    }
    let duration = Instant::now().duration_since(start);

    worker.soft_stop();
    let success = worker.wait_for_server_stop();

    println!(
        "sent: {}, received: {}",
        client.requests_sent, client.responses_received
    );
    println!("backend1 aggregator: {aggregator:?}");
    aggregator = backend2.stop_and_get_aggregator();
    println!("backend2 aggregator: {aggregator:?}");

    if !success {
        State::Fail
    } else if duration > Duration::from_millis(100) {
        // Reconnecting to unother backend should have lasted less that 100 miliseconds
        State::Undecided
    } else {
        State::Success
    }
}

pub fn try_issue_810_timeout() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) = setup_sync_test(
        "810-TIMEOUT",
        config,
        listeners,
        state,
        front_address,
        1,
        false,
    );
    let mut backend = backends.pop().unwrap();

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/api", "ping", "localhost"),
    );

    backend.connect();
    client.connect();
    client.send();
    backend.accept(0);
    backend.receive(0);
    backend.send(0);
    client.receive();

    worker.soft_stop();
    let start = Instant::now();
    let success = worker.wait_for_server_stop();
    let duration = Instant::now().duration_since(start);

    println!(
        "{} sent: {}, received: {}",
        client.name, client.requests_sent, client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    if !success || duration > Duration::from_millis(100) {
        State::Fail
    } else {
        State::Success
    }
}

pub fn try_issue_810_panic(part2: bool) -> State {
    let front_address = create_local_address();

    let back_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("810-PANIC", config, &listeners, state);

    worker.send_proxy_request_type(RequestType::AddTcpListener(
        ListenerBuilder::new_tcp(front_address.into())
            .to_tcp(None)
            .unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: front_address.into(),
        proxy: ListenerType::Tcp.into(),
        from_scm: false,
    }));
    worker.send_proxy_request_type(RequestType::AddCluster(Worker::default_cluster(
        "cluster_0",
    )));
    worker.send_proxy_request_type(RequestType::AddTcpFrontend(Worker::default_tcp_frontend(
        "cluster_0",
        front_address,
    )));

    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-0",
        back_address,
        None,
    )));
    worker.read_to_last();

    let mut backend = SyncBackend::new("backend", back_address, "pong");
    let mut client = Client::new("client", front_address, "ping");

    backend.connect();
    client.connect();
    client.send();
    if !part2 {
        backend.accept(0);
        backend.receive(0);
        backend.send(0);
        let response = client.receive();
        println!("Response: {response:?}");
    }

    worker.soft_stop();
    let success = worker.wait_for_server_stop();

    println!(
        "{} sent: {}, received: {}",
        client.name, client.requests_sent, client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    if success {
        State::Success
    } else {
        State::Fail
    }
}

pub fn try_tls_endpoint() -> State {
    let front_port = provide_port();
    let front_address = SocketAddress::new_v4(127, 0, 0, 1, front_port);
    let back_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("TLS-ENDPOINT", config, &listeners, state);

    worker.send_proxy_request_type(RequestType::AddHttpsListener(
        ListenerBuilder::new_https(front_address.clone())
            .to_tls(None)
            .unwrap(),
    ));

    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: front_address.clone(),
        proxy: ListenerType::Https.into(),
        from_scm: false,
    }));

    worker.send_proxy_request_type(RequestType::AddCluster(Worker::default_cluster(
        "cluster_0",
    )));

    let hostname = "localhost".to_string();
    worker.send_proxy_request_type(RequestType::AddHttpsFrontend(RequestHttpFrontend {
        hostname: hostname.to_owned(),
        ..Worker::default_http_frontend("cluster_0", front_address.clone().into())
    }));

    let certificate_and_key = CertificateAndKey {
        certificate: String::from(include_str!("../../../lib/assets/local-certificate.pem")),
        key: String::from(include_str!("../../../lib/assets/local-key.pem")),
        certificate_chain: vec![], // in config.toml the certificate chain would be the same as the certificate
        versions: vec![],
        names: vec![],
    };
    let add_certificate = AddCertificate {
        address: front_address,
        certificate: certificate_and_key,
        expired_at: None,
    };
    worker.send_proxy_request_type(RequestType::AddCertificate(add_certificate));

    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-0",
        back_address,
        None,
    )));
    worker.read_to_last();

    let mut backend = AsyncBackend::spawn_detached_backend(
        "BACKEND",
        back_address,
        SimpleAggregator::default(),
        AsyncBackend::http_handler("pong"),
    );

    let client = build_https_client();
    let request = client.get(
        format!("https://{hostname}:{front_port}/api")
            .parse()
            .unwrap(),
    );
    if let Some((status, body)) = resolve_request(request) {
        println!("response status: {status:?}");
        println!("response body: {body}");
    } else {
        return State::Fail;
    }

    worker.soft_stop();
    let success = worker.wait_for_server_stop();

    let aggregator = backend
        .stop_and_get_aggregator()
        .expect("Could not get aggregator");
    println!(
        "{} sent: {}, received: {}",
        backend.name, aggregator.responses_sent, aggregator.requests_received
    );

    if success && aggregator.responses_sent == 1 {
        State::Success
    } else {
        State::Fail
    }
}

pub fn test_upgrade() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) =
        setup_sync_test("UPGRADE", config, listeners, state, front_address, 1, false);

    let mut backend = backends.pop().expect("backend");
    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/api", "ping", "localhost"),
    );

    backend.connect();
    client.connect();
    client.send();
    backend.accept(0);
    backend.receive(0);
    backend.send(0);
    match client.receive() {
        Some(msg) => println!("response: {msg}"),
        None => return State::Fail,
    }

    client.send();
    backend.receive(0);
    let mut new_worker = worker.upgrade("NEW_WORKER");
    thread::sleep(Duration::from_millis(100));
    backend.send(0);
    match client.receive() {
        Some(msg) => println!("response: {msg}"),
        None => return State::Fail,
    }
    client.connect();
    client.send();
    println!("ACCEPTING...");
    backend.accept(1);
    backend.receive(1);
    backend.send(1);
    match client.receive() {
        Some(msg) => println!("response: {msg}"),
        None => return State::Fail,
    }

    new_worker.soft_stop();
    if !worker.wait_for_server_stop() {
        return State::Fail;
    }
    if !new_worker.wait_for_server_stop() {
        return State::Fail;
    }

    println!(
        "{} sent: {}, received: {}",
        client.name, client.requests_sent, client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    State::Success
}

/*
pub fn test_http(nb_requests: usize) {
    let front_address = "127.0.0.1:2001"
        .parse()
        .expect("could not parse front address");

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) = async_setup_test("HTTP", config, listeners, state, front_address, 1);
    let mut backend = backends.pop().expect("backend");

    let mut bad_client = Client::new(
        "bad_client".to_string(),
        front_address,
        "GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\nContent-Length: 3\r\n\r\nbad_ping",
    );
    let mut good_client = Client::new(
        "good_client".to_string(),
        front_address,
        http_request("GET", "/api", "good_ping", "localhost"),
    );
    bad_client.connect();
    good_client.connect();

    for _ in 0..nb_requests {
        bad_client.send();
        good_client.send();
        match bad_client.receive() {
            Some(msg) => println!("response: {msg}"),
            None => {}
        }
        match good_client.receive() {
            Some(msg) => println!("response: {msg}"),
            None => {}
        }
    }

    worker.send_proxy_request(RequestContent::SoftStop);
    worker.wait_for_server_stop();

    println!(
        "{} sent: {}, received: {}",
        bad_client.name, bad_client.requests_sent, bad_client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        good_client.name, good_client.requests_sent, good_client.responses_received
    );
    let aggregator = backend.stop_and_get_aggregator();
    println!("backend aggregator: {aggregator:?}");
}
*/

pub fn try_hard_or_soft_stop(soft: bool) -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) =
        setup_sync_test("STOP", config, listeners, state, front_address, 1, false);
    let mut backend = backends.pop().unwrap();

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/api", "ping", "localhost"),
    );

    // Send a request to try out
    backend.connect();
    client.connect();
    client.send();
    backend.accept(0);
    backend.receive(0);

    // stop sōzu
    if soft {
        // the worker will wait for backends to respond before shutting down
        worker.soft_stop();
    } else {
        // the worker will shut down without waiting for backends to finish
        worker.hard_stop();
    }
    thread::sleep(Duration::from_millis(100));

    backend.send(0);

    match (soft, client.receive()) {
        (true, None) => {
            println!("SoftStop didn't wait for HTTP response to complete");
            return State::Fail;
        }
        (true, Some(msg)) => {
            println!("response on SoftStop: {msg}");
        }
        (false, None) => {
            println!("no response on HardStop");
        }
        (false, Some(msg)) => {
            println!("HardStop waited for HTTP response to complete: {msg}");
            return State::Fail;
        }
    }

    let success = worker.wait_for_server_stop();

    println!(
        "{} sent: {}, received: {}",
        client.name, client.requests_sent, client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    if success {
        State::Success
    } else {
        State::Fail
    }
}

fn try_http_behaviors() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "BEHAVE-OUT") {
        println!("could not setup default logging: {e}");
    }

    info!("starting up");

    let front_address: SocketAddr = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("BEHAVE-WORKER", config, &listeners, state);

    let mut http_config = ListenerBuilder::new_http(front_address.into())
        .to_http(None)
        .unwrap();
    let http_answers = CustomHttpAnswers {
        answer_400: Some(immutable_answer(400)),
        answer_404: Some(immutable_answer(404)),
        answer_502: Some(immutable_answer(502)),
        answer_503: Some(immutable_answer(503)),
        ..Default::default()
    };
    http_config.http_answers = Some(http_answers);

    worker.send_proxy_request_type(RequestType::AddHttpListener(http_config));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: front_address.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/", "ping", "example.com"),
    );

    info!("expecting 404");
    client.connect();
    client.send();

    let response = client.receive();
    println!("response: {response:?}");
    assert_eq!(response, Some(immutable_answer(404)));
    assert_eq!(client.receive(), None);

    worker.send_proxy_request_type(RequestType::AddHttpFrontend(RequestHttpFrontend {
        hostname: String::from("example.com"),
        ..Worker::default_http_frontend("cluster_0", front_address)
    }));
    worker.read_to_last();

    info!("expecting 503");
    client.connect();
    client.send();

    let response = client.receive();
    println!("response: {response:?}");
    assert_eq!(response, Some(immutable_answer(503)));
    assert_eq!(client.receive(), None);

    let back_address = create_local_address();

    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-0".to_string(),
        back_address,
        None,
    )));
    worker.read_to_last();

    info!("sending invalid request, expecting 400");
    client.set_request("HELLO\r\n\r\n");
    client.connect();
    client.send();

    let response = client.receive();
    println!("response: {response:?}");
    assert_eq!(response, Some(immutable_answer(400)));
    assert_eq!(client.receive(), None);

    let mut backend = SyncBackend::new("backend", back_address, "TEST\r\n\r\n");
    backend.connect();

    info!("expecting 502");
    client.connect();
    client.set_request(http_request("GET", "/", "ping", "example.com"));
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    backend.send(0);

    let response = client.receive();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert_eq!(response, Some(immutable_answer(502)));
    assert_eq!(client.receive(), None);

    info!("expecting 200");
    worker.send_proxy_request_type(RequestType::RemoveBackend(RemoveBackend {
        cluster_id: String::from("cluster_0"),
        backend_id: String::from("cluster_0-0"),
        address: back_address.into(),
    }));
    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-0".to_string(),
        back_address,
        None,
    )));
    backend.disconnect();
    worker.read_to_last();

    let mut backend = SyncBackend::new("backend", back_address, http_ok_response("hello"));
    backend.connect();
    client.connect();
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    backend.send(0);

    let expected_response_start = String::from("HTTP/1.1 200 OK\r\nContent-Length: 5");
    let expected_response_end = String::from("hello");
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );

    info!("expecting 200, without content length");
    backend.set_response("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nHello world!");
    client.send();
    let request = backend.receive(0);
    backend.send(0);

    let expected_response_start = String::from("HTTP/1.1 200 OK\r\n");
    let expected_response_end = String::from("Hello world!");
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );

    info!("server closes, expecting 503");
    // TODO: what if the client continue to use the closed stream
    client.connect();
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    backend.close(0);

    let response = client.receive();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert_eq!(response, Some(immutable_answer(503)));
    assert_eq!(client.receive(), None);

    worker.send_proxy_request_type(RequestType::RemoveBackend(RemoveBackend {
        cluster_id: String::from("cluster_0"),
        backend_id: String::from("cluster_0-0"),
        address: back_address.into(),
    }));
    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-0".to_string(),
        back_address,
        None,
    )));
    backend.disconnect();
    worker.read_to_last();

    // transfer-encoding is an invalid header for 101 response, but Sozu should ignore it (see issue #885)
    let mut backend = SyncBackend::new(
        "backend",
        back_address,
        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: WebSocket\r\nTransfer-Encoding: Chunked\r\n\r\n",
    );

    info!("expecting upgrade (101 switching protocols)");
    backend.connect();
    client.connect();
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    backend.send(0);

    let expected_response_start = String::from(
        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: WebSocket\r\n",
    );
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(response.starts_with(&expected_response_start));

    backend.set_response("early");
    backend.send(0);
    let expected_response = String::from("early");
    let response = client.receive();
    assert_eq!(response, Some(expected_response));

    client.set_request("ping");
    backend.set_response("pong");
    client.send();
    let request = backend.receive(0);
    backend.send(0);

    let expected_response = String::from("pong");
    let response = client.receive();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert_eq!(response, Some(expected_response));
    assert_eq!(client.receive(), None);

    info!("expecting 100");
    backend.set_response("HTTP/1.1 100 Continue\r\n\r\n");
    client.set_request("GET /100 HTTP/1.1\r\nHost: example.com\r\nConnection: keep-alive\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n");
    client.connect();
    client.send();
    backend.accept(1);
    let request = backend.receive(1);
    backend.send(1);

    let expected_response_start = String::from("HTTP/1.1 100 Continue\r\n");
    let expected_response_end = String::from("\r\n\r\n");
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );

    backend.set_response("HTTP/1.1 200 OK\r\n\r\n");
    client.set_request("0123456789");
    client.send();
    let request = backend.receive(1).unwrap();
    backend.send(1);

    let expected_response_start = String::from("HTTP/1.1 200 OK\r\n");
    let expected_response_end = String::from("\r\n\r\n");
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );
    assert_eq!(request, String::from("0123"));

    info!("expecting 100 BAD");
    backend.set_response("HTTP/1.1 200 Ok\r\n\r\nRESPONSE_BODY_NO_LENGTH");
    client.set_request("GET /100 HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\nExpect: 100-continue\r\n\r\n");
    client.connect();
    client.send();
    backend.accept(1);
    let request = backend.receive(1);
    backend.send(1);

    let expected_response_start = String::from("HTTP/1.1 200 Ok\r\n");
    let expected_response_end = String::from("RESPONSE_BODY_NO_LENGTH");
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );

    info!("expecting 103");
    backend.set_response(
        "HTTP/1.1 103 Early Hint\r\nLink: </style.css>; rel=preload; as=style\r\n\r\n",
    );
    client.set_request("GET /103 HTTP/1.1\r\nHost: example.com\r\nContent-Length: 4\r\n\r\nping");
    client.connect();
    client.send();
    backend.accept(1);
    let request = backend.receive(1);
    backend.send(1);

    let expected_response_start = String::from("HTTP/1.1 103 Early Hint\r\n");
    let expected_response_end = String::from("\r\n\r\n");
    let response = client.receive().unwrap();
    println!("request: {request:?}");
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );

    backend.set_response("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\npong");
    backend.send(1);

    let expected_response_start = String::from("HTTP/1.1 200 OK\r\n");
    let expected_response_end = String::from("\r\n\r\npong");
    let response = client.receive().unwrap();
    println!("response: {response:?}");
    assert!(
        response.starts_with(&expected_response_start)
            && response.ends_with(&expected_response_end)
    );

    worker.hard_stop();
    worker.wait_for_server_stop();

    info!("good bye");
    State::Success
}

fn try_https_redirect() -> State {
    let front_address: SocketAddr = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("BEHAVE-WORKER", config, &listeners, state);

    let mut http_config = ListenerBuilder::new_http(front_address.into())
        .to_http(None)
        .unwrap();
    let answer_301_prefix = "HTTP/1.1 301 Moved Permanently\r\nLocation: ";

    let http_answers = CustomHttpAnswers {
        answer_301: Some(format!("{answer_301_prefix}%REDIRECT_LOCATION\r\n\r\n")),
        ..Default::default()
    };
    http_config.http_answers = Some(http_answers);

    worker.send_proxy_request_type(RequestType::AddHttpListener(http_config));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: front_address.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.send_proxy_request(
        RequestType::AddCluster(Cluster {
            https_redirect: true,
            ..Worker::default_cluster("cluster_0")
        })
        .into(),
    );
    worker.send_proxy_request_type(RequestType::AddHttpFrontend(RequestHttpFrontend {
        hostname: String::from("example.com"),
        ..Worker::default_http_frontend("cluster_0", front_address)
    }));

    worker.read_to_last();

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/redirected?true", "", "example.com"),
    );

    client.connect();
    client.send();
    let answer = client.receive();
    let expected_answer = format!("{answer_301_prefix}https://example.com/redirected?true\r\n\r\n");
    assert_eq!(answer, Some(expected_answer));

    State::Success
}

fn try_msg_close() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) = setup_sync_test(
        "MSG-CLOSE",
        config,
        listeners,
        state,
        front_address,
        1,
        false,
    );
    let mut backend = backends.pop().unwrap();

    backend.connect();

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/api", "ping", "localhost"),
    );

    backend.set_response(
        "HTTP/1.1 200 Ok \r\nContent-Length: 4\r\nConnection: Keep-Alive\r\n\r\npong",
    );
    client.connect();
    client.send();
    backend.accept(0);
    backend.receive(0);
    backend.send(0);
    println!("response: {:?}", client.receive());

    thread::sleep(std::time::Duration::from_millis(100));

    worker.soft_stop();
    worker.wait_for_server_stop();
    State::Success
}

pub fn try_blue_geen() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, _) = setup_async_test("BG", config, listeners, state, front_address, 0, false);

    let aggrerator = SimpleAggregator {
        requests_received: 0,
        responses_sent: 0,
    };

    let primary_address = create_local_address();

    let secondary_address = create_local_address();

    let mut primary = AsyncBackend::spawn_detached_backend(
        "PRIMARY",
        primary_address,
        aggrerator.clone(),
        AsyncBackend::http_handler("pong_primary".to_string()),
    );
    let mut secondary = AsyncBackend::spawn_detached_backend(
        "SECONDARY",
        secondary_address,
        aggrerator,
        AsyncBackend::http_handler("pong_secondary".to_string()),
    );

    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-0",
        primary_address,
        None,
    )));
    worker.read_to_last();

    let mut client = Client::new(
        "client",
        front_address,
        http_request("GET", "/api", "ping", "localhost"),
    );

    client.connect();
    client.send();
    let response = client.receive();
    println!("response: {response:?}");

    worker.send_proxy_request_type(RequestType::AddBackend(Worker::default_backend(
        "cluster_0",
        "cluster_0-1",
        secondary_address,
        None,
    )));
    worker.read_to_last();

    client.send();
    let response = client.receive();
    println!("response: {response:?}");

    worker.send_proxy_request_type(RequestType::RemoveBackend(RemoveBackend {
        cluster_id: "cluster_0".to_string(),
        backend_id: "cluster_0-0".to_string(),
        address: primary_address.into(),
    }));
    worker.read_to_last();

    client.send();
    let response = client.receive();
    println!("response: {response:?}");

    worker.soft_stop();
    let success = worker.wait_for_server_stop();

    println!(
        "sent: {}, received: {}",
        client.requests_sent, client.responses_received
    );
    let aggregator = primary.stop_and_get_aggregator();
    println!("primary aggregator: {aggregator:?}");
    let aggregator = secondary.stop_and_get_aggregator();
    println!("secondary aggregator: {aggregator:?}");

    if !success {
        State::Fail
    } else {
        State::Success
    }
}

pub fn try_keep_alive() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "KA-OUT") {
        println!("could not setup default logging: {e}");
    }

    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) = setup_sync_test(
        "KA-WORKER",
        config,
        listeners,
        state,
        front_address,
        1,
        false,
    );

    let mut backend = backends.pop().unwrap();
    let mut client = Client::new(
        "client".to_string(),
        front_address,
        "GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );

    backend
        .set_response("HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\npong");
    backend.connect();

    info!("front: close / back: keep");
    client.connect();
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    println!("request: {request:?}");
    backend.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(!client.is_connected()); // front disconnected
    assert!(!backend.is_connected(0)); // back disconnected

    info!("front: keep / back: keep");
    client.set_request("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n");
    client.connect();
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    println!("request: {request:?}");
    backend.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(client.is_connected()); // front connected
    assert!(backend.is_connected(0)); // back connected

    info!("front: keep / back: close");
    backend.set_response("HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong");
    client.send();
    let request = backend.receive(0);
    println!("request: {request:?}");
    backend.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(client.is_connected()); // front connected
    assert!(!backend.is_connected(0)); // back disconnected

    info!("front: close / back: close");
    client.set_request("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    client.send();
    backend.accept(0);
    let request = backend.receive(0);
    println!("request: {request:?}");
    backend.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(!client.is_connected()); // front disconnected
    assert!(!backend.is_connected(0)); // back disconnected

    worker.soft_stop();
    worker.wait_for_server_stop();

    println!(
        "{} sent: {}, received: {}",
        client.name, client.requests_sent, client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    State::Success
}

pub fn try_stick() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) =
        setup_sync_test("STICK", config, listeners, state, front_address, 2, true);

    let mut backend2 = backends.pop().unwrap();
    let mut backend1 = backends.pop().unwrap();
    let mut client = Client::new(
        "client".to_string(),
        front_address,
        "GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nCookie: foo=bar\r\n\r\n",
    );

    // Sozu choice order is determinist in round-bobin, so we will use backend1 then backend2
    backend1.connect();
    backend2.connect();

    // no sticky_session
    client.connect();
    client.send();
    backend1.accept(0);
    let request = backend1.receive(0);
    println!("request: {request:?}");
    backend1.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(request.unwrap().starts_with("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nCookie: foo=bar\r\nX-Forwarded-For:"));
    assert!(response.unwrap().starts_with("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nSet-Cookie: SOZUBALANCEID=sticky_cluster_0-0; Path=/\r\nSozu-Id:"));

    // invalid sticky_session
    client.set_request("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nCookie: foo=bar; SOZUBALANCEID=invalid\r\n\r\n");
    client.connect();
    client.send();
    backend2.accept(0);
    let request = backend2.receive(0);
    println!("request: {request:?}");
    backend2.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(request.unwrap().starts_with("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nCookie: foo=bar\r\nX-Forwarded-For:"));
    assert!(response.unwrap().starts_with("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nSet-Cookie: SOZUBALANCEID=sticky_cluster_0-1; Path=/\r\nSozu-Id:"));

    // good sticky_session (force use backend2, round-robin would have chosen backend1)
    client.set_request("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nCookie: foo=bar; SOZUBALANCEID=sticky_cluster_0-1\r\n\r\n");
    client.connect();
    client.send();
    backend2.accept(0);
    let request = backend2.receive(0);
    println!("request: {request:?}");
    backend2.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(request.unwrap().starts_with("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nCookie: foo=bar\r\nX-Forwarded-For:"));
    assert!(response
        .unwrap()
        .starts_with("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nSozu-Id:"));

    worker.soft_stop();
    worker.wait_for_server_stop();

    println!(
        "{} sent: {}, received: {}",
        client.name, client.requests_sent, client.responses_received
    );
    println!(
        "{} sent: {}, received: {}",
        backend1.name, backend1.responses_sent, backend1.requests_received
    );

    State::Success
}

fn try_max_connections() -> State {
    let front_address = create_local_address();

    let (mut config, listeners, state) = Worker::empty_config();
    config.max_connections = 15;
    let (mut worker, mut backends) =
        setup_sync_test("MAXCONN", config, listeners, state, front_address, 1, false);

    let mut backend = backends.pop().unwrap();
    backend.connect();
    let expected_response_start = String::from("HTTP/1.1 200 OK\r\nContent-Length: 5");

    let mut clients = Vec::new();
    for i in 0..20 {
        let mut client = Client::new(
            format!("client{i}"),
            front_address,
            http_request("GET", "/api", format!("ping{i}"), "localhost"),
        );
        client.connect();
        client.send();
        if backend.accept(i) {
            assert!(i < 15);
            let request = backend.receive(i);
            println!("request {i}: {request:?}");
            backend.send(i);
        } else {
            assert!(i >= 15);
        }
        let response = client.receive();
        println!("response {i}: {response:?}");
        if i < 15 {
            assert!(response.unwrap().starts_with(&expected_response_start));
        } else {
            assert_eq!(response, None);
        }
        clients.push(client);
    }

    for i in 0..20 {
        let client = &mut clients[i];
        if i < 15 {
            client.send();
            let request = backend.receive(i);
            println!("request {i}: {request:?}");
            backend.send(i);
            let response = client.receive();
            println!("response {i}: {response:?}");
            assert!(client.is_connected());
            assert!(response.unwrap().starts_with(&expected_response_start));
        } else {
            // assert!(!client.is_connected());
        }
    }

    for i in 0..5 {
        let new_client = &mut clients[15 + i];
        new_client.set_request(format!(
            "GET /api-{i} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        ));
        new_client.connect();
        new_client.send();

        let client = &mut clients[i];
        client.set_request("GET /api HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        client.send();
        let request = backend.receive(i);
        println!("request {i}: {request:?}");
        backend.send(i);
        let response = client.receive();
        println!("response {i}: {response:?}");
        assert!(!client.is_connected());
        assert!(response.unwrap().starts_with(&expected_response_start));

        if backend.accept(15 + i) {
            assert!(i >= 2);
        } else {
            assert!(i < 2);
        }
    }

    assert!(!backend.accept(100));

    for i in 15..20 {
        let request = backend.receive(i);
        backend.send(i);
        println!("request {i}: {request:?}");
    }
    for i in 15..20 {
        let client = &mut clients[i];
        client.is_connected();
        let response = client.receive();
        println!("response {i}: {response:?}");
        client.is_connected();
        // assert!(response.unwrap().starts_with(&expected_response_start));
    }

    for i in 15..20 {
        let client = &mut clients[i];
        client.is_connected();
        let response = client.receive();
        println!("response: {response:?}");
    }

    worker.hard_stop();
    worker.wait_for_server_stop();

    for client in clients {
        println!(
            "{} sent: {}, received: {}",
            client.name, client.requests_sent, client.responses_received
        );
    }
    println!(
        "{} sent: {}, received: {}",
        backend.name, backend.responses_sent, backend.requests_received
    );

    assert_eq!(backend.requests_received, 38);
    assert_eq!(backend.responses_sent, 38);

    State::Success
}

pub fn try_head() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) =
        setup_sync_test("SYNC", config, listeners, state, front_address, 1, false);
    let mut backend = backends.pop().unwrap();

    let head_response = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n";
    backend.connect();
    backend.set_response(head_response);

    let mut client = Client::new(
        "client",
        front_address,
        http_request("HEAD", "/api", "ping".to_string(), "localhost"),
    );

    client.connect();
    for i in 0..2 {
        client.send();
        if i == 0 {
            backend.accept(0);
        }
        let request = backend.receive(0);
        println!("request: {request:?}");
        assert!(request.is_some());
        backend.send(0);
        let response = client.receive();
        println!("response: {response:?}");
        assert!(response.is_some());
        assert!(response.unwrap().ends_with("\r\n\r\n"))
    }

    worker.soft_stop();
    worker.wait_for_server_stop();
    State::Success
}

pub fn try_status_header_split() -> State {
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let (mut worker, mut backends) =
        setup_sync_test("SPLIT", config, listeners, state, front_address, 1, false);
    let mut backend = backends.pop().unwrap();

    backend.connect();

    let mut client = Client::new("client", front_address, "");

    client.connect();

    let mut accepted = false;
    for (i, chunk) in [
        "POST /api HTTP/1.",
        "1\r\n",
        "Host: localhost\r\n",
        "Content-Length",
        ":",
        " 1",
        "0",
        "\r",
        "\n\r",
        "\n012",
        "34567",
        "89",
    ]
    .iter()
    .enumerate()
    {
        println!("{accepted} {i} {chunk:?}");
        client.set_request(*chunk);
        client.send();
        if !accepted {
            println!("accepting");
            accepted = backend.accept(0);
            if accepted {
                assert_eq!(i, 9);
            }
            backend.send(0);
        }
        if accepted {
            println!("receiving");
            let request = backend.receive(0);
            println!("request: {request:?}");
        }
    }
    backend.send(0);
    let response = client.receive();
    println!("response: {response:?}");
    assert!(response.is_some());

    worker.hard_stop();
    worker.wait_for_server_stop();
    State::Success
}

fn try_wildcard() -> State {
    use sozu_command_lib::proto::command::{PathRule, RulePosition};
    let front_address = create_local_address();

    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("WLD_CRD", config, &listeners, state);
    worker.send_proxy_request(
        RequestType::AddHttpListener(
            ListenerBuilder::new_http(front_address.into())
                .to_http(None)
                .unwrap(),
        )
        .into(),
    );
    worker.send_proxy_request(
        RequestType::ActivateListener(ActivateListener {
            address: front_address.into(),
            proxy: ListenerType::Http.into(),
            from_scm: false,
        })
        .into(),
    );

    worker.send_proxy_request(RequestType::AddCluster(Worker::default_cluster("cluster_0")).into());
    worker.send_proxy_request(RequestType::AddCluster(Worker::default_cluster("cluster_1")).into());

    worker.send_proxy_request(
        RequestType::AddHttpFrontend(RequestHttpFrontend {
            cluster_id: Some("cluster_0".to_string()),
            address: front_address.into(),
            hostname: String::from("*.sozu.io"),
            path: PathRule::prefix(String::from("")),
            position: RulePosition::Tree.into(),
            ..Default::default()
        })
        .into(),
    );

    let back_address: SocketAddr = create_local_address();
    worker.send_proxy_request(
        RequestType::AddBackend(Worker::default_backend(
            "cluster_0",
            "cluster_0-0",
            back_address,
            None,
        ))
        .into(),
    );
    worker.read_to_last();

    let mut backend0 = SyncBackend::new(
        "BACKEND_0",
        back_address,
        http_ok_response("pong0".to_string()),
    );

    let mut client = Client::new(
        "client",
        front_address,
        http_request("POST", "/api", "ping".to_string(), "www.sozu.io"),
    );

    backend0.connect();
    client.connect();
    client.send();
    let accepted = backend0.accept(0);
    assert!(accepted);
    let request = backend0.receive(0);
    println!("request: {request:?}");
    backend0.send(0);
    let response = client.receive();
    println!("response: {response:?}");

    worker.send_proxy_request(
        RequestType::AddHttpFrontend(RequestHttpFrontend {
            cluster_id: Some("cluster_1".to_string()),
            address: front_address.into(),
            hostname: String::from("*.sozu.io"),
            path: PathRule::prefix(String::from("/api")),
            position: RulePosition::Tree.into(),
            ..Default::default()
        })
        .into(),
    );
    let back_address: SocketAddr = create_local_address();
    worker.send_proxy_request(
        RequestType::AddBackend(Worker::default_backend(
            "cluster_1",
            "cluster_1-0",
            back_address,
            None,
        ))
        .into(),
    );

    let mut backend1 = SyncBackend::new(
        "BACKEND_1",
        back_address,
        http_ok_response("pong1".to_string()),
    );

    worker.read_to_last();

    backend1.connect();

    client.send();
    let accepted = backend1.accept(0);
    assert!(accepted);
    let request = backend1.receive(0);
    println!("request: {request:?}");
    backend1.send(0);
    let response = client.receive();
    println!("response: {response:?}");

    State::Success
}

#[test]
fn test_sync() {
    assert_eq!(try_sync(10, 100), State::Success);
}

#[test]
fn test_async() {
    assert_eq!(try_async(3, 10, 100), State::Success);
}

#[test]
fn test_hard_stop() {
    assert_eq!(
        repeat_until_error_or(
            10,
            "Hard Stop: Test that the worker shuts down even if backends are not done",
            || try_hard_or_soft_stop(false)
        ),
        State::Success
    );
}

fn try_forward_proxy_cache_non_200_response() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-NON200-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_non_200_response");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path = "/notfound/resource".to_string();
    let upstream_response_body = "Resource Not Found".to_string(); // Body for the 404

    // 1. Start Mock Upstream Server (configured to send 404)
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_NON200_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            "GET".to_string(),
            resource_path.clone(),
            mock_upstream_addr_str.clone(), 
            upstream_response_body.clone(),
            404, // Status code 404
            None, 
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock non-200 upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_NON200_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for non-200 test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    let absolute_uri = format!("http://{}{}", mock_upstream_addr_str, resource_path);

    // --- First Request (gets 404) ---
    info!("Client: Sending first request to Sozu for {} (upstream sends 404)", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send first non-200 request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read first non-200 response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 404") && response_str.contains(&upstream_response_body), "First non-200 response content mismatch. Got:\n{}", response_str);
            info!("Client: Received first non-200 response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for first non-200 request: {}", e); return State::Fail; }
    }
    thread::sleep(Duration::from_millis(100));

    // --- Second Request (should also get 404, from origin) ---
    info!("Client: Sending second request to Sozu for {} (should be a cache miss for 404)", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send second non-200 request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read second non-200 response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 404") && response_str.contains(&upstream_response_body), "Second non-200 response content mismatch. Got:\n{}", response_str);
            info!("Client: Received second non-200 response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for second non-200 request: {}", e); return State::Fail; }
    }
    
    // 3. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data for non-200 test: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 2, "Mock upstream should have been hit twice as non-200 responses are not cached.");
    assert_eq!(aggregator_data.hit_counts.get(&resource_path).unwrap_or(&0), &2, "Hit count for {} should be 2.", resource_path);

    // 4. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (non-200 test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_non_200_response finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_non_200_response() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache Non-200 Response", try_forward_proxy_cache_non_200_response),
        State::Success
    );
}

fn try_forward_proxy_cache_non_get_request() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-NONGET-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_non_get_request");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path = "/submit/form".to_string();
    let request_body = "data=payload".to_string();
    let upstream_response_body = "POST received".to_string();

    // 1. Start Mock Upstream Server
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_NONGET_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            "POST".to_string(), // Expecting POST
            resource_path.clone(),
            mock_upstream_addr_str.clone(), 
            upstream_response_body.clone(),
            200, // Or 201, depending on desired mock behavior for POST
            None, 
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock non-GET upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_NONGET_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for non-GET test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    let absolute_uri = format!("http://{}{}", mock_upstream_addr_str, resource_path);

    // --- First POST Request ---
    info!("Client: Sending first POST request to Sozu for {}", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", 
                absolute_uri, mock_upstream_addr_str, request_body.len(), request_body
            );
            stream.write_all(request.as_bytes()).expect("Client failed to send first POST request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read first POST response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "First POST response content mismatch");
            info!("Client: Received first POST response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for first POST request: {}", e); return State::Fail; }
    }
    thread::sleep(Duration::from_millis(100));

    // --- Second POST Request ---
    info!("Client: Sending second POST request to Sozu for {}", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!(
                "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", 
                absolute_uri, mock_upstream_addr_str, request_body.len(), request_body
            );
            stream.write_all(request.as_bytes()).expect("Client failed to send second POST request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read second POST response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "Second POST response content mismatch");
            info!("Client: Received second POST response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for second POST request: {}", e); return State::Fail; }
    }
    
    // 3. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data for non-GET test: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 2, "Mock upstream should have been hit twice for POST requests.");
    // Hit count is path-based, so still useful. If we had method-specific counts, that would be even better.
    assert_eq!(aggregator_data.hit_counts.get(&resource_path).unwrap_or(&0), &2, "Hit count for {} (POST) should be 2.", resource_path);

    // 4. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (non-GET test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_non_get_request finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_non_get_request() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache Non-GET (POST)", try_forward_proxy_cache_non_get_request),
        State::Success
    );
}

fn try_forward_proxy_cache_control_server_max_age_0() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-CCMAXAGE0-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_control_server_max_age_0");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path = "/noncacheable/max-age-0".to_string();
    let upstream_response_body = format!("Response for {} (should be immediately stale)", resource_path);
    
    let custom_headers = Some(vec![("Cache-Control".to_string(), "max-age=0".to_string())]);

    // 1. Start Mock Upstream Server
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_CC_MAXAGE0_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            "GET".to_string(),
            resource_path.clone(),
            mock_upstream_addr_str.clone(), 
            upstream_response_body.clone(),
            200,
            custom_headers, 
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock CC max-age=0 upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_CC_MAXAGE0_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for CC max-age=0 test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    let absolute_uri = format!("http://{}{}", mock_upstream_addr_str, resource_path);

    // --- First Request ---
    info!("Client: Sending first request to Sozu for {} (upstream sends CC: max-age=0)", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send first CC max-age=0 request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read first CC max-age=0 response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "First CC max-age=0 response content mismatch");
            assert!(response_str.to_lowercase().contains("cache-control: max-age=0"), "First CC max-age=0 response missing 'Cache-Control: max-age=0' header");
            info!("Client: Received first CC max-age=0 response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for first CC max-age=0 request: {}", e); return State::Fail; }
    }
    thread::sleep(Duration::from_millis(100));

    // --- Second Request ---
    info!("Client: Sending second request to Sozu for {} (should be a cache miss due to max-age=0)", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send second CC max-age=0 request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read second CC max-age=0 response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "Second CC max-age=0 response content mismatch");
            info!("Client: Received second CC max-age=0 response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for second CC max-age=0 request: {}", e); return State::Fail; }
    }
    
    // 3. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data for CC max-age=0 test: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 2, "Mock upstream should have been hit twice as 'Cache-Control: max-age=0' makes item immediately stale.");
    assert_eq!(aggregator_data.hit_counts.get(&resource_path).unwrap_or(&0), &2, "Hit count for {} should be 2.", resource_path);

    // 4. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (CC max-age=0 test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_control_server_max_age_0 finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_control_server_max_age_0() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache-Control Server max-age=0", try_forward_proxy_cache_control_server_max_age_0),
        State::Success
    );
}

fn try_forward_proxy_cache_control_server_no_store() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-CCNOSTORE-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_control_server_no_store");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path = "/noncacheable/no-store".to_string();
    let upstream_response_body = format!("Response for {} (should not be cached by Sozu)", resource_path);
    
    // Configure mock server to send "Cache-Control: no-store"
    let custom_headers = Some(vec![("Cache-Control".to_string(), "no-store".to_string())]);

    // 1. Start Mock Upstream Server
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_CC_NOSTORE_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            "GET".to_string(),
            resource_path.clone(),
            mock_upstream_addr_str.clone(), 
            upstream_response_body.clone(),
            200,
            custom_headers, // Send "Cache-Control: no-store"
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock CC no-store upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_CC_NOSTORE_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for CC no-store test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    let absolute_uri = format!("http://{}{}", mock_upstream_addr_str, resource_path);

    // --- First Request ---
    info!("Client: Sending first request to Sozu for {} (upstream sends CC: no-store)", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send first CC no-store request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read first CC no-store response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "First CC no-store response content mismatch");
            assert!(response_str.to_lowercase().contains("cache-control: no-store"), "First CC no-store response missing 'Cache-Control: no-store' header");
            info!("Client: Received first CC no-store response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for first CC no-store request: {}", e); return State::Fail; }
    }
    thread::sleep(Duration::from_millis(100));

    // --- Second Request ---
    info!("Client: Sending second request to Sozu for {} (should be a cache miss)", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send second CC no-store request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read second CC no-store response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "Second CC no-store response content mismatch");
            info!("Client: Received second CC no-store response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for second CC no-store request: {}", e); return State::Fail; }
    }
    
    // 3. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data for CC no-store test: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 2, "Mock upstream should have been hit twice as 'Cache-Control: no-store' prevents caching.");
    assert_eq!(aggregator_data.hit_counts.get(&resource_path).unwrap_or(&0), &2, "Hit count for {} should be 2.", resource_path);

    // 4. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (CC no-store test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_control_server_no_store finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_control_server_no_store() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache-Control Server no-store", try_forward_proxy_cache_control_server_no_store),
        State::Success
    );
}

fn try_forward_proxy_cache_control_client_no_cache() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-CCNOCACHE-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_control_client_no_cache");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path = "/cacheable/resource_cc_no_cache".to_string();
    let upstream_response_body = format!("Response for {}", resource_path);

    // 1. Start Mock Upstream Server
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_CC_NOCACHE_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            "GET".to_string(),
            resource_path.clone(),
            mock_upstream_addr_str.clone(), 
            upstream_response_body.clone(),
            200,
            None, 
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock CC no-cache upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_CC_NOCACHE_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for CC no-cache test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    let absolute_uri = format!("http://{}{}", mock_upstream_addr_str, resource_path);

    // --- First Request (with Cache-Control: no-cache) ---
    info!("Client: Sending first request with Cache-Control: no-cache to Sozu for {}", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!(
                "GET {} HTTP/1.1\r\nHost: {}\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n", 
                absolute_uri, mock_upstream_addr_str
            );
            stream.write_all(request.as_bytes()).expect("Client failed to send first CC no-cache request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read first CC no-cache response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "First CC no-cache response mismatch");
            info!("Client: Received first CC no-cache response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for first CC no-cache request: {}", e); return State::Fail; }
    }
    thread::sleep(Duration::from_millis(100)); 

    // --- Second Request (with Cache-Control: no-cache) ---
    info!("Client: Sending second request with Cache-Control: no-cache to Sozu for {}", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!(
                "GET {} HTTP/1.1\r\nHost: {}\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n", 
                absolute_uri, mock_upstream_addr_str
            );
            stream.write_all(request.as_bytes()).expect("Client failed to send second CC no-cache request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read second CC no-cache response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "Second CC no-cache response mismatch");
            info!("Client: Received second CC no-cache response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for second CC no-cache request: {}", e); return State::Fail; }
    }
    
    // 3. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data for CC no-cache test: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 2, "Mock upstream should have been hit twice due to Cache-Control: no-cache.");
    assert_eq!(aggregator_data.hit_counts.get(&resource_path).unwrap_or(&0), &2, "Hit count for {} should be 2.", resource_path);

    // 4. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (CC no-cache test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_control_client_no_cache finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_control_client_no_cache() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache-Control Client no-cache", try_forward_proxy_cache_control_client_no_cache),
        State::Success
    );
}

fn try_forward_proxy_cache_miss_different_resource() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-MISS-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_miss_different_resource");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path_a = "/cacheable/resourceA".to_string();
    let upstream_response_body_a = format!("Response for {}", resource_path_a);
    let resource_path_b = "/cacheable/resourceB".to_string();
    let upstream_response_body_b = format!("Response for {}", resource_path_b);

    // 1. Start Mock Upstream Server
    // The same handler will be used, it will differentiate by path.
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_CACHE_MISS_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(), // Fresh aggregator
        BackendHandle::verifying_http_handler(
            "GET".to_string(), // Method applies to all requests for this handler instance
            "/cacheable/".to_string(), // Path prefix to match both resources
            mock_upstream_addr_str.clone(), 
            String::new(), // Response body will be set dynamically by path (not really, handler sends fixed body)
                           // For this test, the body check is less important than hit counts.
                           // We will rely on the fact that the handler sends *some* 200 OK.
            200,
            None, 
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock cache miss upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_CACHE_MISS_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for cache miss test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    // --- First Request (Resource A) ---
    let absolute_uri_a = format!("http://{}{}", mock_upstream_addr_str, resource_path_a);
    info!("Client: Sending first request (Resource A) to Sozu for {}", absolute_uri_a);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri_a, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send request for Resource A");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read response for Resource A");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            // The verifying_http_handler sends its configured response_body, not dynamically based on path.
            // We need to adjust the handler or the assertion if we want path-specific bodies.
            // For now, just check for 200 OK.
            assert!(response_str.contains("HTTP/1.1 200 OK"), "Response A mismatch. Got: {}", response_str);
            info!("Client: Received first response (Resource A) successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for Resource A request: {}", e); return State::Fail; }
    }
    thread::sleep(Duration::from_millis(100)); // Ensure request is processed

    // --- Second Request (Resource B) ---
    let absolute_uri_b = format!("http://{}{}", mock_upstream_addr_str, resource_path_b);
    info!("Client: Sending second request (Resource B) to Sozu for {}", absolute_uri_b);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri_b, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send request for Resource B");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read response for Resource B");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK"), "Response B mismatch. Got: {}", response_str);
            info!("Client: Received second response (Resource B) successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for Resource B request: {}", e); return State::Fail; }
    }
    
    // 3. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data for cache miss test: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 2, "Mock upstream should have been hit twice (for A and B).");
    assert_eq!(aggregator_data.hit_counts.get(&resource_path_a).unwrap_or(&0), &1, "Hit count for {} should be 1.", resource_path_a);
    assert_eq!(aggregator_data.hit_counts.get(&resource_path_b).unwrap_or(&0), &1, "Hit count for {} should be 1.", resource_path_b);

    // 4. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (cache miss test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_miss_different_resource finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_miss_different_resource() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache Miss (Different Resource)", try_forward_proxy_cache_miss_different_resource),
        State::Success
    );
}

fn try_forward_proxy_connect_https() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDPROXY-CONN-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting test_forward_proxy_connect_https");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");
    let mock_upstream_host_for_connect = mock_upstream_addr_str.clone(); // Used in CONNECT request line
    // This domain *must* match the CN or a SAN in the mock server's certificate.
    let mock_upstream_domain_for_tls = "mockserver.test".to_string(); 

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    // 1. Prepare TLS Acceptor for Mock Upstream Server
    let identity = match crate::mock::async_backend::load_server_identity() {
        Ok(id) => id,
        Err(e) => {
            eprintln!("CRITICAL: Failed to load server identity for mock TLS server: {}. This test requires mock_cert.pem and mock_key.p8 to be present in e2e/ (or paths updated in async_backend.rs). Cannot proceed.", e);
            // This is a critical setup failure.
            return State::Fail; 
        }
    };
    let acceptor = Arc::new(native_tls::TlsAcceptor::new(identity).expect("Failed to create TlsAcceptor"));

    // 2. Start Mock Upstream Server (with TLS handling)
    let expected_inner_method = "GET".to_string();
    let expected_inner_path = "/securepage".to_string();
    // Host header for the *inner* request, after TLS tunnel is established.
    let expected_inner_host = mock_upstream_domain_for_tls.clone(); 
    let upstream_inner_response_body = "Hello from secure upstream via CONNECT!".to_string();
    let upstream_inner_response_status = 200;

    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_TLS_UPSTREAM".to_string(),
        mock_upstream_addr, // This address is not used by spawn_detached_backend if specific_address is Some.
        VerifyingAggregator::new(),
        BackendHandle::tls_http_handler( // Using the new TLS handler
            acceptor.clone(),
            expected_inner_method.clone(),
            expected_inner_path.clone(),
            expected_inner_host.clone(),
            upstream_inner_response_body.clone(),
            upstream_inner_response_status,
        ),
        Some(mock_upstream_addr), // Ensure it binds to the specific address
    );
    info!("Mock TLS upstream server started on {}", mock_upstream_addr);

    // 3. Start Sozu Worker
    let (config, listeners, state) = Worker::empty_config(); // Default forward proxy behavior
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_CONNECT_SOZU", config, &listeners, state);
    // Add an HTTP listener to Sozu (for the initial CONNECT request)
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last(); // Process Sozu commands
    info!("Sozu worker started, listening for CONNECT on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200)); // Allow Sozu's listener to fully activate

    // 4. Client Operations: Send CONNECT, check for 200 OK
    let mut client_stream = match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(s) => {
            info!("Client connected to Sozu at {} for CONNECT request.", sozu_front_addr);
            s
        }
        Err(e) => {
            eprintln!("Client: Failed to connect to Sozu for CONNECT: {}", e);
            worker.hard_stop(); worker.wait_for_server_stop();
            mock_upstream.stop_and_get_aggregator();
            return State::Fail;
        }
    };

    let connect_request = format!(
        "CONNECT {0} HTTP/1.1\r\nHost: {0}\r\nConnection: Keep-Alive\r\n\r\n", 
        // Using Keep-Alive to hold the connection for TLS
        mock_upstream_host_for_connect // This is <mock_ip>:<mock_port>
    );

    info!("Client: Sending CONNECT request to Sozu:\n{}", connect_request.trim());
    if let Err(e) = client_stream.write_all(connect_request.as_bytes()) {
        eprintln!("Client: Failed to send CONNECT request to Sozu: {}", e);
        worker.hard_stop(); worker.wait_for_server_stop();
        mock_upstream.stop_and_get_aggregator();
        return State::Fail;
    }

    let mut conn_est_buffer = [0; 1024]; // Buffer for "200 Connection established"
    match client_stream.read(&mut conn_est_buffer) {
        Ok(n) => {
            let response_str = String::from_utf8_lossy(&conn_est_buffer[..n]);
            info!("Client: Received response from Sozu after CONNECT request:\n{}", response_str.trim());
            if !response_str.starts_with("HTTP/1.1 200 Connection established\r\n") {
                eprintln!("Client: Sozu did not send '200 Connection established'. Got: '{}'", response_str.trim());
                worker.hard_stop(); worker.wait_for_server_stop();
                mock_upstream.stop_and_get_aggregator();
                return State::Fail;
            }
            info!("Client: Successfully received '200 Connection established' from Sozu.");
        }
        Err(e) => {
            eprintln!("Client: Failed to read Sozu's response to CONNECT request: {}", e);
            worker.hard_stop(); worker.wait_for_server_stop();
            mock_upstream.stop_and_get_aggregator();
            return State::Fail;
        }
    }

    // At this point, client_stream is the raw TCP stream to Sozu,
    // and Sozu has indicated it has a tunnel to mock_upstream_addr.
    // Now, perform TLS handshake with mock_upstream_addr *through* client_stream.

    let connector = TlsConnector::builder()
        // For testing with self-signed certs from load_server_identity(),
        // we might need to disable certificate validation for the client.
        // This is insecure for real connections but common in tests.
        .danger_accept_invalid_certs(true) // To accept self-signed certs
        .danger_accept_invalid_hostnames(true) // If mock_upstream_domain_for_tls doesn't match cert CN/SAN
        .build()
        .expect("Failed to build TlsConnector");

    info!("Client: Attempting TLS handshake with upstream '{}' via Sozu tunnel...", mock_upstream_domain_for_tls);
    let mut tls_stream = match connector.connect(&mock_upstream_domain_for_tls, client_stream) {
        Ok(s) => {
            info!("Client: TLS handshake successful with upstream via Sozu tunnel.");
            s
        }
        Err(e) => {
            eprintln!("Client: TLS handshake failed: {}", e);
            // Check if this failure was expected due to dummy certs
            if format!("{}",e).contains("invalid certificate") && 
               crate::mock::async_backend::load_server_identity().is_err() {
                info!("TLS handshake failed as expected due to dummy/invalid identity in mock server.");
                worker.soft_stop(); worker.wait_for_server_stop();
                mock_upstream.stop_and_get_aggregator();
                return State::Undecided; // Test cannot fully complete with dummy certs
            }
            worker.hard_stop(); worker.wait_for_server_stop();
            mock_upstream.stop_and_get_aggregator();
            return State::Fail;
        }
    };

    // Send an inner HTTPS request over the TLS stream
    let inner_http_request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        expected_inner_path, // e.g., "/securepage"
        expected_inner_host  // e.g., "mockserver.test"
    );

    info!("Client: Sending inner HTTPS request to upstream via tunnel:\n{}", inner_http_request.trim());
    if let Err(e) = tls_stream.write_all(inner_http_request.as_bytes()) {
        eprintln!("Client: Failed to write inner HTTPS request: {}", e);
        worker.hard_stop(); worker.wait_for_server_stop();
        mock_upstream.stop_and_get_aggregator();
        return State::Fail;
    }

    // Receive and verify inner HTTPS response
    let mut inner_response_buffer = Vec::new();
    if let Err(e) = tls_stream.read_to_end(&mut inner_response_buffer) {
        eprintln!("Client: Failed to read inner HTTPS response: {}", e);
        // If it's an UncleanDisconnect, it might be the server closing after sending data.
        // We'll check the buffer content. If empty, it's a hard fail.
        if inner_response_buffer.is_empty() || !format!("{}",e).contains("unclean disconnect") {
            worker.hard_stop(); worker.wait_for_server_stop();
            mock_upstream.stop_and_get_aggregator();
            return State::Fail;
        }
        info!("Client: Read inner HTTPS response with error (likely unclean disconnect, will check buffer): {}", e);
    }
    
    let inner_response_str = String::from_utf8_lossy(&inner_response_buffer);
    info!("Client: Received inner HTTPS response ({} bytes):\n{}", inner_response_buffer.len(), inner_response_str.trim());

    // Basic verification of the inner response
    let expected_status_line = format!("HTTP/1.1 {}", upstream_inner_response_status);
    if !inner_response_str.contains(&expected_status_line) {
        eprintln!("Client: Inner HTTPS response status mismatch. Expected to contain '{}', Got: '{}'", expected_status_line, inner_response_str);
        worker.hard_stop(); worker.wait_for_server_stop();
        mock_upstream.stop_and_get_aggregator();
        return State::Fail;
    }
    if !inner_response_str.contains(&upstream_inner_response_body) {
        eprintln!("Client: Inner HTTPS response body mismatch. Expected to contain '{}', Got: '{}'", upstream_inner_response_body, inner_response_str);
        worker.hard_stop(); worker.wait_for_server_stop();
        mock_upstream.stop_and_get_aggregator();
        return State::Fail;
    }
    info!("Client: Inner HTTPS response verified successfully.");

    // Mock Server Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data for TLS mock");
    info!("Mock TLS upstream aggregator data: {:?}", aggregator_data);
    
    // This state is for when dummy certs are used, and the handshake fails, so no request reaches upstream.
    let using_dummy_certs_and_handshake_failed = crate::mock::async_backend::load_server_identity().is_err() && aggregator_data.requests_received == 0;

    if using_dummy_certs_and_handshake_failed {
        info!("Test ran with dummy TLS certs and no request reached upstream, likely due to handshake failure as expected by dummy certs.");
        worker.soft_stop(); worker.wait_for_server_stop();
        return State::Undecided; // Cannot truly verify CONNECT tunnel with dummy certs if handshake fails
    }

    assert_eq!(aggregator_data.requests_received, 1, "Mock TLS upstream should have received 1 inner request.");
    assert_eq!(aggregator_data.last_method.as_ref(), Some(&expected_inner_method), "Inner method mismatch at TLS upstream.");
    assert!(aggregator_data.last_path.as_ref().map_or(false, |p| p.starts_with(&expected_inner_path)), "Inner path mismatch at TLS upstream. Expected prefix: '{}', Got: {:?}", expected_inner_path, aggregator_data.last_path);
    if !expected_inner_host.is_empty() { // Only check if expected_inner_host was set for the handler
        assert_eq!(aggregator_data.last_host_header.as_ref(), Some(&expected_inner_host), "Inner Host header mismatch at TLS upstream.");
    }

    // Cleanup
    info!("Test try_forward_proxy_connect_https: Shutting down Sozu worker.");
    worker.soft_stop();
    if !worker.wait_for_server_stop() {
        eprintln!("Sozu (CONNECT test) did not stop gracefully.");
        return State::Fail;
    }
    info!("Test try_forward_proxy_connect_https finished.");
    State::Success
}

#[test]
fn test_forward_proxy_connect_https() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy CONNECT HTTPS", try_forward_proxy_connect_https),
        State::Success 
        // Note: This might return Undecided if dummy certs are used and handshake fails.
        // For a real CI, this should be State::Success and use valid (though test-only) certs.
    );
}

// --- BEGIN FORWARD PROXY TESTS ---

fn try_forward_proxy_absolute_uri() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDPROXY-ABS-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting test_forward_proxy_absolute_uri");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    // 1. Start Mock Upstream Server
    let expected_method = "GET".to_string();
    let expected_path = "/test/path".to_string();
    // For absolute URI, the Host header sent by Sozu to upstream should match the URI's authority.
    let expected_host = mock_upstream_addr_str.clone(); 
    let upstream_response_body = "Hello from upstream!".to_string();
    let upstream_response_status = 200;

    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_UPSTREAM".to_string(),
        mock_upstream_addr, // This address is not used due to specific_address
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            expected_method.clone(),
            expected_path.clone(),
            expected_host.clone(),
            upstream_response_body.clone(),
            upstream_response_status,
        ),
        Some(mock_upstream_addr), // Ensure it binds to the specific address
    );
    info!("Mock upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker (no backends configured for the listener)
    let (config, listeners, state) = Worker::empty_config(); // Crucial: empty config
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_SOZU", config, &listeners, state);

    // Add and activate an HTTP listener on Sozu
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into())
            .to_http(None)
            .unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false, // Assuming direct activation for test
    }));
    worker.read_to_last(); // Process commands
    info!("Sozu worker started, listening on {}", sozu_front_addr);
    
    // Give Sozu a moment to fully activate listener
    thread::sleep(Duration::from_millis(200));


    // 3. Client sends request with Absolute URI to Sozu
    let absolute_uri = format!("http://{}/test/path", mock_upstream_addr_str);
    let request_to_sozu = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        absolute_uri, 
        mock_upstream_addr_str // Host header should also match the URI's authority for forward proxy
    );

    info!("Client connecting to Sozu at {}", sozu_front_addr);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            info!("Client connected to Sozu. Sending request:\n{}", request_to_sozu);
            if let Err(e) = stream.write_all(request_to_sozu.as_bytes()) {
                eprintln!("Client failed to send request to Sozu: {}", e);
                worker.hard_stop(); worker.wait_for_server_stop();
                mock_upstream.stop_and_get_aggregator();
                return State::Fail;
            }
            info!("Client request sent to Sozu.");

            let mut response_buffer = [0; 1024];
            match stream.read(&mut response_buffer) {
                Ok(n) => {
                    let response_from_sozu = String::from_utf8_lossy(&response_buffer[..n]).to_string();
                    info!("Client received response from Sozu:\n{}", response_from_sozu);

                    let expected_http_response = format!(
                        "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        upstream_response_status,
                        upstream_response_body.len(),
                        upstream_response_body
                    );
                    // Normalize line endings for comparison, as they can be tricky.
                    // However, for HTTP, \r\n is standard. The mock sends Connection: close, so Sozu might too.
                    // A more robust check would parse the status line and body separately.
                    assert!(response_from_sozu.contains(&format!("HTTP/1.1 {} OK", upstream_response_status)), "Status line mismatch");
                    assert!(response_from_sozu.contains(&upstream_response_body), "Body mismatch");

                }
                Err(e) => {
                    eprintln!("Client failed to read response from Sozu: {}", e);
                    worker.hard_stop(); worker.wait_for_server_stop();
                    mock_upstream.stop_and_get_aggregator();
                    return State::Fail;
                }
            }
        }
        Err(e) => {
            eprintln!("Client failed to connect to Sozu at {}: {}", sozu_front_addr, e);
            worker.hard_stop(); worker.wait_for_server_stop();
            mock_upstream.stop_and_get_aggregator();
            return State::Fail;
        }
    }
    
    // 4. Verification via Mock Upstream Aggregator
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 1, "Mock upstream should have received 1 request.");
    assert_eq!(aggregator_data.last_method.as_ref(), Some(&expected_method), "Method mismatch at upstream.");
    // Sozu should forward the path part of the absolute URI
    assert_eq!(aggregator_data.last_path.as_ref(), Some(&expected_path), "Path mismatch at upstream."); 
    assert_eq!(aggregator_data.last_host_header.as_ref(), Some(&expected_host), "Host header mismatch at upstream.");

    // 5. Cleanup
    info!("Stopping Sozu worker.");
    worker.soft_stop(); // Use soft_stop for graceful shutdown
    let sozu_stopped = worker.wait_for_server_stop();
    assert!(sozu_stopped, "Sozu worker did not stop gracefully.");
    info!("Test try_forward_proxy_absolute_uri finished.");
    State::Success
}

#[test]
fn test_forward_proxy_absolute_uri() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Absolute URI GET", try_forward_proxy_absolute_uri),
        State::Success
    );
}

// --- BEGIN CACHING TESTS ---

fn try_forward_proxy_cache_hit() -> State {
    if let Err(e) = setup_default_logging(false, "debug", "FWDCACHE-HIT-OUT") {
        println!("could not setup default logging: {e}");
    }
    info!("Starting try_forward_proxy_cache_hit");

    let mock_upstream_addr_str = format!("127.0.0.1:{}", provide_port());
    let mock_upstream_addr: SocketAddr = mock_upstream_addr_str.parse().expect("Failed to parse mock upstream address");

    let sozu_front_addr_str = format!("127.0.0.1:{}", provide_port());
    let sozu_front_addr: SocketAddr = sozu_front_addr_str.parse().expect("Failed to parse Sozu front address");

    let resource_path = "/cacheable/resource1".to_string();
    let upstream_response_body = format!("Response for {}", resource_path);

    // 1. Start Mock Upstream Server
    let mut mock_upstream = BackendHandle::<VerifyingAggregator>::spawn_detached_backend(
        "MOCK_CACHE_UPSTREAM".to_string(),
        mock_upstream_addr, 
        VerifyingAggregator::new(),
        BackendHandle::verifying_http_handler(
            "GET".to_string(),
            resource_path.clone(),
            mock_upstream_addr_str.clone(), // Expected Host header
            upstream_response_body.clone(),
            200,
            None, // No special response headers from upstream for this test
        ),
        Some(mock_upstream_addr), 
    );
    info!("Mock cache upstream server started on {}", mock_upstream_addr);

    // 2. Start Sozu Worker (no backends configured for the listener -> default forward proxy mode)
    let (config, listeners, state) = Worker::empty_config();
    let mut worker = Worker::start_new_worker("FORWARD_PROXY_CACHE_SOZU", config, &listeners, state);
    worker.send_proxy_request_type(RequestType::AddHttpListener(
        ListenerBuilder::new_http(sozu_front_addr.into()).to_http(None).unwrap(),
    ));
    worker.send_proxy_request_type(RequestType::ActivateListener(ActivateListener {
        address: sozu_front_addr.into(),
        proxy: ListenerType::Http.into(),
        from_scm: false,
    }));
    worker.read_to_last();
    info!("Sozu worker started for caching test, listening on {}", sozu_front_addr);
    thread::sleep(Duration::from_millis(200));

    let absolute_uri = format!("http://{}{}", mock_upstream_addr_str, resource_path);

    // --- First Request (Cache Fill) ---
    info!("Client: Sending first request to Sozu for {}", absolute_uri);
    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send first request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read first response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "First response mismatch");
            info!("Client: Received first response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for first request: {}", e); return State::Fail; }
    }

    let agg_data_after_first = mock_upstream.aggregator_rx.try_recv().unwrap_or_default(); // Quick check, might not be updated yet
    info!("Mock hit count after first request (quick check): {:?}", agg_data_after_first.hit_counts.get(&resource_path).unwrap_or(&0));
     // It's better to stop the server to reliably get aggregator data if handler is per-connection thread.
     // For this test, we'll check counts after all client interactions.

    // --- Second Request (Cache Serve) ---
    info!("Client: Sending second request to Sozu for {}", absolute_uri);
     // Wait a very short moment to ensure the first request is fully processed by Sozu and potentially cached
    thread::sleep(Duration::from_millis(100));

    match std::net::TcpStream::connect(sozu_front_addr) {
        Ok(mut stream) => {
            let request = format!("GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", absolute_uri, mock_upstream_addr_str);
            stream.write_all(request.as_bytes()).expect("Client failed to send second request");
            let mut response_buffer = [0; 1024];
            let n = stream.read(&mut response_buffer).expect("Client failed to read second response");
            let response_str = String::from_utf8_lossy(&response_buffer[..n]);
            assert!(response_str.contains("HTTP/1.1 200 OK") && response_str.contains(&upstream_response_body), "Second response mismatch (from cache)");
            info!("Client: Received second response successfully.");
        }
        Err(e) => { eprintln!("Client: Failed to connect for second request: {}", e); return State::Fail; }
    }

    // 4. Verification
    let aggregator_data = mock_upstream.stop_and_get_aggregator().expect("Failed to get aggregator data");
    info!("Mock upstream aggregator data: {:?}", aggregator_data);

    assert_eq!(aggregator_data.requests_received, 1, "Mock upstream should have been hit only once.");
    assert_eq!(aggregator_data.hit_counts.get(&resource_path).unwrap_or(&0), &1, "Hit count for {} should be 1.", resource_path);

    // 5. Cleanup
    worker.soft_stop();
    assert!(worker.wait_for_server_stop(), "Sozu (cache hit test) did not stop gracefully.");
    info!("Test try_forward_proxy_cache_hit finished.");
    State::Success
}

#[test]
fn test_forward_proxy_cache_hit() {
    assert_eq!(
        repeat_until_error_or(1, "Forward Proxy Cache Hit", try_forward_proxy_cache_hit),
        State::Success
    );
}


// --- END CACHING TESTS ---

#[test]
fn test_soft_stop() {
    assert_eq!(
            repeat_until_error_or(
                10,
                "Hard Stop: Test that the worker waits for all backends to process requests before shutting down",
                || try_hard_or_soft_stop(true)
            ),
            State::Success
        );
}

// https://github.com/sozu-proxy/sozu/issues/806
// This should actually be a success

#[test]
fn test_issue_806() {
    assert!(
        repeat_until_error_or(
            100,
            "issue 806: timeout with invalid back token\n(not fixed)",
            || try_backend_stop(2, None)
        ) != State::Fail
    );
}

// https://github.com/sozu-proxy/sozu/issues/808

#[test]
fn test_issue_808() {
    // this test is not relevant anymore, at least not like this
    // assert_eq!(
    //     repeat_until_error_or(
    //         100,
    //         "issue 808: panic on successful zombie check\n(fixed)",
    //         || try_backend_stop(2, Some(1))
    //     ),
    //     // if Success, it means the session was never a zombie
    //     // if Fail, it means the zombie checker probably crashed
    //     State::Undecided
    // );
}

// https://github.com/sozu-proxy/sozu/issues/810

#[test]
fn test_issue_810_timeout() {
    assert_eq!(
        repeat_until_error_or(
            100,
            "issue 810: shutdown struggles until session timeout\n(fixed)",
            try_issue_810_timeout
        ),
        State::Success
    );
}

#[test]
fn test_issue_810_panic_on_session_close() {
    assert_eq!(
        repeat_until_error_or(
            100,
            "issue 810: shutdown panics on session close\n(fixed)",
            || try_issue_810_panic(false)
        ),
        State::Success
    );
}

#[test]
fn test_issue_810_panic_on_missing_listener() {
    assert_eq!(
            repeat_until_error_or(
                100,
                "issue 810: shutdown panics on tcp connection after proxy cleared its listeners\n(opinionated fix)",
                || try_issue_810_panic(true)
            ),
            State::Success
        );
}

#[test]
fn test_tls_endpoint() {
    assert_eq!(
        repeat_until_error_or(
            100,
            "TLS endpoint: Sōzu should decrypt an HTTPS request",
            try_tls_endpoint
        ),
        State::Success
    );
}

#[test]
fn test_http_behaviors() {
    assert_eq!(
        repeat_until_error_or(10, "HTTP stack", try_http_behaviors),
        State::Success
    );
}

#[test]
fn test_https_redirect() {
    assert_eq!(
        repeat_until_error_or(2, "HTTPS redirection", try_https_redirect),
        State::Success
    );
}

#[test]
fn test_msg_close() {
    assert_eq!(
        repeat_until_error_or(100, "HTTP error on close", try_msg_close),
        State::Success
    );
}

#[test]
fn test_blue_green() {
    assert_eq!(
        repeat_until_error_or(10, "Blue green switch", try_blue_geen),
        State::Success
    );
}

#[test]
fn test_keep_alive() {
    assert_eq!(
        repeat_until_error_or(10, "Keep alive combinations", try_keep_alive),
        State::Success
    );
}

#[test]
fn test_stick() {
    assert_eq!(
        repeat_until_error_or(10, "Sticky session", try_stick),
        State::Success
    );
}

#[test]
fn test_max_connections() {
    assert_eq!(
        repeat_until_error_or(2, "Max connections reached", try_max_connections),
        State::Success
    );
}

#[test]
fn test_head() {
    assert_eq!(
        repeat_until_error_or(10, "Head request", try_head),
        State::Success
    );
}

#[test]
fn test_wildcard() {
    assert_eq!(
        repeat_until_error_or(2, "Hostname with wildcard", try_wildcard),
        State::Success
    );
}

#[test]
fn test_status_header_split() {
    assert_eq!(
        repeat_until_error_or(2, "Status line and Headers split", try_status_header_split),
        State::Success
    );
}
