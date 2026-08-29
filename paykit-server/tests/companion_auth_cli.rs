use std::{
    io::{self, Read, Write},
    net::TcpListener,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bitcoin::{
    Network,
    bip32::{ChildNumber, Xpriv, Xpub},
    secp256k1::Secp256k1,
};
use paykit_server::bitkit_claim::{
    CLAIM_TYPE, LOCAL_DEMO_CAPABILITIES, QUERY_PARAMETER, UNSIGNED_PAYLOAD_LEN, decrypt_and_verify,
    derive_channel_id, encode_unsigned_payload, parse_unsigned_payload,
};
use pubky::{HttpRelayInboxChannel, PubkyHttpClient};
use serde_json::{Value, json};

const HELPER_DEADLINE: Duration = Duration::from_secs(5);
const FIXTURE_DEADLINE: Duration = Duration::from_secs(6);
const RELAY_TASK_DEADLINE: Duration = Duration::from_secs(7);
const HELPER_TIMEOUT: &str = "helper process exceeded test deadline";
const COMPANION_HANDLE_BYTES: [u8; 32] = [5; 32];

fn account_xpub(network: Network, account_index: u32) -> Xpub {
    let secp = Secp256k1::new();
    let account = Xpriv::new_master(network, &[42; 32])
        .unwrap()
        .derive_priv(
            &secp,
            &[
                ChildNumber::from_hardened_idx(84).unwrap(),
                ChildNumber::from_hardened_idx(1).unwrap(),
                ChildNumber::from_hardened_idx(account_index).unwrap(),
            ],
        )
        .unwrap();
    Xpub::from_priv(&secp, &account)
}

fn auth_url(relay: &str, auth_secret: &[u8; 32]) -> String {
    let client_public_key = pubky::Keypair::from_secret(&[8; 32]).public_key();
    format!(
        "pubkyauth://signin_grant?caps={LOCAL_DEMO_CAPABILITIES}&relay={relay}&secret={}&cid=app.paykit.server&cpk={}&{QUERY_PARAMETER}={CLAIM_TYPE}",
        URL_SAFE_NO_PAD.encode(auth_secret),
        client_public_key.as_inner(),
    )
}

fn valid_input() -> Value {
    json!({
        "version": 1,
        "companion_handle": URL_SAFE_NO_PAD.encode(COMPANION_HANDLE_BYTES),
        "creator_secret": URL_SAFE_NO_PAD.encode([7; 32]),
        "account_xpub": account_xpub(Network::Testnet, 0).to_string(),
        "account_index": 0,
    })
}

fn spawn_helper() -> Child {
    spawn_helper_with_server("http://127.0.0.1:1")
}

fn spawn_helper_with_server(server_url: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_paykit-companion-auth"))
        .env("PAYKIT_SERVER_URL", server_url)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn spawn_auth_server(auth_url: String) -> (String, std::thread::JoinHandle<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let server_url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let started = Instant::now();
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(accepted) => break accepted,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= FIXTURE_DEADLINE {
                        return Err("auth request fixture accept timed out".to_owned());
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return Err("auth request fixture accept failed".to_owned()),
            }
        };
        stream
            .set_read_timeout(Some(FIXTURE_DEADLINE))
            .map_err(|_| "auth request fixture deadline failed".to_owned())?;
        let expected_handle = URL_SAFE_NO_PAD.encode(COMPANION_HANDLE_BYTES);
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream
                .read(&mut buffer)
                .map_err(|_| "auth request fixture read failed".to_owned())?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n")
                && request
                    .windows(expected_handle.len())
                    .any(|window| window == expected_handle.as_bytes())
            {
                break;
            }
        }
        let request_text = String::from_utf8(request)
            .map_err(|_| "auth request fixture received non-UTF8".to_owned())?;
        if !request_text.starts_with("POST /setup/companion-auth-request HTTP/1.1\r\n")
            || !request_text.contains(&expected_handle)
        {
            return Err("auth request fixture received wrong request".to_owned());
        }
        let body = json!({"version": 1, "auth_url": auth_url}).to_string();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .map_err(|_| "auth request fixture response failed".to_owned())?;
        Ok(())
    });
    (server_url, server)
}

fn spawn_raw_auth_server(
    response: Vec<u8>,
) -> (String, std::thread::JoinHandle<Result<(), String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server_url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        listener
            .set_nonblocking(false)
            .map_err(|_| "raw auth fixture blocking mode failed".to_owned())?;
        let (mut stream, _) = listener
            .accept()
            .map_err(|_| "raw auth fixture accept failed".to_owned())?;
        stream
            .set_read_timeout(Some(FIXTURE_DEADLINE))
            .map_err(|_| "raw auth fixture deadline failed".to_owned())?;
        let mut request = [0_u8; 4096];
        let read = stream
            .read(&mut request)
            .map_err(|_| "raw auth fixture read failed".to_owned())?;
        if !request[..read]
            .windows(b"POST /setup/companion-auth-request ".len())
            .any(|window| window == b"POST /setup/companion-auth-request ")
        {
            return Err("raw auth fixture received wrong request".to_owned());
        }
        stream
            .write_all(&response)
            .map_err(|_| "raw auth fixture response failed".to_owned())?;
        Ok(())
    });
    (server_url, server)
}

fn wait_with_output_deadline(mut child: Child, deadline: Duration) -> Result<Output, &'static str> {
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            return Err(HELPER_TIMEOUT);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout).unwrap();
    }
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr).unwrap();
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn run_helper(input: &Value) -> Output {
    let mut child = spawn_helper();
    serde_json::to_writer(child.stdin.as_mut().unwrap(), input).unwrap();
    child.stdin.take().unwrap().flush().unwrap();
    wait_with_output_deadline(child, HELPER_DEADLINE).expect(HELPER_TIMEOUT)
}

fn run_helper_with_server(input: &Value, server_url: &str) -> Output {
    let mut child = spawn_helper_with_server(server_url);
    serde_json::to_writer(child.stdin.as_mut().unwrap(), input).unwrap();
    child.stdin.take().unwrap().flush().unwrap();
    wait_with_output_deadline(child, HELPER_DEADLINE).expect(HELPER_TIMEOUT)
}

#[test]
fn helper_accepts_only_a_companion_handle_not_a_caller_supplied_auth_url() {
    let mut old_input = valid_input();
    old_input
        .as_object_mut()
        .unwrap()
        .remove("companion_handle");
    old_input["auth_url"] = json!(auth_url("http://127.0.0.1:1/inbox", &[9; 32]));
    let old_output = run_helper_with_server(&old_input, "http://127.0.0.1:1");
    assert!(!old_output.status.success());
    assert_eq!(
        String::from_utf8(old_output.stderr).unwrap(),
        "invalid input\n"
    );

    let handle_input = valid_input();
    let handle_output = run_helper_with_server(&handle_input, "http://127.0.0.1:1");
    assert!(!handle_output.status.success());
    assert_eq!(
        String::from_utf8(handle_output.stderr).unwrap(),
        "companion authentication failed\n"
    );
}

#[test]
fn helper_rejects_unsafe_server_origins_and_untrusted_responses() {
    for server_url in [
        "file:///tmp/paykit",
        "http://user:password@127.0.0.1:3001",
        "http://127.0.0.1:3001/path",
        "http://127.0.0.1:3001?query=1",
        "http://127.0.0.1:3001#fragment",
    ] {
        let output = run_helper_with_server(&valid_input(), server_url);
        assert!(!output.status.success());
        assert_eq!(String::from_utf8(output.stderr).unwrap(), "invalid input\n");
    }

    let valid_body = r#"{"version":1,"auth_url":"pubkyauth://signin_grant"}"#;
    let mut oversized_chunked = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
        16 * 1024 + 1
    )
    .into_bytes();
    oversized_chunked.extend(vec![b'a'; 16 * 1024 + 1]);
    oversized_chunked.extend_from_slice(b"\r\n0\r\n\r\n");
    let responses = vec![
        b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/evil\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        format!("HTTP/1.1 200 OK\r\nContent-Type: application/jsonevil\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{valid_body}", valid_body.len()).into_bytes(),
        format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{valid_body}", valid_body.len()).into_bytes(),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: 20000\r\nConnection: close\r\n\r\n".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: 58\r\nConnection: close\r\n\r\n{\"version\":2,\"auth_url\":\"pubkyauth://signin_grant\"}".to_vec(),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: 71\r\nConnection: close\r\n\r\n{\"version\":1,\"auth_url\":\"pubkyauth://signin_grant\",\"extra\":true}".to_vec(),
        oversized_chunked,
    ];
    for response in responses {
        let (server_url, server) = spawn_raw_auth_server(response);
        let output = run_helper_with_server(&valid_input(), &server_url);
        server.join().unwrap().unwrap();
        assert!(!output.status.success());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            "companion authentication failed\n"
        );
    }
}

#[test]
fn helper_deadline_kills_and_reaps_without_waiting_for_product_io() {
    let child = spawn_helper();

    let error = wait_with_output_deadline(child, Duration::ZERO).unwrap_err();

    assert_eq!(error, HELPER_TIMEOUT);
}

#[test]
fn unsigned_payload_encoder_matches_the_canonical_84_byte_parser_fixture() {
    let account_index = 7;
    let serialized_xpub = account_xpub(Network::Testnet, account_index).encode();

    let encoded = encode_unsigned_payload(account_index, &serialized_xpub);

    assert_eq!(encoded.len(), UNSIGNED_PAYLOAD_LEN);
    assert_eq!(encoded[0], 1);
    assert_eq!(&encoded[1..5], &account_index.to_be_bytes());
    assert_eq!(encoded[5], 0);
    assert_eq!(&encoded[6..], &serialized_xpub);
    let parsed = parse_unsigned_payload(&encoded).unwrap();
    assert_eq!(parsed.account_index, account_index);
    assert_eq!(parsed.serialized_xpub, serialized_xpub);
}

#[test]
fn helper_rejects_unknown_missing_fields_and_non_object_input() {
    let mut unknown = valid_input();
    unknown["extra"] = json!(true);
    let mut invalid = vec![unknown, json!([])];
    for field in [
        "version",
        "companion_handle",
        "creator_secret",
        "account_xpub",
        "account_index",
    ] {
        let mut missing = valid_input();
        missing.as_object_mut().unwrap().remove(field);
        invalid.push(missing);
    }

    for input in invalid {
        let output = run_helper(&input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(String::from_utf8(output.stderr).unwrap(), "invalid input\n");
    }
}

#[test]
fn helper_requires_protocol_version_one() {
    for version in [0, 2] {
        let mut input = valid_input();
        input["version"] = json!(version);
        let output = run_helper(&input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(String::from_utf8(output.stderr).unwrap(), "invalid input\n");
    }

    let output = run_helper(&valid_input());
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "companion authentication failed\n"
    );
}

#[test]
fn helper_requires_an_unpadded_base64url_secret_of_exactly_32_bytes() {
    for creator_secret in [
        URL_SAFE_NO_PAD.encode([7; 31]),
        URL_SAFE_NO_PAD.encode([7; 33]),
        format!("{}=", URL_SAFE_NO_PAD.encode([7; 32])),
        "not_base64url!".to_owned(),
    ] {
        let mut input = valid_input();
        input["creator_secret"] = json!(creator_secret);
        let output = run_helper(&input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(String::from_utf8(output.stderr).unwrap(), "invalid input\n");
    }
}

#[test]
fn helper_rejects_non_tpub_non_account_and_mismatched_account_keys() {
    let secp = Secp256k1::new();
    let testnet_master = Xpub::from_priv(
        &secp,
        &Xpriv::new_master(Network::Testnet, &[42; 32]).unwrap(),
    );
    let invalid = [
        account_xpub(Network::Bitcoin, 0).to_string(),
        testnet_master.to_string(),
        account_xpub(Network::Testnet, 1).to_string(),
        "not-an-xpub".to_owned(),
    ];

    for account_xpub in invalid {
        let mut input = valid_input();
        input["account_xpub"] = json!(account_xpub);
        let output = run_helper(&input);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(String::from_utf8(output.stderr).unwrap(), "invalid input\n");
    }
}

#[test]
fn helper_rejects_account_indexes_outside_the_bip32_hardened_index_range() {
    let mut input = valid_input();
    input["account_index"] = json!(1_u64 << 31);

    let output = run_helper(&input);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(String::from_utf8(output.stderr).unwrap(), "invalid input\n");
}

#[test]
fn helper_rejects_another_client_id_before_relay_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let relay = format!("http://{}/inbox", listener.local_addr().unwrap());
    let input = valid_input();
    let substituted =
        auth_url(&relay, &[9; 32]).replace("cid=app.paykit.server", "cid=other.paykit.server");
    let (server_url, auth_server) = spawn_auth_server(substituted);

    let output = run_helper_with_server(&input, &server_url);
    auth_server.join().unwrap().unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "companion authentication failed\n"
    );
    assert!(matches!(
        listener.accept(),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
}

#[test]
fn helper_failure_is_coarse_and_redacts_every_sensitive_input() {
    let input = valid_input();
    let (server_url, auth_server) = spawn_auth_server(auth_url(
        "http://127.0.0.1:1/inbox/private-channel",
        &[9; 32],
    ));

    let output = run_helper_with_server(&input, &server_url);
    auth_server.join().unwrap().unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr, "companion authentication failed\n");
    for sensitive in [
        input["companion_handle"].as_str().unwrap(),
        input["creator_secret"].as_str().unwrap(),
        input["account_xpub"].as_str().unwrap(),
        "private-channel",
    ] {
        assert!(!stderr.contains(sensitive));
    }
}

#[test]
fn helper_attempts_the_companion_channel_before_normal_auth_approval() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let relay = format!("http://{}/inbox", listener.local_addr().unwrap());
    let expected_channel = derive_channel_id(&[9; 32]);
    let server = std::thread::spawn(move || {
        let started = Instant::now();
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(accepted) => break accepted,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= FIXTURE_DEADLINE {
                        return Err("ordering fixture accept exceeded test deadline");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return Err("ordering fixture accept failed"),
            }
        };
        stream.set_nonblocking(true).unwrap();
        let mut request_line = Vec::new();
        loop {
            let mut byte = [0];
            match stream.read(&mut byte) {
                Ok(0) => return Err("ordering fixture request ended before request line"),
                Ok(_) => {
                    request_line.push(byte[0]);
                    if byte[0] == b'\n' {
                        break;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= FIXTURE_DEADLINE {
                        return Err("ordering fixture read exceeded test deadline");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return Err("ordering fixture read failed"),
            }
        }
        stream
            .write_all(
                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .map_err(|_| "ordering fixture response failed")?;
        String::from_utf8(request_line).map_err(|_| "ordering fixture request line was not UTF-8")
    });

    let (server_url, auth_server) = spawn_auth_server(auth_url(&relay, &[9; 32]));
    let output = run_helper_with_server(&valid_input(), &server_url);
    auth_server.join().unwrap().unwrap();
    let first_request = server
        .join()
        .unwrap()
        .expect("ordering fixture failed without sensitive details");

    assert!(!output.status.success());
    assert!(first_request.contains(&expected_channel), "{first_request}");
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "companion authentication failed\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn helper_reports_coarse_failure_without_panicking_when_success_stdout_is_broken() {
    let relay = http_relay::HttpRelay::builder()
        .http_port(0)
        .run()
        .await
        .unwrap();
    let inbox = relay.local_url().join("inbox").unwrap();
    let input = valid_input();
    let (server_url, auth_server) = spawn_auth_server(auth_url(inbox.as_str(), &[9; 32]));
    let sensitive = [
        input["companion_handle"].as_str().unwrap().to_owned(),
        input["creator_secret"].as_str().unwrap().to_owned(),
        input["account_xpub"].as_str().unwrap().to_owned(),
    ];

    let output = tokio::time::timeout(
        RELAY_TASK_DEADLINE,
        tokio::task::spawn_blocking(move || {
            let mut child = spawn_helper_with_server(&server_url);
            drop(child.stdout.take());
            serde_json::to_writer(child.stdin.as_mut().unwrap(), &input).unwrap();
            child.stdin.take().unwrap().flush().unwrap();
            wait_with_output_deadline(child, HELPER_DEADLINE).expect(HELPER_TIMEOUT)
        }),
    )
    .await
    .expect("relay test task exceeded test deadline")
    .unwrap();
    auth_server.join().unwrap().unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr, "companion authentication failed\n");
    assert!(!stderr.contains("panicked"), "{stderr}");
    for sensitive in sensitive {
        assert!(!stderr.contains(&sensitive));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn helper_delivers_the_exact_companion_envelope_and_grant() {
    let relay = http_relay::HttpRelay::builder()
        .http_port(0)
        .run()
        .await
        .unwrap();
    let inbox = relay.local_url().join("inbox").unwrap();
    let input = valid_input();
    let (server_url, auth_server) = spawn_auth_server(auth_url(inbox.as_str(), &[9; 32]));

    let output = tokio::time::timeout(
        RELAY_TASK_DEADLINE,
        tokio::task::spawn_blocking(move || run_helper_with_server(&input, &server_url)),
    )
    .await
    .expect("relay test task exceeded test deadline")
    .unwrap();
    auth_server.join().unwrap().unwrap();

    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, b"{\"version\":1,\"status\":\"approved\"}\n");
    assert!(output.stderr.is_empty());

    let client = PubkyHttpClient::new().unwrap();
    let claim_channel =
        HttpRelayInboxChannel::new(inbox.clone(), derive_channel_id(&[9; 32])).unwrap();
    let encrypted_claim = claim_channel
        .poll(&client, Some(Duration::from_secs(1)))
        .await
        .unwrap()
        .unwrap();
    let creator = pubky::Keypair::from_secret(&[7; 32]);
    let verifying_key =
        ed25519_dalek::VerifyingKey::from_bytes(creator.public_key().as_bytes()).unwrap();
    let claim = decrypt_and_verify(&encrypted_claim, &[9; 32], &verifying_key).unwrap();
    assert_eq!(claim.account_index, 0);
    assert_eq!(
        claim.serialized_xpub,
        account_xpub(Network::Testnet, 0).encode()
    );
}
