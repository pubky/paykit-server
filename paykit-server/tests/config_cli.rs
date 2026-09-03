use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const KEY: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";
const MASTER_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
const DATABASE_URL: &str = "postgres://paykit:deployment-check-secret@127.0.0.1:1/paykit";

fn config(client_id: bool) -> String {
    format!(
        r#"[http]
listen_addr = "127.0.0.1:3001"
[locks]
trusted_public_key = "{KEY}"
[setup]
allowed_origins = ["http://127.0.0.1:8080"]
[paykit]
{}receiver_path = "bitkit/server"
network = "mainnet"
[bitcoin]
network = "regtest"
[electrum]
endpoint = "tcp://127.0.0.1:50001"
[outbox]
poll_interval = "5s"
"#,
        if client_id {
            "client_id = \"app.paykit.server\"\n"
        } else {
            ""
        }
    )
}

fn run_check(source: &str, extra_args: &[&str]) -> std::process::Output {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("paykit-check-config-{nonce}.toml"));
    fs::write(&path, source).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_paykit-server"))
        .arg("--check-config")
        .args(extra_args)
        .env("PAYKIT_CONFIG", &path)
        .env("PAYKIT_DATABASE_URL", DATABASE_URL)
        .env("PAYKIT_MASTER_KEY", MASTER_KEY)
        .output()
        .unwrap();
    fs::remove_file(path).unwrap();
    output
}

#[test]
fn check_config_validates_without_connecting_to_postgres() {
    let output = run_check(&config(true), &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "configuration valid"
    );
}

#[test]
fn check_config_reports_missing_client_id_without_secrets() {
    let output = run_check(&config(false), &[]);
    assert!(!output.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(combined.contains("paykit.client_id"), "{combined}");
    assert!(!combined.contains(MASTER_KEY));
    assert!(!combined.contains(DATABASE_URL));
    assert!(!combined.contains("deployment-check-secret"));
}

#[test]
fn unknown_arguments_fail_closed() {
    let output = run_check(&config(true), &["--unexpected"]);
    assert!(!output.status.success());
}
