//! Bakes the build commit and time into the binary for `GET /version`.

use std::{env, path::Path, process::Command};

fn main() {
    for name in [
        "GIT_COMMIT",
        "GITHUB_SHA",
        "BUILD_TIME",
        "SOURCE_DATE_EPOCH",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    println!("cargo:rustc-env=PAYKIT_BUILD_COMMIT={}", commit());
    println!("cargo:rustc-env=PAYKIT_BUILT_AT={}", built_at());
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn commit() -> String {
    if let Some(commit) = non_empty_env("GIT_COMMIT").or_else(|| non_empty_env("GITHUB_SHA")) {
        return commit;
    }
    let Some(commit) = git(&["rev-parse", "HEAD"]) else {
        return "unknown".to_owned();
    };
    // Rebuild when HEAD moves, so a local build never reports a stale commit.
    // `--git-path` resolves shared refs to the common directory in linked
    // worktrees; a missing path would make Cargo rerun this script every build.
    let paths = git(&[
        "rev-parse",
        "--path-format=absolute",
        "--git-path",
        "HEAD",
        "--git-path",
        "refs",
        "--git-path",
        "packed-refs",
    ]);
    for path in paths.iter().flat_map(|paths| paths.lines()) {
        if Path::new(path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    commit
}

fn built_at() -> String {
    if let Some(time) = non_empty_env("BUILD_TIME") {
        return time;
    }
    non_empty_env("SOURCE_DATE_EPOCH")
        .and_then(|epoch| epoch.parse::<i64>().ok())
        .map(rfc3339_utc)
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Formats Unix seconds as RFC 3339 UTC (days-to-civil, Howard Hinnant).
fn rfc3339_utc(epoch: i64) -> String {
    let (days, secs) = (epoch.div_euclid(86_400), epoch.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs / 3_600,
        secs % 3_600 / 60,
        secs % 60
    )
}
