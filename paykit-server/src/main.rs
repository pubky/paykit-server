use std::{env, fs};

use paykit_server::{
    Server,
    config::{Config, ConfigEnvironment},
    startup::initialize_database,
};
use tracing_subscriber::EnvFilter;

const AUTH_RELAY_LOG_TARGET: &str = "pubky::actors::auth::relay::auth_relay_listener";
const HTTP_RELAY_CHANNEL_LOG_TARGET: &str = "pubky::actors::auth::relay::http_relay_inbox_channel";
const HTTP_RELAY_LINK_LOG_TARGET: &str = "pubky::actors::auth::relay::http_relay_link_channel";

fn production_log_filter() -> EnvFilter {
    EnvFilter::new(format!(
        "info,{AUTH_RELAY_LOG_TARGET}=off,{HTTP_RELAY_CHANNEL_LOG_TARGET}=off,{HTTP_RELAY_LINK_LOG_TARGET}=off"
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(production_log_filter())
        .with_current_span(false)
        .with_span_list(false)
        .init();
    let check_config = match env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => false,
        [argument] if argument == "--check-config" => true,
        _ => return Err(anyhow::anyhow!("usage: paykit-server [--check-config]")),
    };
    let config_path =
        env::var("PAYKIT_CONFIG").map_err(|_| anyhow::anyhow!("PAYKIT_CONFIG is required"))?;
    let source = fs::read_to_string(config_path)
        .map_err(|_| anyhow::anyhow!("configuration could not be read"))?;
    let config = Config::from_toml_and_environment(
        &source,
        ConfigEnvironment {
            database_url: env::var("PAYKIT_DATABASE_URL").ok(),
            master_key: env::var("PAYKIT_MASTER_KEY").ok(),
        },
    )?;
    if check_config {
        println!("configuration valid");
        return Ok(());
    }
    let pool = initialize_database(&config).await?;
    let listen_addr = config.http.listen_addr.clone();
    let server = Server::build(config, pool).await?;
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    server.run(listener).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::{Event, Subscriber};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    use super::*;

    #[derive(Clone, Default)]
    struct CountEvents(Arc<Mutex<usize>>);

    impl<S> Layer<S> for CountEvents
    where
        S: Subscriber,
    {
        fn on_event(&self, _: &Event<'_>, _: Context<'_, S>) {
            *self.0.lock().unwrap() += 1;
        }
    }

    #[test]
    fn production_filter_suppresses_all_url_bearing_auth_relay_events() {
        let capture = CountEvents::default();
        let subscriber = tracing_subscriber::registry()
            .with(production_log_filter())
            .with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                target: "pubky::actors::auth::relay::auth_relay_listener",
                "URL-bearing dependency event"
            );
            tracing::error!(
                target: "pubky::actors::auth::relay::auth_relay_listener",
                "URL-bearing dependency error"
            );
            tracing::error!(
                target: "pubky::actors::auth::relay::http_relay_inbox_channel",
                "URL-bearing dependency error"
            );
            tracing::error!(
                target: "pubky::actors::auth::relay::http_relay_link_channel",
                "URL-bearing dependency error"
            );
            tracing::warn!(
                target: "pubky::actors::auth::relay::auth_relay_listener",
                "coarse dependency warning"
            );
            tracing::info!(target: "paykit_server", "application event");
        });
        assert_eq!(*capture.0.lock().unwrap(), 1);
    }
}
