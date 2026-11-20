//! Request-scoped diagnostics with bounded safe correlation IDs.

use std::{cell::Cell, future::Future, time::Instant};

use uuid::Uuid;

struct RequestContext {
    request_id: Uuid,
    request_instance_id: Uuid,
    operation: &'static str,
    failure_emitted: Cell<bool>,
    setup_trace_link: Cell<Option<Uuid>>,
}

tokio::task_local! {
    static REQUEST_CONTEXT: RequestContext;
}

pub(crate) async fn scope<T>(
    request_id: Uuid,
    request_instance_id: Uuid,
    operation: &'static str,
    future: impl Future<Output = T>,
) -> (T, bool) {
    REQUEST_CONTEXT
        .scope(
            RequestContext {
                request_id,
                request_instance_id,
                operation,
                failure_emitted: Cell::new(false),
                setup_trace_link: Cell::new(None),
            },
            async {
                let output = future.await;
                let failure_emitted = REQUEST_CONTEXT.with(|context| context.failure_emitted.get());
                (output, failure_emitted)
            },
        )
        .await
}

/// Links one server-owned HTTP invocation to a safe setup workflow ID. The
/// bearer flow token is deliberately never recorded.
pub(crate) fn link_setup_trace(setup_trace_id: Uuid) {
    let _ = REQUEST_CONTEXT.try_with(|context| {
        if context.setup_trace_link.get() == Some(setup_trace_id) {
            return;
        }
        context.setup_trace_link.set(Some(setup_trace_id));
        tracing::info!(
            event = "paykit_setup_request_link",
            operation = context.operation,
            request_id = %context.request_id.hyphenated(),
            request_instance_id = %context.request_instance_id.hyphenated(),
            setup_trace_id = %setup_trace_id.hyphenated(),
            "Paykit setup request linked to workflow"
        );
    });
}

/// Emits at most one failure event for one request. Call at the narrowest point
/// where a bounded, secret-free cause is known; outer mappings may call again as
/// a fallback without producing duplicate warnings.
pub(crate) fn failure(_operation: &'static str, stage: &'static str, category: &'static str) {
    let _ = REQUEST_CONTEXT.try_with(|context| {
        if context.failure_emitted.replace(true) {
            return;
        }
        tracing::warn!(
            event = "paykit_request_failure",
            operation = context.operation,
            stage,
            category,
            request_id = %context.request_id.hyphenated(),
            request_instance_id = %context.request_instance_id.hyphenated(),
            "Paykit request failed"
        );
    });
}

/// Emits validated server-owned IDs after a successful atomic commit or exact replay lookup.
/// Request IDs remain transport correlation; these IDs identify durable domain work.
pub(crate) fn invoice_persisted(invoice_id: Uuid, outbox_job_id: Uuid, replayed: bool) {
    let _ = REQUEST_CONTEXT.try_with(|context| {
        tracing::info!(
            event = "paykit_invoice_persisted",
            operation = context.operation,
            stage = if replayed { "exact_replay" } else { "db_commit" },
            outcome = if replayed { "replayed" } else { "created" },
            truth = "persisted",
            request_id = %context.request_id.hyphenated(),
            request_instance_id = %context.request_instance_id.hyphenated(),
            invoice_id = %invoice_id.hyphenated(),
            outbox_job_id = %outbox_job_id.hyphenated(),
            "Paykit invoice persisted"
        );
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimingOutcome {
    Completed,
    Deferred,
    RoutineFailure,
    Failed,
}

impl TimingOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Deferred => "deferred",
            Self::RoutineFailure => "failed",
            Self::Failed => "failed",
        }
    }
}

pub(crate) fn timing(
    operation: &'static str,
    stage: &'static str,
    outcome: TimingOutcome,
    started: Instant,
) {
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let routine_failure = outcome == TimingOutcome::RoutineFailure;
    let outcome = outcome.label();
    if outcome == "failed" {
        if routine_failure {
            tracing::debug!(
                event = "paykit_task_timing",
                operation,
                stage,
                outcome,
                elapsed_ms,
                "Paykit task timing"
            );
        } else {
            tracing::warn!(
                event = "paykit_task_timing",
                operation,
                stage,
                outcome,
                elapsed_ms,
                "Paykit task timing"
            );
        }
    } else {
        tracing::info!(
            event = "paykit_task_timing",
            operation,
            stage,
            outcome,
            elapsed_ms,
            "Paykit task timing"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    type CapturedFields = Vec<(String, String)>;

    struct CapturedEvent {
        level: Level,
        fields: CapturedFields,
    }

    #[derive(Clone, Default)]
    struct EventCapture(Arc<Mutex<Vec<CapturedEvent>>>);

    impl<S: Subscriber> Layer<S> for EventCapture {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut fields = Vec::new();
            event.record(&mut FieldVisitor(&mut fields));
            self.0.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                fields,
            });
        }
    }

    struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
            self.0.push((field.name().to_owned(), format!("{value:?}")));
        }
    }

    fn field<'a>(event: &'a [(String, String)], name: &str) -> Option<&'a str> {
        event
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.trim_matches('"'))
    }

    #[test]
    fn task_timing_events_use_only_coarse_bounded_fields() {
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            timing(
                "invoice_create",
                "invoice_preflight",
                TimingOutcome::Completed,
                std::time::Instant::now(),
            );
            timing(
                "worker",
                "outbox_retry",
                TimingOutcome::Deferred,
                std::time::Instant::now(),
            );
            timing(
                "worker",
                "outbox_enqueue",
                TimingOutcome::Failed,
                std::time::Instant::now(),
            );
        });

        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].level, Level::INFO);
        assert_eq!(
            field(&events[0].fields, "event"),
            Some("paykit_task_timing")
        );
        assert_eq!(
            field(&events[0].fields, "operation"),
            Some("invoice_create")
        );
        assert_eq!(field(&events[0].fields, "stage"), Some("invoice_preflight"));
        assert_eq!(field(&events[0].fields, "outcome"), Some("completed"));
        assert!(
            field(&events[0].fields, "elapsed_ms")
                .unwrap()
                .parse::<u64>()
                .is_ok()
        );
        assert_eq!(events[1].level, Level::INFO);
        assert_eq!(field(&events[1].fields, "outcome"), Some("deferred"));
        assert_eq!(events[2].level, Level::WARN);
        assert_eq!(field(&events[2].fields, "outcome"), Some("failed"));
        for event in events.iter() {
            assert!(event.fields.iter().all(|(name, _)| {
                matches!(
                    name.as_str(),
                    "message" | "event" | "operation" | "stage" | "outcome" | "elapsed_ms"
                )
            }));
        }
    }

    #[test]
    fn expected_retry_worker_timings_are_debug_but_other_failures_remain_warn() {
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());

        tracing::subscriber::with_default(subscriber, || {
            for stage in ["shared_transport", "observer"] {
                timing(
                    "worker",
                    stage,
                    TimingOutcome::RoutineFailure,
                    std::time::Instant::now(),
                );
            }
            timing(
                "worker",
                "outbox_enqueue",
                TimingOutcome::Failed,
                std::time::Instant::now(),
            );
            timing(
                "invoice_create",
                "observer",
                TimingOutcome::Failed,
                std::time::Instant::now(),
            );
        });

        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].level, Level::DEBUG);
        assert_eq!(events[1].level, Level::DEBUG);
        assert_eq!(events[2].level, Level::WARN);
        assert_eq!(events[3].level, Level::WARN);
    }
}
