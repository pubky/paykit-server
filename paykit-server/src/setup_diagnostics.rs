use crate::bitkit_claim::ClaimError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetupStage {
    AuthComplete,
    IdentityValidate,
    SessionExport,
    RelayReceive,
    ClaimVerify,
    XpubValidate,
    LockAcquire,
    CreatorLoad,
    MarkerPublish,
    MarkerReadback,
    Persistence,
    Compensation,
    LockRelease,
    RelayAck,
}

impl SetupStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AuthComplete => "auth_complete",
            Self::IdentityValidate => "identity_validate",
            Self::SessionExport => "session_export",
            Self::RelayReceive => "relay_receive",
            Self::ClaimVerify => "claim_verify",
            Self::XpubValidate => "xpub_validate",
            Self::LockAcquire => "lock_acquire",
            Self::CreatorLoad => "creator_load",
            Self::MarkerPublish => "marker_publish",
            Self::MarkerReadback => "marker_readback",
            Self::Persistence => "persistence",
            Self::Compensation => "compensation",
            Self::LockRelease => "lock_release",
            Self::RelayAck => "relay_ack",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetupOutcome {
    Started,
    Succeeded,
    Failed,
    Absent,
}

impl SetupOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Absent => "absent",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetupFailureClass {
    None,
    Auth,
    InvalidIdentity,
    SessionExport,
    Transport,
    BodyAbsent,
    InvalidRequest,
    InvalidPayload,
    InvalidEnvelope,
    Authentication,
    Storage,
    ReadbackMismatch,
}

impl SetupFailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Auth => "auth",
            Self::InvalidIdentity => "invalid_identity",
            Self::SessionExport => "session_export",
            Self::Transport => "transport",
            Self::BodyAbsent => "body_absent",
            Self::InvalidRequest => "invalid_request",
            Self::InvalidPayload => "invalid_payload",
            Self::InvalidEnvelope => "invalid_envelope",
            Self::Authentication => "authentication",
            Self::Storage => "storage",
            Self::ReadbackMismatch => "readback_mismatch",
        }
    }
}

pub(crate) fn claim_failure_class(error: &ClaimError) -> SetupFailureClass {
    match error {
        ClaimError::InvalidAuthRequest => SetupFailureClass::InvalidRequest,
        ClaimError::InvalidPayload => SetupFailureClass::InvalidPayload,
        ClaimError::InvalidEnvelope => SetupFailureClass::InvalidEnvelope,
        ClaimError::AuthenticationFailed => SetupFailureClass::Authentication,
    }
}

pub(crate) fn emit_setup_stage(stage: SetupStage, outcome: SetupOutcome, class: SetupFailureClass) {
    let stage = stage.as_str();
    let outcome = outcome.as_str();
    let class = class.as_str();
    match outcome {
        "failed" | "absent" => tracing::warn!(
            event = "paykit_setup_completion",
            stage,
            outcome,
            class,
            "Paykit setup completion stage"
        ),
        _ => tracing::info!(
            event = "paykit_setup_completion",
            stage,
            outcome,
            class,
            "Paykit setup completion stage"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::{Event, Subscriber};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    use super::*;

    type CapturedEvents = Vec<Vec<(String, String)>>;

    #[derive(Clone, Default)]
    struct EventCapture(Arc<Mutex<CapturedEvents>>);

    impl<S> Layer<S> for EventCapture
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut fields = Vec::new();
            event.record(&mut FieldVisitor(&mut fields));
            self.0.lock().unwrap().push(fields);
        }
    }

    struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

    impl tracing::field::Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn core::fmt::Debug) {
            self.0.push((field.name().to_owned(), format!("{value:?}")));
        }
    }

    #[test]
    fn emits_only_closed_secret_free_fields() {
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            emit_setup_stage(
                SetupStage::RelayReceive,
                SetupOutcome::Absent,
                SetupFailureClass::BodyAbsent,
            );
        });

        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 1);
        let fields = &events[0];
        for name in ["event", "stage", "outcome", "class", "message"] {
            assert!(fields.iter().any(|(field, _)| field == name));
        }
        for forbidden in [
            "flow_id",
            "creator",
            "authorization_url",
            "relay",
            "secret",
            "session",
            "xpub",
            "payload",
            "error",
        ] {
            assert!(fields.iter().all(|(name, _)| name != forbidden));
        }
    }

    #[test]
    fn vocabulary_is_stable() {
        assert_eq!(SetupStage::AuthComplete.as_str(), "auth_complete");
        assert_eq!(SetupStage::ClaimVerify.as_str(), "claim_verify");
        assert_eq!(SetupStage::XpubValidate.as_str(), "xpub_validate");
        assert_eq!(SetupStage::MarkerPublish.as_str(), "marker_publish");
        assert_eq!(SetupStage::MarkerReadback.as_str(), "marker_readback");
        assert_eq!(SetupStage::Persistence.as_str(), "persistence");
        assert_eq!(SetupStage::LockRelease.as_str(), "lock_release");
        assert_eq!(SetupStage::RelayAck.as_str(), "relay_ack");
    }
}
