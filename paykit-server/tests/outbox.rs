use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use paykit_lib::{
    PaykitApp, PaykitAppCapabilities, PaykitAppId, PaykitAppRegistry, PaymentAmount,
    PaymentEndpointIdentifier, PaymentEndpointPayload, PaymentReference, PaymentRequestTerms,
    PublicKey,
};
use paykit_sdk::OutboundPrivateMessageStatus;
use paykit_server::{
    application::semantic_intent::{DeliveryIntentV1, PaymentTermsV1},
    workers::outbox::{
        Adapter, HandoffError, HandoffFailure, HandoffResult, RetryableHandoffCause,
        RetryableHandoffStage, handoff,
    },
};

fn registry(capable: bool) -> PaykitAppRegistry {
    let mut registry = PaykitAppRegistry::new(Some(
        PublicKey::try_from_z32("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
    ));
    registry
        .register_app(
            PaykitAppId::new("reader").unwrap(),
            PaykitApp::new(
                "Reader",
                PaykitAppCapabilities {
                    private_payments: true,
                    payment_requests: true,
                    receipts: false,
                    outgoing_payments: capable,
                },
            )
            .unwrap(),
        )
        .unwrap();
    registry
}

struct FakeAdapter {
    registry: PaykitAppRegistry,
    recovery_marker_error: Option<HandoffError>,
    link_error: Option<HandoffError>,
    payment_request_calls: Mutex<usize>,
    allowance_error: Option<HandoffError>,
    calls: Mutex<Vec<&'static str>>,
}

impl FakeAdapter {
    fn new(registry: PaykitAppRegistry) -> Self {
        Self {
            registry,
            recovery_marker_error: None,
            link_error: None,
            payment_request_calls: Mutex::new(0),
            allowance_error: None,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, call: &'static str) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Adapter for FakeAdapter {
    async fn fetch_registry(
        &self,
        _reader: &str,
    ) -> Result<Option<PaykitAppRegistry>, HandoffError> {
        self.record("fetch_registry");
        Ok(Some(self.registry.clone()))
    }

    async fn observe_recovery_marker(&self, _reader: &str) -> Result<(), HandoffError> {
        self.record("observe_recovery_marker");
        self.recovery_marker_error.map_or(Ok(()), Err)
    }

    async fn ensure_link_with_peer(&self, _reader: &str) -> Result<(), HandoffError> {
        self.record("ensure_link_with_peer");
        self.link_error.map_or(Ok(()), Err)
    }

    async fn accept_allowance_proposals(&self, _reader: &str) -> Result<usize, HandoffError> {
        self.record("accept_allowance_proposals");
        self.allowance_error.map_or(Ok(1), Err)
    }

    async fn propose_payment_request(
        &self,
        _reader: &str,
        _terms: &PaymentTermsV1,
    ) -> Result<HandoffResult, HandoffError> {
        self.record("propose_payment_request");
        *self.payment_request_calls.lock().unwrap() += 1;
        Ok(HandoffResult::PaymentRequestProposal {
            outbound_message_id: 42,
            event_id: "event-42".into(),
            payment_request_id: "request-42".into(),
        })
    }

    async fn cancel_payment_request(
        &self,
        _reader: &str,
        _payment_request_id: &str,
    ) -> Result<HandoffResult, HandoffError> {
        Err(HandoffError::Permanent)
    }

    async fn outbound_status(
        &self,
        _outbound_message_id: u64,
    ) -> Result<Option<OutboundPrivateMessageStatus>, HandoffError> {
        Ok(Some(OutboundPrivateMessageStatus::Sent))
    }
}

fn payment_intent() -> DeliveryIntentV1 {
    DeliveryIntentV1::payment_request(
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy".into(),
        PaykitAppId::new("paykit-server").unwrap(),
        &PaymentRequestTerms::builder(
            PaymentAmount::new("0.00050000", "btc").unwrap(),
            PaymentReference::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            vec![PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap()],
        )
        .required_app_id(Some(PaykitAppId::new("paykit-server").unwrap()))
        .payment_endpoints(Some(std::collections::HashMap::from([(
            PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            PaymentEndpointPayload::new("private-address"),
        )])))
        .build()
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn incapable_registry_is_retryable_without_handoff() {
    let changed = registry(false);
    let adapter = FakeAdapter::new(changed);

    assert_eq!(
        handoff(&adapter, &payment_intent()).await,
        Err(HandoffFailure::Retryable(
            RetryableHandoffStage::RegistryIncapable
        ))
    );
    assert_eq!(*adapter.payment_request_calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn link_failure_has_one_durable_diagnostic_stage() {
    let selected = registry(true);
    let adapter = FakeAdapter {
        link_error: Some(HandoffError::Retryable(RetryableHandoffCause::Transport)),
        ..FakeAdapter::new(selected.clone())
    };

    assert_eq!(
        handoff(&adapter, &payment_intent()).await,
        Err(HandoffFailure::Retryable(
            RetryableHandoffStage::LinkEstablishment
        ))
    );
    assert_eq!(
        adapter.calls(),
        [
            "fetch_registry",
            "observe_recovery_marker",
            "ensure_link_with_peer"
        ]
    );
}

#[tokio::test]
async fn allowance_intake_runs_on_the_link_before_the_request() {
    let adapter = FakeAdapter::new(registry(true));
    handoff(&adapter, &payment_intent()).await.unwrap();
    assert_eq!(
        adapter.calls(),
        [
            "fetch_registry",
            "observe_recovery_marker",
            "ensure_link_with_peer",
            "accept_allowance_proposals",
            "propose_payment_request",
        ]
    );
}

#[tokio::test]
async fn failed_allowance_intake_still_proposes_the_request() {
    for error in [
        HandoffError::Retryable(RetryableHandoffCause::Transport),
        HandoffError::Retryable(RetryableHandoffCause::RecoveryRequired),
        HandoffError::Permanent,
    ] {
        let adapter = FakeAdapter {
            allowance_error: Some(error),
            ..FakeAdapter::new(registry(true))
        };
        assert!(matches!(
            handoff(&adapter, &payment_intent()).await,
            Ok(HandoffResult::PaymentRequestProposal {
                outbound_message_id: 42,
                ..
            })
        ));
        assert_eq!(*adapter.payment_request_calls.lock().unwrap(), 1);
    }
}

#[tokio::test]
async fn retry_after_an_ambiguous_handoff_can_propose_twice() {
    let selected = registry(true);
    let adapter = Arc::new(FakeAdapter::new(selected.clone()));
    let intent = payment_intent();

    // A database worker may be reclaimed after the public SDK queued the first
    // proposal but before its fenced state transition; repeating the public API
    // is deliberate at-least-once behavior.
    let first = handoff(adapter.as_ref(), &intent).await.unwrap();
    let second = handoff(adapter.as_ref(), &intent).await.unwrap();
    assert!(matches!(
        (&first, &second),
        (
            HandoffResult::PaymentRequestProposal { outbound_message_id: 42, event_id, payment_request_id },
            HandoffResult::PaymentRequestProposal { outbound_message_id: 42, event_id: second_event, payment_request_id: second_request }
        ) if event_id == "event-42" && payment_request_id == "request-42" && second_event == event_id && second_request == payment_request_id
    ));
    assert_eq!(*adapter.payment_request_calls.lock().unwrap(), 2);
}

#[tokio::test]
async fn recovery_marker_observation_precedes_link_ensure_and_enqueue() {
    let selected = registry(true);
    let adapter = FakeAdapter {
        recovery_marker_error: None,
        ..FakeAdapter::new(selected)
    };

    handoff(&adapter, &payment_intent()).await.unwrap();

    assert_eq!(
        *adapter.calls.lock().unwrap(),
        [
            "fetch_registry",
            "observe_recovery_marker",
            "ensure_link_with_peer",
            "accept_allowance_proposals",
            "propose_payment_request",
        ]
    );
}

#[tokio::test]
async fn recovery_marker_lookup_failure_never_ensures_or_enqueues() {
    let selected = registry(true);
    let adapter = FakeAdapter {
        recovery_marker_error: Some(HandoffError::Retryable(RetryableHandoffCause::Transport)),
        ..FakeAdapter::new(selected)
    };

    assert_eq!(
        handoff(&adapter, &payment_intent()).await,
        Err(HandoffFailure::Retryable(
            RetryableHandoffStage::RecoveryMarkerObservation
        ))
    );
    assert_eq!(
        *adapter.calls.lock().unwrap(),
        ["fetch_registry", "observe_recovery_marker"]
    );
    assert_eq!(*adapter.payment_request_calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn confirmed_absent_recovery_marker_continues_to_link_ensure() {
    let selected = registry(true);
    let adapter = FakeAdapter {
        recovery_marker_error: None,
        ..FakeAdapter::new(selected)
    };

    handoff(&adapter, &payment_intent()).await.unwrap();

    assert_eq!(*adapter.payment_request_calls.lock().unwrap(), 1);
    assert!(
        adapter
            .calls
            .lock()
            .unwrap()
            .windows(2)
            .any(|calls| calls == ["observe_recovery_marker", "ensure_link_with_peer"])
    );
}
