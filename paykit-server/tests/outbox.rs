use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use paykit_lib::{
    PaykitReceiverCapabilities, PaykitReceiverMarker, PaykitReceiverPath, PaymentAmount,
    PaymentEndpointIdentifier, PaymentEndpointPayload, PaymentReference, PaymentRequestTerms,
    PublicKey,
};
use paykit_sdk::OutboundPrivateMessageStatus;
use paykit_server::{
    application::semantic_intent::{DeliveryIntentV1, PaymentTermsV1, ReceivingDetailV1},
    workers::outbox::{
        Adapter, HandoffError, HandoffFailure, HandoffResult, RetryableHandoffCause,
        RetryableHandoffStage, handoff,
    },
};

fn marker(path: &str) -> PaykitReceiverMarker {
    PaykitReceiverMarker::new(
        PaykitReceiverPath::new(path).unwrap(),
        PaykitReceiverCapabilities {
            private_payments: true,
            payment_requests: true,
            receipts: false,
            outgoing_payments: true,
        },
        PublicKey::try_from_z32("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
    )
}

struct FakeAdapter {
    marker: PaykitReceiverMarker,
    link_error: Option<HandoffError>,
    payment_request_calls: Mutex<usize>,
    allowance_error: Option<HandoffError>,
    calls: Mutex<Vec<&'static str>>,
}

impl FakeAdapter {
    fn new(marker: PaykitReceiverMarker) -> Self {
        Self {
            marker,
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
    async fn fetch_marker(
        &self,
        _reader: &str,
        _path: &str,
    ) -> Result<Option<PaykitReceiverMarker>, HandoffError> {
        self.record("fetch_marker");
        Ok(Some(self.marker.clone()))
    }

    async fn ensure_link_with_peer(&self, _reader: &str, _path: &str) -> Result<(), HandoffError> {
        self.record("ensure_link_with_peer");
        self.link_error.map_or(Ok(()), Err)
    }

    async fn accept_allowance_proposals(
        &self,
        _reader: &str,
        path: &str,
    ) -> Result<usize, HandoffError> {
        self.record("accept_allowance_proposals");
        assert_eq!(path, self.marker.receiver_path.as_str());
        self.allowance_error.map_or(Ok(1), Err)
    }

    async fn enqueue_private_payment_list_with_receiving_details(
        &self,
        _reader: &str,
        _path: &str,
        _details: &[ReceivingDetailV1],
    ) -> Result<HandoffResult, HandoffError> {
        self.record("enqueue_private_payment_list");
        Ok(HandoffResult::EndpointPublication {
            outbound_message_id: 41,
        })
    }

    async fn propose_payment_request(
        &self,
        _reader: &str,
        _path: &str,
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

    async fn outbound_status(
        &self,
        _outbound_message_id: u64,
    ) -> Result<Option<OutboundPrivateMessageStatus>, HandoffError> {
        Ok(Some(OutboundPrivateMessageStatus::Sent))
    }
}

fn payment_intent(marker: &PaykitReceiverMarker) -> DeliveryIntentV1 {
    DeliveryIntentV1::payment_request(
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy".into(),
        marker,
        PaykitReceiverPath::new("paykit/server").unwrap(),
        &PaymentRequestTerms::builder(
            PaymentAmount::new("0.00050000", "btc").unwrap(),
            PaymentReference::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            vec![PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap()],
        )
        .build()
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn changed_selected_marker_is_retryable_without_a_reselection() {
    let selected = marker("bitkit/wallet");
    let changed = marker("other/wallet");
    let adapter = FakeAdapter::new(changed);

    assert_eq!(
        handoff(&adapter, &payment_intent(&selected)).await,
        Err(HandoffFailure::Retryable(
            RetryableHandoffStage::MarkerChanged
        ))
    );
    assert_eq!(*adapter.payment_request_calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn link_failure_has_one_durable_diagnostic_stage() {
    let selected = marker("bitkit/wallet");
    let adapter = FakeAdapter {
        link_error: Some(HandoffError::Retryable(RetryableHandoffCause::Transport)),
        ..FakeAdapter::new(selected.clone())
    };

    assert_eq!(
        handoff(&adapter, &payment_intent(&selected)).await,
        Err(HandoffFailure::Retryable(
            RetryableHandoffStage::LinkEstablishment
        ))
    );
    assert_eq!(adapter.calls(), ["fetch_marker", "ensure_link_with_peer"]);
}

fn endpoint_intent(marker: &PaykitReceiverMarker) -> DeliveryIntentV1 {
    DeliveryIntentV1::endpoint(
        "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy".into(),
        marker,
        PaykitReceiverPath::new("paykit/server").unwrap(),
        vec![(
            PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            PaymentEndpointPayload::new(r#"{"value":"address"}"#),
        )],
    )
    .unwrap()
}

#[tokio::test]
async fn allowance_intake_runs_on_the_linked_path_before_each_delivery() {
    let selected = marker("bitkit/wallet");
    for (intent, delivery) in [
        (payment_intent(&selected), "propose_payment_request"),
        (endpoint_intent(&selected), "enqueue_private_payment_list"),
    ] {
        let adapter = FakeAdapter::new(selected.clone());
        handoff(&adapter, &intent).await.unwrap();
        assert_eq!(
            adapter.calls(),
            [
                "fetch_marker",
                "ensure_link_with_peer",
                "accept_allowance_proposals",
                delivery,
            ]
        );
    }
}

#[tokio::test]
async fn failed_allowance_intake_still_proposes_the_request() {
    let selected = marker("bitkit/wallet");
    for error in [
        HandoffError::Retryable(RetryableHandoffCause::Transport),
        HandoffError::Retryable(RetryableHandoffCause::RecoveryRequired),
        HandoffError::Permanent,
    ] {
        let adapter = FakeAdapter {
            allowance_error: Some(error),
            ..FakeAdapter::new(selected.clone())
        };
        assert!(matches!(
            handoff(&adapter, &payment_intent(&selected)).await,
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
    let selected = marker("bitkit/wallet");
    let adapter = Arc::new(FakeAdapter::new(selected.clone()));
    let intent = payment_intent(&selected);

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
