use paykit_lib::{
    PaykitReceiverCapabilities, PaykitReceiverMarker, PaykitReceiverPath, PaymentAmount,
    PaymentEndpointIdentifier, PaymentEndpointPayload, PaymentReference, PaymentRequestTerms,
    PublicKey,
};
use paykit_server::{application::semantic_intent::DeliveryIntentV1, domain::locks::ReaderPubky};

fn marker() -> PaykitReceiverMarker {
    PaykitReceiverMarker::new(
        PaykitReceiverPath::new("bitkit/wallet").unwrap(),
        PaykitReceiverCapabilities {
            private_payments: true,
            payment_requests: true,
            receipts: false,
            outgoing_payments: false,
        },
        PublicKey::try_from_z32("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy").unwrap(),
    )
}

pub fn endpoint_intent(reader: &ReaderPubky, address: String) -> DeliveryIntentV1 {
    DeliveryIntentV1::endpoint(
        reader.to_string(),
        &marker(),
        PaykitReceiverPath::new("paykit/server").unwrap(),
        vec![(
            PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            PaymentEndpointPayload::new(address),
        )],
    )
    .unwrap()
}

pub fn payment_intent(reader: &ReaderPubky) -> DeliveryIntentV1 {
    DeliveryIntentV1::payment_request(
        reader.to_string(),
        &marker(),
        PaykitReceiverPath::new("paykit/server").unwrap(),
        &PaymentRequestTerms {
            amount: PaymentAmount::new("0.00000100", "btc").unwrap(),
            payment_reference: PaymentReference::new(uuid::Uuid::new_v4().to_string()).unwrap(),
            proposal_expires_at: None,
            recurrence: None,
            accepted_payment_endpoint_identifiers: vec![
                PaymentEndpointIdentifier::new("btc-bitcoin-p2wpkh").unwrap(),
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
}
