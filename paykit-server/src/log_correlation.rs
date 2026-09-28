//! Closed, non-secret log correlation fields.
//!
//! Pubky identity refs strip the shared canonical `pubky` prefix, then expose
//! only the first and last four ASCII payload characters (`abcd...wxyz`).
//! Durable outbox UUIDs are safe opaque `operation_ref` values; never substitute
//! sequential database IDs or payment, bundle, address, path, proof, or key data.

use uuid::Uuid;

const PUBKY_PREFIX: &str = "pubky";
const PUBKY_PAYLOAD_LEN: usize = 52;
const Z32_ALPHABET: &[u8] = b"ybndrfg8ejkmcpqxot1uwisza345h769";
const INVALID_PUBKY_REF: &str = "invalid_identity";

pub(crate) fn pubky_ref(canonical: &str) -> String {
    let Some(payload) = canonical.strip_prefix(PUBKY_PREFIX) else {
        return INVALID_PUBKY_REF.into();
    };
    if payload.len() != PUBKY_PAYLOAD_LEN
        || !payload
            .bytes()
            .all(|character| Z32_ALPHABET.contains(&character))
    {
        return INVALID_PUBKY_REF.into();
    }

    format!("{}...{}", &payload[..4], &payload[payload.len() - 4..])
}

pub(crate) fn operation_ref(operation_id: Uuid) -> String {
    operation_id.hyphenated().to_string()
}

pub(crate) fn emit_handoff_failure(
    stage: &str,
    cause: &str,
    creator: Option<&str>,
    reader: &str,
    operation_id: Uuid,
) {
    let reader_ref = pubky_ref(reader);
    let operation_ref = operation_ref(operation_id);
    if let Some(creator) = creator {
        let creator_ref = pubky_ref(creator);
        tracing::warn!(
            stage,
            cause,
            creator_ref,
            reader_ref,
            operation_ref,
            "Paykit handoff failed"
        );
    } else {
        tracing::warn!(
            stage,
            cause,
            reader_ref,
            operation_ref,
            "Paykit handoff failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::{Event, Subscriber};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    use super::*;

    const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
    const READER: &str = "pubky7ir1ttte48bcp4zjychjyscicrwi1j34mtt91ptsafdbjmr8g9eo";

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

    fn field<'a>(event: &'a [(String, String)], name: &str) -> &'a str {
        event
            .iter()
            .find_map(|(field, value)| (field == name).then_some(value.as_str()))
            .unwrap_or_else(|| panic!("missing {name}"))
    }

    #[test]
    fn pubky_refs_strip_shared_prefix_and_fail_closed_on_invalid_input() {
        crate::domain::locks::parse_creator(CREATOR).unwrap();
        crate::domain::locks::parse_reader(READER).unwrap();
        assert_eq!(pubky_ref(CREATOR), "tkrq...p7qy");
        assert_eq!(pubky_ref(READER), "7ir1...g9eo");
        assert_ne!(pubky_ref(CREATOR), pubky_ref(READER));
        for invalid in [
            "",
            "pubky",
            "pubkyshort",
            "tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
            "pubky!!!!8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy",
        ] {
            assert_eq!(pubky_ref(invalid), INVALID_PUBKY_REF);
            if !invalid.is_empty() {
                assert!(!pubky_ref(invalid).contains(invalid));
            }
        }
    }

    #[test]
    fn handoff_events_have_stable_distinct_safe_operation_and_identity_refs() {
        let operation = Uuid::parse_str("123e4567-e89b-42d3-a456-426614174000").unwrap();
        let other_operation = Uuid::parse_str("223e4567-e89b-42d3-a456-426614174001").unwrap();
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            emit_handoff_failure(
                "link_establishment",
                "link_pending",
                Some(CREATOR),
                READER,
                operation,
            );
            emit_handoff_failure(
                "link_establishment",
                "link_pending",
                Some(CREATOR),
                READER,
                operation,
            );
            emit_handoff_failure(
                "link_establishment",
                "link_pending",
                Some(CREATOR),
                READER,
                other_operation,
            );
        });

        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 3);
        for event in events.iter() {
            assert_eq!(field(event, "stage"), "\"link_establishment\"");
            assert_eq!(field(event, "cause"), "\"link_pending\"");
            assert_eq!(field(event, "creator_ref"), "\"tkrq...p7qy\"");
            assert_eq!(field(event, "reader_ref"), "\"7ir1...g9eo\"");
            let rendered = event
                .iter()
                .map(|(_, value)| value.as_str())
                .collect::<String>();
            assert!(!rendered.contains(CREATOR));
            assert!(!rendered.contains(READER));
            for forbidden in [
                "bundle",
                "payment_request",
                "xpub",
                "address",
                "receiver_path",
                "signature",
                "proof",
                "secret",
            ] {
                assert!(!rendered.contains(forbidden));
            }
        }
        assert_eq!(
            field(&events[0], "operation_ref"),
            field(&events[1], "operation_ref")
        );
        assert_eq!(
            field(&events[0], "operation_ref"),
            "\"123e4567-e89b-42d3-a456-426614174000\""
        );
        assert_ne!(
            field(&events[0], "operation_ref"),
            field(&events[2], "operation_ref")
        );
    }
}
