//! Deterministic selection of a reader's publicly discoverable Paykit receiver.
//!
//! Discovery deliberately returns every public candidate.  A marker is usable
//! only when it advertises both private payments and Payment Requests; selection
//! then applies the operator's first-segment priority and canonical full-path
//! lexical tie break.

use paykit_lib::{PaykitReceiverMarker, PaykitReceiverPath};

use crate::config::ReceiverPathPriority;

/// A validated public marker selected for one invoice intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedReaderMarker {
    pub receiver_path: PaykitReceiverPath,
    pub marker: PaykitReceiverMarker,
}

/// Select a capable marker without depending on listing order.
pub fn select_reader_marker(
    candidates: impl IntoIterator<Item = PaykitReceiverMarker>,
    priority: &[ReceiverPathPriority],
) -> Option<SelectedReaderMarker> {
    let mut capable = candidates
        .into_iter()
        .filter(|marker| {
            marker.capabilities.private_payments && marker.capabilities.payment_requests
        })
        .collect::<Vec<_>>();
    capable.sort_by_key(|marker| rank(marker, priority));
    capable
        .into_iter()
        .next()
        .map(|marker| SelectedReaderMarker {
            receiver_path: marker.receiver_path.clone(),
            marker,
        })
}

fn rank(marker: &PaykitReceiverMarker, priority: &[ReceiverPathPriority]) -> (usize, String) {
    let path = marker.receiver_path.as_str();
    let first_segment = path.split('/').next().unwrap_or_default();
    let priority_index = priority
        .iter()
        .position(|configured| configured.as_str() == first_segment)
        .unwrap_or(priority.len());
    (priority_index, path.into())
}

#[cfg(test)]
mod tests {
    use paykit_lib::{PaykitReceiverCapabilities, PublicKey};

    use super::*;

    fn marker(path: &str, capable: bool) -> PaykitReceiverMarker {
        PaykitReceiverMarker::new(
            PaykitReceiverPath::new(path).unwrap(),
            PaykitReceiverCapabilities {
                private_payments: capable,
                payment_requests: capable,
                receipts: false,
                outgoing_payments: false,
            },
            PublicKey::try_from_z32("tkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy")
                .unwrap(),
        )
    }

    fn priority(values: &[&str]) -> Vec<ReceiverPathPriority> {
        values
            .iter()
            .map(|value| ReceiverPathPriority::parse((*value).into()).unwrap())
            .collect()
    }

    #[test]
    fn prefers_bitkit_then_lexical_full_path_and_filters_capabilities() {
        let selected = select_reader_marker(
            [
                marker("other/wallet", true),
                marker("other/server", true),
                marker("bitkit/wallet", true),
                marker("bitkit/server", false),
            ],
            &priority(&["bitkit"]),
        )
        .unwrap();
        assert_eq!(selected.receiver_path.as_str(), "bitkit/wallet");

        let selected = select_reader_marker(
            [marker("other/wallet", true), marker("other/server", true)],
            &priority(&["bitkit"]),
        )
        .unwrap();
        assert_eq!(selected.receiver_path.as_str(), "other/server");
    }

    #[test]
    fn configured_priority_overrides_the_default_order() {
        let selected = select_reader_marker(
            [marker("bitkit/wallet", true), marker("other/server", true)],
            &priority(&["other", "bitkit"]),
        )
        .unwrap();
        assert_eq!(selected.receiver_path.as_str(), "other/server");
    }
}
