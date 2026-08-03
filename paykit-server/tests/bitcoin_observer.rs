use bitcoin::{OutPoint, Txid, hashes::Hash};
use paykit_server::{
    bitcoin::{DirectBinding, ObservationAction, ObservationTarget, ObservedOutput, TrackedOutput},
    config::BitcoinNetwork,
};

fn outpoint(label: u8) -> OutPoint {
    OutPoint::new(Txid::from_byte_array([label; 32]), 0)
}

fn binding(label: u8, sats: u64, confirmations: u32, present: bool) -> DirectBinding {
    DirectBinding::new(outpoint(label).to_string(), sats, confirmations, present)
}

fn output(label: u8, sats: u64, confirmations: u32, present: bool) -> ObservedOutput {
    ObservedOutput {
        network: BitcoinNetwork::Regtest,
        address: "bcrt1invoice".into(),
        outpoint: outpoint(label),
        sats,
        confirmations,
        present,
    }
}

#[test]
fn direct_output_status_is_factual_and_confirmation_based() {
    let mut observed = output(1, 101, 0, true);
    assert_eq!(observed.status(), "detected");
    observed.confirmations = 1;
    assert_eq!(observed.status(), "confirmed");
    observed.present = false;
    assert_eq!(observed.status(), "undetected");
}

#[test]
fn amount_matched_zero_conf_output_can_be_replaced() {
    let current = binding(1, 100, 0, true);
    assert_eq!(
        current.action_for(&output(2, 100, 0, true), 100),
        ObservationAction::Replace
    );
}

#[test]
fn one_confirmation_freezes_amount_matched_output_until_reorg() {
    let confirmed = binding(1, 100, 1, true);
    assert_eq!(
        confirmed.action_for(&output(2, 100, 0, true), 100),
        ObservationAction::Ignore
    );
    let reorged = binding(1, 100, 0, true);
    assert_eq!(
        reorged.action_for(&output(2, 100, 0, true), 100),
        ObservationAction::Replace
    );
}

#[test]
fn underpayment_remains_replaceable_at_every_confirmation_count() {
    let underpaid = binding(1, 99, 42, true);
    assert_eq!(
        underpaid.action_for(&output(2, 100, 0, true), 100),
        ObservationAction::Replace
    );
}

#[test]
fn matching_six_confirmation_output_is_final_at_exactly_six() {
    let final_output = binding(1, 100, 9, true);
    assert!(final_output.is_final(100));
    assert_eq!(final_output.reported_confirmations(100), 6);
    assert_eq!(
        final_output.action_for(&output(2, 100, 0, true), 100),
        ObservationAction::Ignore
    );
}

#[test]
fn bitcoin_observation_debug_redacts_addresses_and_outpoints() {
    let outpoint = outpoint(9);
    let address = "bcrt1qinvoiceaddress";
    let target = ObservationTarget::new(address, Some(TrackedOutput::new(outpoint, 100)));
    let observed = ObservedOutput {
        network: BitcoinNetwork::Regtest,
        address: address.into(),
        outpoint,
        sats: 100,
        confirmations: 0,
        present: true,
    };
    let binding = DirectBinding::new(outpoint.to_string(), 100, 0, true);

    for debug in [
        format!("{target:?}"),
        format!("{:?}", target.current().unwrap()),
        format!("{observed:?}"),
        format!("{binding:?}"),
    ] {
        assert!(!debug.contains(address));
        assert!(!debug.contains(&outpoint.to_string()));
    }
}
