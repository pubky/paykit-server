//! Allowance intake: the server accepts, as the Allowee, the Allowance
//! proposals its invoiced readers leave on their links.
//!
//! The SDK derives a link's whole Allowance history for every read and every
//! acceptance, and the reader decides how large that history is. The intake
//! therefore runs on its own worker, never in an outbox handoff or the shared
//! transport loop, one task per Creator, and bounds how often and for how long
//! it reads each reader. It only queues acceptances; the transport worker
//! delivers them.

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex as StdMutex,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use paykit_lib::AllowanceId;
use paykit_sdk::{
    AllowanceFilter, AllowanceHistoryStatus, AllowanceLifecycleState, AllowanceLocalRole,
    AllowanceRecord, LinkedPeerState, PaykitSdkError, PubkyPublicKey, StorageAdapter,
};
use tokio::sync::Mutex as TokioMutex;
use tracing::{info, warn};
use uuid::Uuid;

use super::{PaykitAdapter, sdk_error_label};
use crate::{
    domain::locks::{ReaderPubky, parse_reader},
    persistence::InvoiceStore,
};

/// Acceptances queued per read. Each one is a durable outbound message, so a
/// reader that floods the link cannot turn one read into unbounded output; a
/// later read accepts the rest.
const MAX_ALLOWANCE_ACCEPTANCES_PER_READ: usize = 4;

/// Least time between two reads of one reader's Allowances.
pub const ALLOWANCE_INTAKE_COOLDOWN: Duration = Duration::from_secs(10);

/// A reader whose Allowance scan took `d` is read again no sooner than `d`
/// times this after that read began, so the intake spends at most about a
/// tenth of the time deriving any one reader's history, however large it grows.
/// Only the scan counts: acceptances and sends are bounded per read.
const ALLOWANCE_INTAKE_SPACING_FACTOR: u32 = 10;

/// Longest wait for one Allowance scan or one acceptance, including the wait
/// for the Creator lock. The SDK leaves timeouts to the caller, and a cancelled
/// shared-state operation exposes no unconfirmed state, so a stalled step is
/// dropped and counts as a failed read.
const ALLOWANCE_INTAKE_STEP_TIMEOUT: Duration = Duration::from_secs(20);

/// Longest a Creator's whole intake pass may take, finding its due readers
/// included: the read budget, one scan and the acceptances of the last read
/// fit well within it. The worker drops a pass that overruns it, so every
/// intake slot is freed within this time.
pub const ALLOWANCE_INTAKE_PASS_DEADLINE: Duration = Duration::from_secs(150);

/// A Creator's pass starts no new read after this much time spent reading;
/// the readers left over are read on the next pass. Finding the due readers
/// does not count, and the first read always starts, so every pass reads at
/// least one due reader.
const ALLOWANCE_INTAKE_READ_BUDGET: Duration = Duration::from_secs(5);

/// Decides when a reader's Allowances are read. A read is worth doing only
/// when the reader's link holds stream items an earlier read did not cover,
/// and no sooner than the cooldown, or ten times the last read's scan,
/// after the last read began.
#[derive(Debug)]
pub struct AllowanceIntake {
    cooldown: Duration,
    readers: StdMutex<HashMap<(Uuid, PubkyPublicKey), ReaderIntake>>,
}

#[derive(Debug, Default)]
struct ReaderIntake {
    /// Newest stream item an earlier read covered completely.
    covered_through: Option<u64>,
    last_started: Option<Instant>,
    last_scan: Duration,
}

/// A read that is due, to be reported back with [`AllowanceIntake::finish`].
struct IntakeTicket {
    creator_id: Uuid,
    reader: PubkyPublicKey,
    newest_stream_item_id: u64,
}

impl AllowanceIntake {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            readers: StdMutex::new(HashMap::new()),
        }
    }

    fn with_reader<T>(
        &self,
        creator_id: Uuid,
        reader: &PubkyPublicKey,
        f: impl FnOnce(&mut ReaderIntake) -> T,
    ) -> T {
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(readers.entry((creator_id, reader.clone())).or_default())
    }

    /// Whether the reader's link, whose newest stream item is `newest`, is due
    /// for a read at `now`.
    fn is_due(
        &self,
        creator_id: Uuid,
        reader: &PubkyPublicKey,
        newest: Option<u64>,
        now: Instant,
    ) -> bool {
        let Some(newest) = newest else {
            return false;
        };
        self.with_reader(creator_id, reader, |state| {
            let spacing = self
                .cooldown
                .max(state.last_scan * ALLOWANCE_INTAKE_SPACING_FACTOR);
            state.covered_through.is_none_or(|covered| newest > covered)
                && state
                    .last_started
                    .is_none_or(|started| now.saturating_duration_since(started) >= spacing)
        })
    }

    /// Starts a due read. It counts against the spacing at once, so a failed
    /// read is not retried sooner.
    fn begin(
        &self,
        creator_id: Uuid,
        reader: &PubkyPublicKey,
        newest: Option<u64>,
        now: Instant,
    ) -> Option<IntakeTicket> {
        if !self.is_due(creator_id, reader, newest, now) {
            return None;
        }
        self.with_reader(creator_id, reader, |state| state.last_started = Some(now));
        Some(IntakeTicket {
            creator_id,
            reader: reader.clone(),
            newest_stream_item_id: newest?,
        })
    }

    /// When the reader's last read started, if it was ever read.
    fn last_started(&self, creator_id: Uuid, reader: &PubkyPublicKey) -> Option<Instant> {
        self.with_reader(creator_id, reader, |state| state.last_started)
    }

    /// Records a finished read and how long its scan took. When it failed or
    /// left proposals behind the per-read limit, the same items stay due for
    /// the next read.
    fn finish(&self, ticket: IntakeTicket, drained: bool, scan: Duration) {
        self.with_reader(ticket.creator_id, &ticket.reader, |state| {
            state.last_scan = scan;
            if drained {
                state.covered_through = Some(ticket.newest_stream_item_id);
            }
        });
    }
}

/// Orders ready Creators for the intake's bounded task slots: a Creator never
/// started goes first, then the one whose last pass started longest ago. When
/// slow passes hold every slot, each Creator still gets a turn.
#[derive(Debug, Default)]
pub struct IntakeTurns {
    last_started: HashMap<Uuid, u64>,
    next: u64,
}

impl IntakeTurns {
    /// The ready Creators, least recently started first. A Creator that is no
    /// longer ready is forgotten.
    pub fn order(&mut self, mut ready: Vec<Uuid>) -> Vec<Uuid> {
        let current = ready.iter().copied().collect::<HashSet<_>>();
        self.last_started
            .retain(|creator, _| current.contains(creator));
        ready.sort_by_key(|creator| self.last_started.get(creator).copied());
        ready
    }

    /// Records that `creator`'s pass started.
    pub fn started(&mut self, creator: Uuid) {
        self.last_started.insert(creator, self.next);
        self.next += 1;
    }
}

impl PaykitAdapter {
    /// One intake pass over this Creator's linked readers. Only readers this
    /// Creator has invoiced are read: a Linked peer that another App on the
    /// shared identity linked is left to that App. A peer is checked again on
    /// every pass, and nothing it sent counts as covered until it is invoiced,
    /// so a first invoice makes the proposals it already sent due. Read and
    /// acceptance failures are logged per reader and retried after the spacing.
    ///
    /// Returns how many acceptances it queued. It sends none of them: the
    /// caller wakes the transport worker, which delivers them.
    pub async fn take_in_allowances(
        &self,
        intake: &AllowanceIntake,
        invoices: &InvoiceStore,
    ) -> paykit_sdk::Result<usize> {
        let now = Instant::now();
        let linked = {
            let _guard = self.mutation_lock.lock().await;
            self.sdk.linked_peers().await?
        }
        .into_iter()
        .filter(|peer| peer.state == LinkedPeerState::Linked)
        .map(|peer| peer.counterparty)
        .collect::<Vec<_>>();
        if linked.is_empty() {
            return Ok(0);
        }
        // Eligibility first: the stream of a peer this Creator never invoiced
        // is not read at all.
        let candidates = linked.iter().filter_map(reader_of).collect::<Vec<_>>();
        let invoiced = invoices
            .invoiced_readers(self.creator_id, &candidates)
            .await
            .map_err(|_| PaykitSdkError::Storage {
                context: "invoiced readers unavailable".into(),
                source: None,
            })?
            .iter()
            .map(ReaderPubky::to_string)
            .collect::<HashSet<_>>();
        let eligible = linked
            .into_iter()
            .filter(|peer| {
                reader_of(peer).is_some_and(|reader| invoiced.contains(&reader.to_string()))
            })
            .collect::<Vec<_>>();
        if eligible.is_empty() {
            return Ok(0);
        }
        let newest = self.newest_stream_items(&eligible).await?;
        let readers = eligible
            .into_iter()
            .filter_map(|peer| {
                let newest = newest.get(&peer).copied();
                intake
                    .is_due(self.creator_id, &peer, newest, now)
                    .then_some((peer, newest?))
            })
            .collect::<Vec<_>>();
        if readers.is_empty() {
            return Ok(0);
        }
        Ok(read_readers(
            self,
            &self.mutation_lock,
            intake,
            self.creator_id,
            readers,
            ALLOWANCE_INTAKE_READ_BUDGET,
            ALLOWANCE_INTAKE_STEP_TIMEOUT,
        )
        .await)
    }

    async fn newest_stream_items(
        &self,
        linked: &[PubkyPublicKey],
    ) -> paykit_sdk::Result<HashMap<PubkyPublicKey, u64>> {
        let linked = linked.to_vec();
        self.storage
            .transaction(move |transaction| {
                Ok(linked
                    .into_iter()
                    .filter_map(|peer| {
                        let newest = transaction
                            .private_stream_items(&peer)
                            .iter()
                            .map(|item| item.stream_item_id)
                            .max()?;
                        Some((peer, newest))
                    })
                    .collect())
            })
            .await
    }
}

/// The Locks Reader identity of a linked peer, in the canonical form invoice
/// admission hashes.
fn reader_of(peer: &PubkyPublicKey) -> Option<ReaderPubky> {
    parse_reader(&format!("pubky{peer}")).ok()
}

/// What the intake does on one reader's link.
#[async_trait]
trait AllowanceLink: Send + Sync {
    /// Proposals awaiting acceptance, each with the stream item that carried
    /// it, at most one more than a read accepts. The SDK derives the link's
    /// whole history for this, so it must not run under the Creator mutation
    /// lock.
    async fn pending_proposals(
        &self,
        reader: &PubkyPublicKey,
    ) -> paykit_sdk::Result<Vec<(AllowanceId, u64)>>;

    /// Queues one acceptance of the proposal carried by `stream_item_id`. The
    /// SDK derives the link's history again, and the caller serializes it with
    /// the Creator's other mutations.
    async fn accept(
        &self,
        reader: &PubkyPublicKey,
        id: &AllowanceId,
        stream_item_id: u64,
    ) -> paykit_sdk::Result<()>;
}

struct IntakeRead {
    accepted: usize,
    /// How long reading the pending proposals took, up to its failure if it
    /// failed. It spaces out the next read on every outcome.
    scan: Duration,
    /// Whether no proposal is left waiting behind the per-read limit, or why
    /// the read stopped.
    result: paykit_sdk::Result<bool>,
}

/// Reads the due readers, `(reader, newest stream item)`, least recently read
/// first, until `budget` has passed since the first read started; the first
/// read always starts. A reader left with proposals behind the per-read limit
/// therefore goes behind the others next pass. Returns how many acceptances it
/// queued.
async fn read_readers<L: AllowanceLink + ?Sized>(
    link: &L,
    mutation_lock: &TokioMutex<()>,
    intake: &AllowanceIntake,
    creator_id: Uuid,
    mut readers: Vec<(PubkyPublicKey, u64)>,
    budget: Duration,
    step_timeout: Duration,
) -> usize {
    readers.sort_by_key(|(peer, _)| intake.last_started(creator_id, peer));
    let mut reading_since = None::<Instant>;
    let mut queued = 0;
    for (peer, newest) in readers {
        if reading_since.is_some_and(|since| since.elapsed() >= budget) {
            break;
        }
        let now = Instant::now();
        let Some(ticket) = intake.begin(creator_id, &peer, Some(newest), now) else {
            continue;
        };
        reading_since.get_or_insert(now);
        let read = accept_proposals(link, mutation_lock, &peer, step_timeout).await;
        if read.accepted > 0 {
            info!(
                accepted = read.accepted,
                "accepted Paykit Allowance proposals"
            );
        }
        queued += read.accepted;
        let drained = match read.result {
            Ok(drained) => drained,
            Err(error) => {
                warn!(
                    stage = "allowance_intake",
                    cause = sdk_error_label(&error),
                    "Paykit Allowance intake failed; retried after the spacing"
                );
                false
            }
        };
        intake.finish(ticket, drained, read.scan);
    }
    queued
}

/// Accepts up to [`MAX_ALLOWANCE_ACCEPTANCES_PER_READ`] pending proposals.
/// The Creator mutation lock is held for one acceptance at a time, never for
/// the scan, so other Creator work interleaves with a reader's large history.
async fn accept_proposals<L: AllowanceLink + ?Sized>(
    link: &L,
    mutation_lock: &TokioMutex<()>,
    reader: &PubkyPublicKey,
    step_timeout: Duration,
) -> IntakeRead {
    let started = Instant::now();
    let pending = tokio::time::timeout(step_timeout, link.pending_proposals(reader))
        .await
        .unwrap_or_else(|_| Err(timed_out("Allowance scan")));
    let mut read = IntakeRead {
        accepted: 0,
        scan: started.elapsed(),
        result: Ok(true),
    };
    let pending = match pending {
        Ok(pending) => pending,
        Err(error) => {
            read.result = Err(error);
            return read;
        }
    };
    for (id, stream_item_id) in pending.iter().take(MAX_ALLOWANCE_ACCEPTANCES_PER_READ) {
        let accepted = tokio::time::timeout(step_timeout, async {
            let _guard = mutation_lock.lock().await;
            link.accept(reader, id, *stream_item_id).await
        })
        .await
        .unwrap_or_else(|_| Err(timed_out("Allowance acceptance")));
        if let Err(error) = accepted {
            read.result = Err(error);
            return read;
        }
        read.accepted += 1;
    }
    read.result = Ok(pending.len() <= MAX_ALLOWANCE_ACCEPTANCES_PER_READ);
    read
}

fn timed_out(step: &str) -> PaykitSdkError {
    PaykitSdkError::Transport {
        context: format!("{step} timed out"),
        source: None,
    }
}

#[async_trait]
impl AllowanceLink for PaykitAdapter {
    async fn pending_proposals(
        &self,
        reader: &PubkyPublicKey,
    ) -> paykit_sdk::Result<Vec<(AllowanceId, u64)>> {
        Ok(self
            .sdk
            .list_allowances(AllowanceFilter {
                counterparty: Some(reader.clone()),
                local_role: Some(AllowanceLocalRole::Allowee),
                states: vec![AllowanceLifecycleState::Proposed],
            })
            .await?
            .iter()
            .filter_map(awaiting_allowee_acceptance)
            .take(MAX_ALLOWANCE_ACCEPTANCES_PER_READ + 1)
            .collect())
    }

    async fn accept(
        &self,
        reader: &PubkyPublicKey,
        id: &AllowanceId,
        stream_item_id: u64,
    ) -> paykit_sdk::Result<()> {
        self.sdk
            .accept_allowance(reader.clone(), id, stream_item_id)
            .await
            .map(|_| ())
    }
}

/// An Allowance proposal the reader sent, naming this identity as the Allowee,
/// with consistent history and no response yet: its ID and the stream item
/// that carried it.
fn awaiting_allowee_acceptance(record: &AllowanceRecord) -> Option<(AllowanceId, u64)> {
    if record.local_role != Some(AllowanceLocalRole::Allowee)
        || record.state != AllowanceLifecycleState::Proposed
        || record.history_status != AllowanceHistoryStatus::Consistent
        || record.proposal_outbound_message_id.is_some()
    {
        return None;
    }
    Some((
        AllowanceId::new(record.allowance_id.clone()).ok()?,
        record.proposal_stream_item_id?,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    const READER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const STEP: Duration = Duration::from_secs(5);

    fn reader() -> PubkyPublicKey {
        PubkyPublicKey::from_raw_or_app_key(READER).unwrap()
    }

    fn other_reader() -> PubkyPublicKey {
        PubkyPublicKey::from_raw_or_app_key(
            "pubky5jsjx1o6fzu6aeeo697r3i5rx15zq41kikcye8wtwdqm4nb4tryo",
        )
        .unwrap()
    }

    fn received_allowee_proposal() -> AllowanceRecord {
        AllowanceRecord {
            counterparty: reader(),
            allowance_id: "b7f9c2a1-6d43-4b0e-a8d4-0fe2c712ab44".into(),
            local_role: Some(AllowanceLocalRole::Allowee),
            state: AllowanceLifecycleState::Proposed,
            history_status: AllowanceHistoryStatus::Consistent,
            proposal_event_id: Some("8a0d8b4c-913f-4e31-9f2c-2a6f5bb4d201".into()),
            terms: None,
            proposal_stream_item_id: Some(7),
            proposal_outbound_message_id: None,
            proposal_outbound_status: None,
            acceptance_event_id: None,
            acceptance_outbound_status: None,
            rejection_event_id: None,
            rejection_outbound_status: None,
            end_event_id: None,
            end_outbound_status: None,
            pending_causal_event_ids: Vec::new(),
            conflict_event_ids: Vec::new(),
            last_stream_item_id: Some(7),
            last_outbound_message_id: None,
            last_outbound_status: None,
            last_event_at: None,
            invalid_reason: None,
        }
    }

    #[test]
    fn a_linked_peer_maps_to_the_reader_identity_invoices_hash() {
        // Admission hashes `ReaderPubky::to_string()`; the intake must look the
        // same identity up under the same bytes.
        let admitted = parse_reader(&format!("pubky{READER}")).unwrap();
        let peer = PubkyPublicKey::from_raw_or_app_key(admitted.to_string()).unwrap();
        assert_eq!(reader_of(&peer).unwrap().to_string(), admitted.to_string());
        assert_eq!(
            reader_of(&reader()).unwrap().to_string(),
            admitted.to_string()
        );
    }

    #[test]
    fn allowances_are_read_only_when_the_link_received_new_items() {
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let creator = Uuid::new_v4();
        let start = Instant::now();

        // Nothing received on the link: nothing to read.
        assert!(intake.begin(creator, &reader(), None, start).is_none());

        let ticket = intake.begin(creator, &reader(), Some(5), start).unwrap();
        intake.finish(ticket, true, Duration::ZERO);

        // The same items again, long after the cooldown: still nothing to read.
        let later = start + Duration::from_secs(3_600);
        assert!(intake.begin(creator, &reader(), Some(5), later).is_none());
        assert!(intake.begin(creator, &reader(), Some(6), later).is_some());
    }

    #[test]
    fn a_reader_is_read_at_most_once_per_cooldown() {
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let creator = Uuid::new_v4();
        let start = Instant::now();

        let ticket = intake.begin(creator, &reader(), Some(1), start).unwrap();
        intake.finish(ticket, true, Duration::ZERO);

        // A flood of new items does not make the next read come sooner.
        for (seconds, newest) in [(1, 10), (5, 500), (9, 90_000)] {
            let now = start + Duration::from_secs(seconds);
            assert!(
                intake
                    .begin(creator, &reader(), Some(newest), now)
                    .is_none()
            );
        }
        let after = start + Duration::from_secs(10);
        assert!(
            intake
                .begin(creator, &reader(), Some(90_000), after)
                .is_some()
        );
    }

    #[test]
    fn a_slow_scan_spaces_the_next_read_out_to_ten_times_its_duration() {
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let creator = Uuid::new_v4();
        let start = Instant::now();

        // Scanning a large history takes 4 s: the next read waits 40 s.
        let ticket = intake.begin(creator, &reader(), Some(1), start).unwrap();
        intake.finish(ticket, true, Duration::from_secs(4));
        let early = start + Duration::from_secs(39);
        assert!(intake.begin(creator, &reader(), Some(2), early).is_none());
        let due = start + Duration::from_secs(40);
        let ticket = intake.begin(creator, &reader(), Some(2), due).unwrap();

        // A quick scan brings it back to the cooldown.
        intake.finish(ticket, true, Duration::ZERO);
        let next = due + Duration::from_secs(10);
        assert!(intake.begin(creator, &reader(), Some(3), next).is_some());
    }

    #[test]
    fn a_failed_or_unfinished_read_is_retried_after_the_cooldown() {
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let creator = Uuid::new_v4();
        let start = Instant::now();
        let after = start + Duration::from_secs(10);

        // A failed read counted against the cooldown and covers nothing.
        let ticket = intake.begin(creator, &reader(), Some(4), start).unwrap();
        assert!(intake.begin(creator, &reader(), Some(4), start).is_none());
        intake.finish(ticket, false, Duration::ZERO);
        let ticket = intake.begin(creator, &reader(), Some(4), after).unwrap();

        // A read that left proposals behind the per-read limit keeps the items due.
        intake.finish(ticket, false, Duration::ZERO);
        let again = after + Duration::from_secs(10);
        let ticket = intake.begin(creator, &reader(), Some(4), again).unwrap();
        intake.finish(ticket, true, Duration::ZERO);
        let last = again + Duration::from_secs(10);
        assert!(intake.begin(creator, &reader(), Some(4), last).is_none());
    }

    #[test]
    fn a_link_skipped_before_its_first_invoice_stays_due() {
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let creator = Uuid::new_v4();
        let now = Instant::now();

        // A pass that finds the peer not yet invoiced, including one whose
        // invoice commits right after the lookup, checks only whether it is
        // due. Its proposal stays due until a pass finds it invoiced.
        for pass in 0..3 {
            let at = now + Duration::from_secs(pass);
            assert!(intake.is_due(creator, &reader(), Some(8), at));
        }
        assert!(intake.begin(creator, &reader(), Some(8), now).is_some());
    }

    #[test]
    fn readers_and_creators_are_scheduled_independently() {
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let (creator, other_creator) = (Uuid::new_v4(), Uuid::new_v4());
        let now = Instant::now();

        assert!(intake.begin(creator, &reader(), Some(1), now).is_some());
        assert!(
            intake
                .begin(creator, &other_reader(), Some(1), now)
                .is_some()
        );
        assert!(
            intake
                .begin(other_creator, &reader(), Some(1), now)
                .is_some()
        );
    }

    #[test]
    fn creators_take_turns_least_recently_started_first() {
        let mut turns = IntakeTurns::default();
        let creators = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

        // Two slots: the first two start, and the third goes first next time.
        let order = turns.order(creators.to_vec());
        turns.started(order[0]);
        turns.started(order[1]);
        assert_eq!(turns.order(creators.to_vec())[0], order[2]);
        turns.started(order[2]);
        assert_eq!(turns.order(creators.to_vec()), order);

        // A Creator that is no longer ready is forgotten; if it comes back it
        // counts as never started.
        turns.order(vec![order[1], order[2]]);
        assert_eq!(turns.order(creators.to_vec())[0], order[0]);
    }

    #[tokio::test]
    async fn every_pass_reads_a_due_reader_even_with_no_budget_left() {
        let lock = TokioMutex::new(());
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let creator = Uuid::new_v4();
        let link = ReadyLink::default();
        let readers = vec![(reader(), 1), (other_reader(), 1)];

        // However long finding the due readers took, a pass reads one reader;
        // with no budget, only one. The next pass reads the other.
        for expected in [reader(), other_reader()] {
            let queued = read_readers(
                &link,
                &lock,
                &intake,
                creator,
                readers.clone(),
                Duration::ZERO,
                STEP,
            )
            .await;
            assert_eq!(queued, 1);
            assert_eq!(link.scans.lock().unwrap().last(), Some(&expected));
        }
        assert_eq!(link.scans.lock().unwrap().len(), 2);

        // With budget, one pass reads every due reader.
        let intake = AllowanceIntake::new(Duration::from_secs(10));
        let queued = read_readers(
            &link,
            &lock,
            &intake,
            creator,
            readers,
            Duration::from_secs(5),
            STEP,
        )
        .await;
        assert_eq!(queued, 2);
    }

    #[tokio::test]
    async fn a_reader_left_with_a_backlog_goes_behind_the_others() {
        let lock = TokioMutex::new(());
        // No cooldown: the backlogged reader stays due on every pass.
        let intake = AllowanceIntake::new(Duration::ZERO);
        let creator = Uuid::new_v4();
        let link = ReadyLink {
            backlog: HashMap::from([(reader(), MAX_ALLOWANCE_ACCEPTANCES_PER_READ * 3)]),
            ..ReadyLink::default()
        };
        let readers = vec![(reader(), 1), (other_reader(), 1)];

        // One read per pass. The backlogged reader is read first, is not
        // drained, and the next pass reads the other reader before it again.
        for _ in 0..2 {
            read_readers(
                &link,
                &lock,
                &intake,
                creator,
                readers.clone(),
                Duration::ZERO,
                STEP,
            )
            .await;
            // Past the scan's spacing: the backlogged reader is due again.
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(intake.is_due(creator, &reader(), Some(1), Instant::now()));
        }
        assert_eq!(*link.scans.lock().unwrap(), [reader(), other_reader()]);
    }

    #[tokio::test]
    async fn a_stalled_scan_or_acceptance_ends_the_read_within_the_step_timeout() {
        const SHORT: Duration = Duration::from_millis(100);

        // A scan that never returns, like a derivation that never finishes.
        let lock = Arc::new(TokioMutex::new(()));
        let link = SlowLink::new(&lock, 1);
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            accept_proposals(&link, &lock, &reader(), SHORT),
        )
        .await
        .expect("the stalled scan held the read");
        assert!(matches!(read.result, Err(PaykitSdkError::Transport { .. })));
        assert!(read.scan >= SHORT);

        // An acceptance that cannot get the Creator lock.
        let link = SlowLink::new(&lock, 1);
        link.scan_release.notify_one();
        let held = lock.clone().lock_owned().await;
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            accept_proposals(&link, &lock, &reader(), SHORT),
        )
        .await
        .expect("the stalled acceptance held the read");
        assert!(matches!(read.result, Err(PaykitSdkError::Transport { .. })));
        assert_eq!(read.accepted, 0);
        drop(held);
    }

    /// A link with one pending proposal on every scan, or `backlog` for a
    /// reader listed there.
    #[derive(Default)]
    struct ReadyLink {
        scans: StdMutex<Vec<PubkyPublicKey>>,
        backlog: HashMap<PubkyPublicKey, usize>,
    }

    #[async_trait]
    impl AllowanceLink for ReadyLink {
        async fn pending_proposals(
            &self,
            reader: &PubkyPublicKey,
        ) -> paykit_sdk::Result<Vec<(AllowanceId, u64)>> {
            self.scans.lock().unwrap().push(reader.clone());
            let pending = self.backlog.get(reader).copied().unwrap_or(1);
            Ok((0..pending as u64)
                .map(|item| (AllowanceId::new(Uuid::new_v4().to_string()).unwrap(), item))
                .collect())
        }

        async fn accept(
            &self,
            _reader: &PubkyPublicKey,
            _id: &AllowanceId,
            _stream_item_id: u64,
        ) -> paykit_sdk::Result<()> {
            Ok(())
        }
    }

    /// A link whose scan can be held in flight, like the derivation of a
    /// large Allowance history, and that reports whether the Creator mutation
    /// lock was held at each step.
    struct SlowLink {
        lock: Arc<TokioMutex<()>>,
        pending: usize,
        scan_entered: tokio::sync::Notify,
        scan_release: tokio::sync::Notify,
        lock_held_during_accepts: StdMutex<Vec<bool>>,
        lock_held_during_scan: StdMutex<Option<bool>>,
        scan_takes: Duration,
        fail_scan: bool,
        fail_accept: bool,
    }

    impl SlowLink {
        fn new(lock: &Arc<TokioMutex<()>>, pending: usize) -> Self {
            Self {
                lock: lock.clone(),
                pending,
                scan_entered: tokio::sync::Notify::new(),
                scan_release: tokio::sync::Notify::new(),
                lock_held_during_accepts: StdMutex::new(Vec::new()),
                lock_held_during_scan: StdMutex::new(None),
                scan_takes: Duration::ZERO,
                fail_scan: false,
                fail_accept: false,
            }
        }
    }

    #[async_trait]
    impl AllowanceLink for SlowLink {
        async fn pending_proposals(
            &self,
            _reader: &PubkyPublicKey,
        ) -> paykit_sdk::Result<Vec<(AllowanceId, u64)>> {
            *self.lock_held_during_scan.lock().unwrap() = Some(self.lock.try_lock().is_err());
            self.scan_entered.notify_one();
            self.scan_release.notified().await;
            tokio::time::sleep(self.scan_takes).await;
            if self.fail_scan {
                return Err(PaykitSdkError::Transport {
                    context: "history not read".into(),
                    source: None,
                });
            }
            Ok((0..self.pending as u64)
                .map(|item| (AllowanceId::new(Uuid::new_v4().to_string()).unwrap(), item))
                .collect())
        }

        async fn accept(
            &self,
            _reader: &PubkyPublicKey,
            _id: &AllowanceId,
            _stream_item_id: u64,
        ) -> paykit_sdk::Result<()> {
            self.lock_held_during_accepts
                .lock()
                .unwrap()
                .push(self.lock.try_lock().is_err());
            if self.fail_accept {
                return Err(PaykitSdkError::Transport {
                    context: "acceptance not stored".into(),
                    source: None,
                });
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_large_allowance_history_does_not_hold_the_creator_lock_while_it_is_read() {
        let lock = Arc::new(TokioMutex::new(()));
        let link = Arc::new(SlowLink::new(&lock, 1_000));
        let intake = tokio::spawn({
            let (link, lock) = (link.clone(), lock.clone());
            async move { accept_proposals(link.as_ref(), &lock, &reader(), STEP).await }
        });

        // The scan is in flight, as for a reader with a very large history.
        // Every other Creator operation takes this lock, and none waits for it.
        link.scan_entered.notified().await;
        let other_creator_work = tokio::time::timeout(Duration::from_secs(5), lock.lock()).await;
        assert!(
            other_creator_work.is_ok(),
            "another operation waited for the reader's scan"
        );
        drop(other_creator_work);

        link.scan_release.notify_one();
        let outcome = intake.await.unwrap();

        assert_eq!(*link.lock_held_during_scan.lock().unwrap(), Some(false));
        // Each acceptance runs under the lock, and the lock is free between them.
        assert_eq!(
            *link.lock_held_during_accepts.lock().unwrap(),
            [true; MAX_ALLOWANCE_ACCEPTANCES_PER_READ]
        );
        assert_eq!(outcome.accepted, MAX_ALLOWANCE_ACCEPTANCES_PER_READ);
        assert!(!outcome.result.unwrap());
        assert!(lock.try_lock().is_ok());
    }

    #[tokio::test]
    async fn the_intake_accepts_everything_pending_up_to_its_limit() {
        for (pending, accepted, drained) in [
            (0, 0, true),
            (1, 1, true),
            (
                MAX_ALLOWANCE_ACCEPTANCES_PER_READ,
                MAX_ALLOWANCE_ACCEPTANCES_PER_READ,
                true,
            ),
            (
                MAX_ALLOWANCE_ACCEPTANCES_PER_READ + 1,
                MAX_ALLOWANCE_ACCEPTANCES_PER_READ,
                false,
            ),
        ] {
            let lock = Arc::new(TokioMutex::new(()));
            let link = SlowLink::new(&lock, pending);
            link.scan_release.notify_one();

            let outcome = accept_proposals(&link, &lock, &reader(), STEP).await;

            assert_eq!(
                (outcome.accepted, outcome.result.unwrap()),
                (accepted, drained)
            );
        }
    }

    #[tokio::test]
    async fn a_failed_acceptance_ends_the_read_and_frees_the_lock() {
        let lock = Arc::new(TokioMutex::new(()));
        let mut link = SlowLink::new(&lock, 3);
        link.fail_accept = true;
        link.scan_release.notify_one();

        let read = accept_proposals(&link, &lock, &reader(), STEP).await;

        assert!(matches!(read.result, Err(PaykitSdkError::Transport { .. })));
        assert_eq!(read.accepted, 0);
        assert_eq!(link.lock_held_during_accepts.lock().unwrap().len(), 1);
        assert!(lock.try_lock().is_ok());
    }

    #[tokio::test]
    async fn a_failed_read_is_spaced_out_by_the_time_its_scan_took() {
        const SCAN: Duration = Duration::from_millis(200);
        // A scan that fails after deriving a large history, and a scan that
        // succeeds before an acceptance fails.
        for (fail_scan, fail_accept) in [(true, false), (false, true)] {
            let lock = Arc::new(TokioMutex::new(()));
            let mut link = SlowLink::new(&lock, 3);
            link.scan_takes = SCAN;
            link.fail_scan = fail_scan;
            link.fail_accept = fail_accept;
            link.scan_release.notify_one();
            let intake = AllowanceIntake::new(Duration::ZERO);
            let creator = Uuid::new_v4();
            let start = Instant::now();
            let ticket = intake.begin(creator, &reader(), Some(1), start).unwrap();

            let read = accept_proposals(&link, &lock, &reader(), STEP).await;
            assert!(read.result.is_err());
            assert!(read.scan >= SCAN, "{:?}", read.scan);
            intake.finish(ticket, false, read.scan);

            let spacing = read.scan * ALLOWANCE_INTAKE_SPACING_FACTOR;
            let early = start + spacing - Duration::from_millis(1);
            assert!(intake.begin(creator, &reader(), Some(1), early).is_none());
            assert!(
                intake
                    .begin(creator, &reader(), Some(1), start + spacing)
                    .is_some()
            );
        }
    }

    #[test]
    fn only_received_consistent_allowee_proposals_are_accepted() {
        let (id, stream_item_id) =
            awaiting_allowee_acceptance(&received_allowee_proposal()).unwrap();
        assert_eq!(id.as_str(), "b7f9c2a1-6d43-4b0e-a8d4-0fe2c712ab44");
        assert_eq!(stream_item_id, 7);

        let rejected: [fn(&mut AllowanceRecord); 7] = [
            // The receiver would be the payer.
            |record| record.local_role = Some(AllowanceLocalRole::Allower),
            |record| record.local_role = None,
            // Already answered, ended or colliding.
            |record| record.state = AllowanceLifecycleState::Accepted,
            |record| record.state = AllowanceLifecycleState::Conflicted,
            // Evidence that needs review or recovery first.
            |record| record.history_status = AllowanceHistoryStatus::Invalid,
            |record| record.history_status = AllowanceHistoryStatus::UnresolvedReferences,
            // A proposal this receiver sent is not its to accept.
            |record| {
                record.proposal_stream_item_id = None;
                record.proposal_outbound_message_id = Some(3);
            },
        ];
        for change in rejected {
            let mut record = received_allowee_proposal();
            change(&mut record);
            assert!(awaiting_allowee_acceptance(&record).is_none());
        }
    }
}
