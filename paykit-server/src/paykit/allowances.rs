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
    future::Future,
    sync::Mutex as StdMutex,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use paykit_lib::AllowanceId;
use paykit_sdk::{
    AllowanceFilter, AllowanceHistoryStatus, AllowanceLifecycleState, AllowanceLocalRole,
    AllowanceRecord, LinkedPeerState, PaykitSdkError, PubkyPublicKey, StorageAdapter,
};
use tokio::{sync::Mutex as TokioMutex, task::JoinSet};
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

/// A Creator's pass starts no new read after this much time; the readers left
/// over are read on the next pass.
const ALLOWANCE_INTAKE_PASS_BUDGET: Duration = Duration::from_secs(5);

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

/// Runs each Creator's intake pass on its own task, at most one per Creator.
/// A pass that stalls, such as one deriving a very large history, holds back
/// only its own Creator's next pass.
#[derive(Default)]
pub struct AllowanceIntakePasses {
    running: HashSet<Uuid>,
    passes: JoinSet<Uuid>,
}

impl AllowanceIntakePasses {
    /// Starts a pass for `creator_id` unless its previous one is still running.
    pub fn start<F>(&mut self, creator_id: Uuid, pass: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.reap();
        if !self.running.insert(creator_id) {
            return false;
        }
        self.passes.spawn(async move {
            pass.await;
            creator_id
        });
        true
    }

    fn reap(&mut self) {
        while let Some(finished) = self.passes.try_join_next() {
            if let Ok(creator_id) = finished {
                self.running.remove(&creator_id);
            }
        }
        // A panicked pass loses its Creator ID; forget every Creator whose task is gone.
        if self.passes.is_empty() {
            self.running.clear();
        }
    }

    /// Lets the running passes finish. An acceptance is never cut short, since
    /// it holds the Creator's shared-state lock.
    pub async fn drain(&mut self) {
        while self.passes.join_next().await.is_some() {}
        self.running.clear();
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
        let pass_started = Instant::now();
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
        let newest = self.newest_stream_items(&linked).await?;
        let due = linked
            .into_iter()
            .filter_map(|peer| {
                let newest = newest.get(&peer).copied();
                intake
                    .is_due(self.creator_id, &peer, newest, pass_started)
                    .then_some((peer, newest?))
            })
            .collect::<Vec<_>>();
        if due.is_empty() {
            return Ok(0);
        }
        let candidates = due
            .iter()
            .filter_map(|(peer, _)| reader_of(peer))
            .collect::<Vec<_>>();
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
        let mut queued = 0;
        for (peer, newest) in due {
            if !reader_of(&peer).is_some_and(|reader| invoiced.contains(&reader.to_string())) {
                continue;
            }
            if pass_started.elapsed() >= ALLOWANCE_INTAKE_PASS_BUDGET {
                break;
            }
            let Some(ticket) = intake.begin(self.creator_id, &peer, Some(newest), Instant::now())
            else {
                continue;
            };
            let read = accept_proposals(self, &self.mutation_lock, &peer).await;
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
        Ok(queued)
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

/// Accepts up to [`MAX_ALLOWANCE_ACCEPTANCES_PER_READ`] pending proposals.
/// The Creator mutation lock is held for one acceptance at a time, never for
/// the scan, so other Creator work interleaves with a reader's large history.
async fn accept_proposals<L: AllowanceLink + ?Sized>(
    link: &L,
    mutation_lock: &TokioMutex<()>,
    reader: &PubkyPublicKey,
) -> IntakeRead {
    let started = Instant::now();
    let pending = link.pending_proposals(reader).await;
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
        let _guard = mutation_lock.lock().await;
        if let Err(error) = link.accept(reader, id, *stream_item_id).await {
            read.result = Err(error);
            return read;
        }
        read.accepted += 1;
    }
    read.result = Ok(pending.len() <= MAX_ALLOWANCE_ACCEPTANCES_PER_READ);
    read
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    const READER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";

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

    #[tokio::test]
    async fn a_stalled_pass_holds_back_only_its_own_creator() {
        let mut passes = AllowanceIntakePasses::default();
        let (stalled, other) = (Uuid::new_v4(), Uuid::new_v4());
        let release = Arc::new(tokio::sync::Notify::new());
        let other_passes = Arc::new(AtomicUsize::new(0));

        assert!(passes.start(stalled, {
            let release = release.clone();
            async move { release.notified().await }
        }));
        for _ in 0..3 {
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let counter = other_passes.clone();
            assert!(passes.start(other, async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = done_tx.send(());
            }));
            tokio::time::timeout(Duration::from_secs(5), done_rx)
                .await
                .expect("another Creator's pass waited for the stalled one")
                .unwrap();
            while passes.running.contains(&other) {
                tokio::task::yield_now().await;
                passes.reap();
            }
            // The stalled Creator gets no second, overlapping pass.
            assert!(!passes.start(stalled, async {}));
        }
        assert_eq!(other_passes.load(Ordering::SeqCst), 3);

        release.notify_one();
        passes.drain().await;
        assert!(passes.start(stalled, async {}));
        passes.drain().await;
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
            async move { accept_proposals(link.as_ref(), &lock, &reader()).await }
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

            let outcome = accept_proposals(&link, &lock, &reader()).await;

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

        let read = accept_proposals(&link, &lock, &reader()).await;

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

            let read = accept_proposals(&link, &lock, &reader()).await;
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
