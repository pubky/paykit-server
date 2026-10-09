//! Durable unpublished Marketplace invoice preparation.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    application::semantic_intent::DeliveryIntentV1,
    crypto::{Crypto, EncryptedEnvelope, EnvelopeContext, LookupHash},
    domain::locks::{CreatorPubky, ReaderPubky, parse_reader},
    persistence::{PersistenceError, invoices::InvoicePaymentRecordV1},
};

const OPERATION_BINDING_UNIQUE_CONSTRAINT: &str = "marketplace_preparation_operation_binding_key";

/// Opaque inputs for one atomic Marketplace preparation.
pub struct MarketplacePreparationInput<'a> {
    pub creator: &'a CreatorPubky,
    pub reader: &'a ReaderPubky,
    pub reference: &'a str,
    pub operation_id: &'a str,
    pub request_binding: &'a [u8],
    pub total_sats: u64,
    pub payment_window_seconds: u64,
    pub prepare_ttl_seconds: u64,
    pub payloads: &'a dyn MarketplacePreparationPayloadFactory,
}

/// Immutable caller binding used to activate one prepared Marketplace invoice.
pub struct MarketplaceActivationInput<'a> {
    pub creator: &'a CreatorPubky,
    pub invoice_id: Uuid,
    pub total_sats: u64,
    pub proposal_acceptance_window: std::time::Duration,
}

/// Closed lifecycle failures used to preserve caller recovery semantics.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum MarketplaceLifecyclePersistenceError {
    /// Prepared invoice expired before activation and needs a new attempt.
    #[error("marketplace payment preparation expired")]
    PrepareExpired,
    /// A terminal lifecycle transition already won.
    #[error("marketplace payment lifecycle is terminal")]
    LifecycleTerminal,
    /// Caller total differs from the authoritative prepared total.
    #[error("marketplace payment total does not match")]
    TotalMismatch,
    /// A different immutable business outcome was already recorded.
    #[error("marketplace business resolution conflicts with persisted outcome")]
    ResolutionConflict,
    /// Generic persistence or validation failure.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
}

/// Complete private proposal material derived after the transaction reserves an address index.
pub struct MarketplacePreparationPayloads {
    pub payment_request_intent: DeliveryIntentV1,
}

/// Derives immutable proposal material from a transaction-reserved child index.
pub trait MarketplacePreparationPayloadFactory: Send + Sync {
    fn for_child_index(
        &self,
        child_index: i64,
    ) -> Result<MarketplacePreparationPayloads, PersistenceError>;
}

/// Side-effect-free result of checking one scoped operation identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketplacePreparationPreflight {
    New,
    ExactReplay,
    Conflict,
}

/// Secret-free result of preparing an unpublished Marketplace invoice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketplacePreparationResult {
    invoice_id: Uuid,
    total_sats: u64,
    prepared_at: OffsetDateTime,
    prepare_expires_at: OffsetDateTime,
    replayed: bool,
}

impl MarketplacePreparationResult {
    /// Builds a result returned by an injected persistence adapter.
    pub fn new(
        invoice_id: Uuid,
        total_sats: u64,
        prepared_at: OffsetDateTime,
        prepare_expires_at: OffsetDateTime,
        replayed: bool,
    ) -> Self {
        Self {
            invoice_id,
            total_sats,
            prepared_at,
            prepare_expires_at,
            replayed,
        }
    }

    pub fn invoice_id(&self) -> Uuid {
        self.invoice_id
    }

    pub fn total_sats(&self) -> u64 {
        self.total_sats
    }

    pub fn prepared_at(&self) -> OffsetDateTime {
        self.prepared_at
    }

    pub fn prepare_expires_at(&self) -> OffsetDateTime {
        self.prepare_expires_at
    }

    pub fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Secret-free stored activation response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketplaceActivationResult {
    invoice_id: Uuid,
    activated_at: OffsetDateTime,
    payment_deadline: OffsetDateTime,
    total_sats: u64,
    replayed: bool,
}

impl MarketplaceActivationResult {
    /// Builds a result returned by an injected persistence adapter.
    pub fn new(
        invoice_id: Uuid,
        activated_at: OffsetDateTime,
        payment_deadline: OffsetDateTime,
        total_sats: u64,
        replayed: bool,
    ) -> Self {
        Self {
            invoice_id,
            activated_at,
            payment_deadline,
            total_sats,
            replayed,
        }
    }

    pub fn invoice_id(&self) -> Uuid {
        self.invoice_id
    }
    pub fn activated_at(&self) -> OffsetDateTime {
        self.activated_at
    }
    pub fn payment_deadline(&self) -> OffsetDateTime {
        self.payment_deadline
    }
    pub fn total_sats(&self) -> u64 {
        self.total_sats
    }
    pub fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Secret-free stored void response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketplaceVoidResult {
    invoice_id: Uuid,
    voided_at: OffsetDateTime,
    replayed: bool,
}

impl MarketplaceVoidResult {
    /// Builds a result returned by an injected persistence adapter.
    pub fn new(invoice_id: Uuid, voided_at: OffsetDateTime, replayed: bool) -> Self {
        Self {
            invoice_id,
            voided_at,
            replayed,
        }
    }

    pub fn invoice_id(&self) -> Uuid {
        self.invoice_id
    }
    pub fn voided_at(&self) -> OffsetDateTime {
        self.voided_at
    }
    pub fn replayed(&self) -> bool {
        self.replayed
    }
}

/// Closed Marketplace business outcome vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketplaceResolutionOutcome {
    PaidManually,
    Refunded,
    Abandoned,
}

impl MarketplaceResolutionOutcome {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "paid_manually" => Some(Self::PaidManually),
            "refunded" => Some(Self::Refunded),
            "abandoned" => Some(Self::Abandoned),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PaidManually => "paid_manually",
            Self::Refunded => "refunded",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Secret-free stored Marketplace resolution response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketplaceResolutionResult {
    invoice_id: Uuid,
    outcome: MarketplaceResolutionOutcome,
    resolved_at: OffsetDateTime,
    replayed: bool,
}

impl MarketplaceResolutionResult {
    /// Builds a result returned by an injected persistence adapter.
    pub fn new(
        invoice_id: Uuid,
        outcome: MarketplaceResolutionOutcome,
        resolved_at: OffsetDateTime,
        replayed: bool,
    ) -> Self {
        Self {
            invoice_id,
            outcome,
            resolved_at,
            replayed,
        }
    }

    pub fn invoice_id(&self) -> Uuid {
        self.invoice_id
    }
    pub fn outcome(&self) -> MarketplaceResolutionOutcome {
        self.outcome
    }
    pub fn resolved_at(&self) -> OffsetDateTime {
        self.resolved_at
    }
    pub fn replayed(&self) -> bool {
        self.replayed
    }
}

/// PostgreSQL-backed Marketplace preparation store.
#[derive(Clone, Debug)]
pub struct MarketplacePreparationStore {
    pool: PgPool,
    crypto: Arc<Crypto>,
}

#[derive(Serialize, Deserialize)]
struct PreparationEnvelopeV1 {
    version: u8,
    operation_id: String,
    request_binding: Vec<u8>,
    reader: String,
    reference: String,
    total_sats: u64,
    payment_window_seconds: u64,
    prepare_ttl_seconds: u64,
    prepared_at: OffsetDateTime,
    prepare_expires_at: OffsetDateTime,
    child_index: i64,
    payment_request_intent: DeliveryIntentV1,
}

struct CreatorRow {
    id: Uuid,
    creator_lookup_hash: Vec<u8>,
    next_child_index: i64,
}

struct PreparationRow {
    id: Uuid,
    operation_lookup_hash: Vec<u8>,
    request_lookup_hash: Vec<u8>,
    reader_lookup_hash: Vec<u8>,
    preparation_envelope: Vec<u8>,
    prepared_at: OffsetDateTime,
    prepare_expires_at: OffsetDateTime,
}

struct LifecycleRow {
    id: Uuid,
    creator_lookup_hash: Vec<u8>,
    preparation_envelope: Vec<u8>,
    state: String,
    prepared_at: OffsetDateTime,
    prepare_expires_at: OffsetDateTime,
    activated_at: Option<OffsetDateTime>,
    payment_deadline: Option<OffsetDateTime>,
    voided_at: Option<OffsetDateTime>,
    business_outcome: Option<String>,
    resolved_at: Option<OffsetDateTime>,
}

impl MarketplacePreparationStore {
    pub fn new(pool: &PgPool, crypto: Arc<Crypto>) -> Self {
        Self {
            pool: pool.clone(),
            crypto,
        }
    }

    /// Resolves replay or changed binding before mutable external work.
    pub async fn preflight(
        &self,
        creator: &CreatorPubky,
        operation_id: &str,
        request_binding: &[u8],
    ) -> Result<MarketplacePreparationPreflight, PersistenceError> {
        if operation_id.is_empty() || request_binding.is_empty() {
            return Err(PersistenceError::InvalidInput);
        }
        let creator_hash = self.crypto.lookup_hash(creator.to_string().as_bytes());
        let operation_hash = self.crypto.lookup_hash(operation_id.as_bytes());
        let request_hash = self.crypto.lookup_hash(request_binding);
        let stored: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT preparations.request_lookup_hash
             FROM marketplace_payment_preparations AS preparations
             JOIN creators ON creators.id = preparations.creator_id
             WHERE creators.creator_lookup_hash = $1
               AND preparations.operation_lookup_hash = $2",
        )
        .bind(creator_hash.as_bytes().as_slice())
        .bind(operation_hash.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        Ok(match stored {
            None => MarketplacePreparationPreflight::New,
            Some(stored) if stored_hash(&stored)? == request_hash => {
                MarketplacePreparationPreflight::ExactReplay
            }
            Some(_) => MarketplacePreparationPreflight::Conflict,
        })
    }

    /// Creates or exactly replays one Creator-scoped Marketplace operation.
    pub async fn prepare(
        &self,
        input: MarketplacePreparationInput<'_>,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        if input.operation_id.is_empty()
            || input.request_binding.is_empty()
            || input.total_sats == 0
            || input.payment_window_seconds == 0
            || input.prepare_ttl_seconds == 0
        {
            return Err(PersistenceError::InvalidInput);
        }
        let ttl_seconds =
            i64::try_from(input.prepare_ttl_seconds).map_err(|_| PersistenceError::InvalidInput)?;
        let ttl = Duration::seconds(ttl_seconds);
        let creator_hash = self
            .crypto
            .lookup_hash(input.creator.to_string().as_bytes());
        let operation_hash = self.crypto.lookup_hash(input.operation_id.as_bytes());
        let request_hash = self.crypto.lookup_hash(input.request_binding);
        let reader_hash = self.crypto.lookup_hash(input.reader.to_string().as_bytes());

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let creator = sqlx::query_as::<_, (Uuid, Vec<u8>, i64)>(
            "SELECT id, creator_lookup_hash, next_child_index
             FROM creators WHERE creator_lookup_hash = $1 FOR UPDATE",
        )
        .bind(creator_hash.as_bytes().as_slice())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?
        .map(|(id, creator_lookup_hash, next_child_index)| CreatorRow {
            id,
            creator_lookup_hash,
            next_child_index,
        })
        .ok_or(PersistenceError::CorruptOrMissing)?;
        if stored_hash(&creator.creator_lookup_hash)? != creator_hash
            || creator.next_child_index < 0
        {
            return Err(PersistenceError::CorruptOrMissing);
        }

        if let Some(row) = self
            .load_locked(&mut tx, creator.id, operation_hash)
            .await?
        {
            let result = self.validate_replay(
                creator_hash,
                operation_hash,
                request_hash,
                reader_hash,
                input,
                row,
            )?;
            tx.commit()
                .await
                .map_err(|_| PersistenceError::Unavailable)?;
            return Ok(result);
        }

        let prepared_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let prepare_expires_at = prepared_at
            .checked_add(ttl)
            .ok_or(PersistenceError::InvalidInput)?;
        let payloads = input.payloads.for_child_index(creator.next_child_index)?;
        validate_intent(
            &payloads.payment_request_intent,
            input.reader,
            input.reference,
            input.total_sats,
        )?;
        let invoice_id = Uuid::new_v4();
        let envelope = PreparationEnvelopeV1 {
            version: 1,
            operation_id: input.operation_id.to_owned(),
            request_binding: input.request_binding.to_vec(),
            reader: input.reader.to_string(),
            reference: input.reference.to_owned(),
            total_sats: input.total_sats,
            payment_window_seconds: input.payment_window_seconds,
            prepare_ttl_seconds: input.prepare_ttl_seconds,
            prepared_at,
            prepare_expires_at,
            child_index: creator.next_child_index,
            payment_request_intent: payloads.payment_request_intent,
        };
        let plaintext =
            postcard::to_allocvec(&envelope).map_err(|_| PersistenceError::CorruptOrMissing)?;
        let encrypted = self
            .crypto
            .encrypt(
                &EnvelopeContext::marketplace_preparation(creator_hash, invoice_id),
                &plaintext,
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        sqlx::query(
            "INSERT INTO marketplace_payment_preparations
             (id, creator_id, operation_lookup_hash, request_lookup_hash,
              reader_lookup_hash, preparation_envelope, prepared_at, prepare_expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(invoice_id)
        .bind(creator.id)
        .bind(operation_hash.as_bytes().as_slice())
        .bind(request_hash.as_bytes().as_slice())
        .bind(reader_hash.as_bytes().as_slice())
        .bind(encrypted.as_bytes())
        .bind(prepared_at)
        .bind(prepare_expires_at)
        .execute(&mut *tx)
        .await
        .map_err(classify_preparation_insert_error)?;
        sqlx::query(
            "UPDATE creators SET next_child_index = next_child_index + 1, updated_at = NOW()
             WHERE id = $1",
        )
        .bind(creator.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        tx.commit()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        Ok(MarketplacePreparationResult {
            invoice_id,
            total_sats: input.total_sats,
            prepared_at,
            prepare_expires_at,
            replayed: false,
        })
    }

    /// Atomically activates a preparation and admits exactly one proposal intent.
    pub async fn activate(
        &self,
        input: MarketplaceActivationInput<'_>,
    ) -> Result<Option<MarketplaceActivationResult>, MarketplaceLifecyclePersistenceError> {
        if input.total_sats == 0 || input.proposal_acceptance_window.is_zero() {
            return Err(PersistenceError::InvalidInput.into());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let Some(row) = self
            .load_lifecycle_locked(&mut tx, input.creator, input.invoice_id)
            .await?
        else {
            tx.commit()
                .await
                .map_err(|_| PersistenceError::Unavailable)?;
            return Ok(None);
        };
        let creator_hash = stored_hash(&row.creator_lookup_hash)?;
        let mut envelope = self.decode_lifecycle_envelope(creator_hash, &row)?;
        match row.state.as_str() {
            "active" => {
                if envelope.total_sats != input.total_sats {
                    return Err(MarketplaceLifecyclePersistenceError::TotalMismatch);
                }
                let (Some(activated_at), Some(payment_deadline)) =
                    (row.activated_at, row.payment_deadline)
                else {
                    return Err(PersistenceError::CorruptOrMissing.into());
                };
                let outbox_count: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM outbox
                     WHERE marketplace_preparation_id = $1
                       AND creator_id = (
                           SELECT creator_id FROM marketplace_payment_preparations WHERE id = $1
                       )
                       AND intent_kind = 'payment_request_proposal'",
                )
                .bind(row.id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| PersistenceError::Unavailable)?;
                if outbox_count != 1 {
                    return Err(PersistenceError::CorruptOrMissing.into());
                }
                self.validate_settlement(&mut tx, creator_hash, &row, &envelope)
                    .await?;
                tx.commit()
                    .await
                    .map_err(|_| PersistenceError::Unavailable)?;
                return Ok(Some(MarketplaceActivationResult {
                    invoice_id: row.id,
                    activated_at,
                    payment_deadline,
                    total_sats: envelope.total_sats,
                    replayed: true,
                }));
            }
            "prepared" => {}
            "voided" => return Err(MarketplaceLifecyclePersistenceError::LifecycleTerminal),
            _ => return Err(PersistenceError::CorruptOrMissing.into()),
        }
        if row.activated_at.is_some()
            || row.payment_deadline.is_some()
            || row.voided_at.is_some()
            || row.resolved_at.is_some()
        {
            return Err(if row.resolved_at.is_some() {
                MarketplaceLifecyclePersistenceError::LifecycleTerminal
            } else {
                PersistenceError::CorruptOrMissing.into()
            });
        }
        let now: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        if now >= row.prepare_expires_at {
            return Err(MarketplaceLifecyclePersistenceError::PrepareExpired);
        }
        if envelope.total_sats != input.total_sats {
            return Err(MarketplaceLifecyclePersistenceError::TotalMismatch);
        }
        let window_seconds = i64::try_from(envelope.payment_window_seconds)
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let payment_deadline = now
            .checked_add(Duration::seconds(window_seconds))
            .ok_or(PersistenceError::InvalidInput)?;
        let configured_proposal_duration = Duration::try_from(input.proposal_acceptance_window)
            .map_err(|_| PersistenceError::InvalidInput)?;
        let proposal_duration =
            configured_proposal_duration.min(Duration::seconds(window_seconds) / 2);
        let proposal_expires_at = now
            .checked_add(proposal_duration)
            .ok_or(PersistenceError::InvalidInput)?;
        envelope
            .payment_request_intent
            .set_deadlines(
                proposal_expires_at
                    .format(&Rfc3339)
                    .map_err(|_| PersistenceError::InvalidInput)?,
                payment_deadline
                    .format(&Rfc3339)
                    .map_err(|_| PersistenceError::InvalidInput)?,
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        validate_intent(
            &envelope.payment_request_intent,
            &parse_reader(&envelope.reader).map_err(|_| PersistenceError::CorruptOrMissing)?,
            &envelope.reference,
            envelope.total_sats,
        )?;
        let outbox_id = Uuid::new_v4();
        let intent_plaintext = postcard::to_allocvec(&envelope.payment_request_intent)
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let intent_envelope = self
            .crypto
            .encrypt(
                &EnvelopeContext::outbox_semantic_intent(creator_hash, outbox_id),
                &intent_plaintext,
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let proposal_lookup_hash = self.crypto.payment_request_proposal_lookup_hash(
            envelope
                .payment_request_intent
                .proposal_payment_reference()
                .map_err(|_| PersistenceError::CorruptOrMissing)?
                .as_bytes(),
        );
        sqlx::query(
            "UPDATE marketplace_payment_preparations
             SET state = 'active', activated_at = $2, payment_deadline = $3,
                 updated_at = clock_timestamp()
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(now)
        .bind(payment_deadline)
        .execute(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        let payment_record = InvoicePaymentRecordV1::from_intent(
            envelope.child_index,
            &envelope.payment_request_intent,
        )?;
        let bitcoin_address_hash = self
            .crypto
            .bitcoin_address_lookup_hash(payment_record.bitcoin_address()?.as_bytes());
        let derivation_index_hash = self
            .crypto
            .bitcoin_derivation_index_lookup_hash(creator_hash, payment_record.derivation_index());
        let payment_record_plaintext = postcard::to_allocvec(&payment_record)
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let payment_record_envelope = self
            .crypto
            .encrypt(
                &EnvelopeContext::marketplace_settlement_payment_record(creator_hash, row.id),
                &payment_record_plaintext,
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        sqlx::query(
            "INSERT INTO marketplace_settlements (
                 preparation_id, creator_id, payment_record_envelope,
                 bitcoin_address_lookup_hash, derivation_index_lookup_hash,
                 payment_status, confirmation_count, amount_matched
             )
             SELECT id, creator_id, $2, $3, $4, 'undetected', 0, FALSE
             FROM marketplace_payment_preparations WHERE id = $1",
        )
        .bind(row.id)
        .bind(payment_record_envelope.as_bytes())
        .bind(bitcoin_address_hash.as_bytes().as_slice())
        .bind(derivation_index_hash.as_bytes().as_slice())
        .execute(&mut *tx)
        .await
        .map_err(classify_settlement_insert_error)?;
        let inserted = sqlx::query(
            "INSERT INTO outbox
             (id, creator_id, marketplace_preparation_id, intent_envelope, intent_kind,
              status, proposal_lookup_hash)
             SELECT $1, creator_id, id, $2, 'payment_request_proposal', 'queued', $3
             FROM marketplace_payment_preparations WHERE id = $4",
        )
        .bind(outbox_id)
        .bind(intent_envelope.as_bytes())
        .bind(proposal_lookup_hash.as_bytes().as_slice())
        .bind(row.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        if inserted.rows_affected() != 1 {
            return Err(PersistenceError::CorruptOrMissing.into());
        }
        tx.commit()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        Ok(Some(MarketplaceActivationResult {
            invoice_id: row.id,
            activated_at: now,
            payment_deadline,
            total_sats: envelope.total_sats,
            replayed: false,
        }))
    }

    /// Atomically voids a still-prepared invoice without creating SDK work.
    pub async fn void(
        &self,
        creator: &CreatorPubky,
        invoice_id: Uuid,
    ) -> Result<Option<MarketplaceVoidResult>, MarketplaceLifecyclePersistenceError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let Some(row) = self
            .load_lifecycle_locked(&mut tx, creator, invoice_id)
            .await?
        else {
            tx.commit()
                .await
                .map_err(|_| PersistenceError::Unavailable)?;
            return Ok(None);
        };
        let creator_hash = stored_hash(&row.creator_lookup_hash)?;
        self.decode_lifecycle_envelope(creator_hash, &row)?;
        match row.state.as_str() {
            "voided" => {
                let Some(voided_at) = row.voided_at else {
                    return Err(PersistenceError::CorruptOrMissing.into());
                };
                tx.commit()
                    .await
                    .map_err(|_| PersistenceError::Unavailable)?;
                return Ok(Some(MarketplaceVoidResult {
                    invoice_id: row.id,
                    voided_at,
                    replayed: true,
                }));
            }
            "prepared" => {}
            "active" => return Err(MarketplaceLifecyclePersistenceError::LifecycleTerminal),
            _ => return Err(PersistenceError::CorruptOrMissing.into()),
        }
        if row.activated_at.is_some() || row.payment_deadline.is_some() || row.voided_at.is_some() {
            return Err(PersistenceError::CorruptOrMissing.into());
        }
        let voided_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        sqlx::query(
            "UPDATE marketplace_payment_preparations
             SET state = 'voided', voided_at = $2, updated_at = clock_timestamp()
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(voided_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        tx.commit()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        Ok(Some(MarketplaceVoidResult {
            invoice_id: row.id,
            voided_at,
            replayed: false,
        }))
    }

    /// Records one immutable business outcome without changing protocol or payment facts.
    pub async fn resolve(
        &self,
        creator: &CreatorPubky,
        invoice_id: Uuid,
        outcome: MarketplaceResolutionOutcome,
    ) -> Result<Option<MarketplaceResolutionResult>, MarketplaceLifecyclePersistenceError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let Some(row) = self
            .load_lifecycle_locked(&mut tx, creator, invoice_id)
            .await?
        else {
            tx.commit()
                .await
                .map_err(|_| PersistenceError::Unavailable)?;
            return Ok(None);
        };
        let creator_hash = stored_hash(&row.creator_lookup_hash)?;
        self.decode_lifecycle_envelope(creator_hash, &row)?;
        if !matches!(row.state.as_str(), "prepared" | "active" | "voided") {
            return Err(PersistenceError::CorruptOrMissing.into());
        }
        match (&row.business_outcome, row.resolved_at) {
            (Some(existing), Some(resolved_at)) => {
                let existing = MarketplaceResolutionOutcome::parse(existing)
                    .ok_or(PersistenceError::CorruptOrMissing)?;
                if existing != outcome {
                    return Err(MarketplaceLifecyclePersistenceError::ResolutionConflict);
                }
                tx.commit()
                    .await
                    .map_err(|_| PersistenceError::Unavailable)?;
                return Ok(Some(MarketplaceResolutionResult::new(
                    row.id,
                    existing,
                    resolved_at,
                    true,
                )));
            }
            (None, None) => {}
            _ => return Err(PersistenceError::CorruptOrMissing.into()),
        }
        let resolved_at: OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let updated = sqlx::query(
            "UPDATE marketplace_payment_preparations
             SET business_outcome = $2, resolved_at = $3, updated_at = clock_timestamp()
             WHERE id = $1 AND business_outcome IS NULL AND resolved_at IS NULL",
        )
        .bind(row.id)
        .bind(outcome.as_str())
        .bind(resolved_at)
        .execute(&mut *tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(PersistenceError::CorruptOrMissing.into());
        }
        tx.commit()
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        Ok(Some(MarketplaceResolutionResult::new(
            row.id,
            outcome,
            resolved_at,
            false,
        )))
    }

    async fn load_locked(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        creator_id: Uuid,
        operation_hash: LookupHash,
    ) -> Result<Option<PreparationRow>, PersistenceError> {
        sqlx::query_as::<
            _,
            (
                Uuid,
                Vec<u8>,
                Vec<u8>,
                Vec<u8>,
                Vec<u8>,
                OffsetDateTime,
                OffsetDateTime,
            ),
        >(
            "SELECT id, operation_lookup_hash, request_lookup_hash, reader_lookup_hash,
                    preparation_envelope, prepared_at, prepare_expires_at
             FROM marketplace_payment_preparations
             WHERE creator_id = $1 AND operation_lookup_hash = $2 FOR UPDATE",
        )
        .bind(creator_id)
        .bind(operation_hash.as_bytes().as_slice())
        .fetch_optional(&mut **tx)
        .await
        .map(|row| {
            row.map(
                |(
                    id,
                    operation_lookup_hash,
                    request_lookup_hash,
                    reader_lookup_hash,
                    preparation_envelope,
                    prepared_at,
                    prepare_expires_at,
                )| PreparationRow {
                    id,
                    operation_lookup_hash,
                    request_lookup_hash,
                    reader_lookup_hash,
                    preparation_envelope,
                    prepared_at,
                    prepare_expires_at,
                },
            )
        })
        .map_err(|_| PersistenceError::Unavailable)
    }

    async fn load_lifecycle_locked(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        creator: &CreatorPubky,
        invoice_id: Uuid,
    ) -> Result<Option<LifecycleRow>, PersistenceError> {
        let creator_hash = self.crypto.lookup_hash(creator.to_string().as_bytes());
        sqlx::query_as::<
            _,
            (
                Uuid,
                Vec<u8>,
                Vec<u8>,
                String,
                OffsetDateTime,
                OffsetDateTime,
                Option<OffsetDateTime>,
                Option<OffsetDateTime>,
                Option<OffsetDateTime>,
                Option<String>,
                Option<OffsetDateTime>,
            ),
        >(
            "SELECT preparation.id, creators.creator_lookup_hash,
                    preparation.preparation_envelope, preparation.state,
                    preparation.prepared_at, preparation.prepare_expires_at,
                    preparation.activated_at, preparation.payment_deadline,
                    preparation.voided_at, preparation.business_outcome,
                    preparation.resolved_at
             FROM marketplace_payment_preparations AS preparation
             JOIN creators ON creators.id = preparation.creator_id
             WHERE preparation.id = $1 AND creators.creator_lookup_hash = $2
             FOR UPDATE OF preparation",
        )
        .bind(invoice_id)
        .bind(creator_hash.as_bytes().as_slice())
        .fetch_optional(&mut **tx)
        .await
        .map(|row| {
            row.map(
                |(
                    id,
                    creator_lookup_hash,
                    preparation_envelope,
                    state,
                    prepared_at,
                    prepare_expires_at,
                    activated_at,
                    payment_deadline,
                    voided_at,
                    business_outcome,
                    resolved_at,
                )| LifecycleRow {
                    id,
                    creator_lookup_hash,
                    preparation_envelope,
                    state,
                    prepared_at,
                    prepare_expires_at,
                    activated_at,
                    payment_deadline,
                    voided_at,
                    business_outcome,
                    resolved_at,
                },
            )
        })
        .map_err(|_| PersistenceError::Unavailable)
    }

    fn decode_lifecycle_envelope(
        &self,
        creator_hash: LookupHash,
        row: &LifecycleRow,
    ) -> Result<PreparationEnvelopeV1, PersistenceError> {
        let plaintext = self
            .crypto
            .decrypt(
                &EnvelopeContext::marketplace_preparation(creator_hash, row.id),
                &EncryptedEnvelope::from_bytes(row.preparation_envelope.clone()),
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let envelope: PreparationEnvelopeV1 =
            postcard::from_bytes(&plaintext).map_err(|_| PersistenceError::CorruptOrMissing)?;
        let ttl = i64::try_from(envelope.prepare_ttl_seconds)
            .map(Duration::seconds)
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        if envelope.version != 1
            || envelope.prepared_at != row.prepared_at
            || envelope.prepare_expires_at != row.prepare_expires_at
            || envelope.prepared_at.checked_add(ttl) != Some(envelope.prepare_expires_at)
            || envelope.payment_window_seconds == 0
            || envelope.total_sats == 0
        {
            return Err(PersistenceError::CorruptOrMissing);
        }
        validate_intent(
            &envelope.payment_request_intent,
            &parse_reader(&envelope.reader).map_err(|_| PersistenceError::CorruptOrMissing)?,
            &envelope.reference,
            envelope.total_sats,
        )
        .map_err(|_| PersistenceError::CorruptOrMissing)?;
        Ok(envelope)
    }

    async fn validate_settlement(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        creator_hash: LookupHash,
        row: &LifecycleRow,
        preparation: &PreparationEnvelopeV1,
    ) -> Result<(), PersistenceError> {
        let stored: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT payment_record_envelope, bitcoin_address_lookup_hash,
                    derivation_index_lookup_hash
             FROM marketplace_settlements
             WHERE preparation_id = $1 AND creator_id = (
                 SELECT creator_id FROM marketplace_payment_preparations WHERE id = $1
             )",
        )
        .bind(row.id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        let Some((encrypted, address_hash, derivation_hash)) = stored else {
            return Err(PersistenceError::CorruptOrMissing);
        };
        let plaintext = self
            .crypto
            .decrypt(
                &EnvelopeContext::marketplace_settlement_payment_record(creator_hash, row.id),
                &EncryptedEnvelope::from_bytes(encrypted),
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let actual: InvoicePaymentRecordV1 =
            postcard::from_bytes(&plaintext).map_err(|_| PersistenceError::CorruptOrMissing)?;
        let expected = InvoicePaymentRecordV1::from_intent(
            preparation.child_index,
            &preparation.payment_request_intent,
        )
        .map_err(|_| PersistenceError::CorruptOrMissing)?;
        if actual != expected
            || address_hash.as_slice()
                != self
                    .crypto
                    .bitcoin_address_lookup_hash(expected.bitcoin_address()?.as_bytes())
                    .as_bytes()
            || derivation_hash.as_slice()
                != self
                    .crypto
                    .bitcoin_derivation_index_lookup_hash(creator_hash, expected.derivation_index())
                    .as_bytes()
        {
            return Err(PersistenceError::CorruptOrMissing);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_replay(
        &self,
        creator_hash: LookupHash,
        operation_hash: LookupHash,
        request_hash: LookupHash,
        reader_hash: LookupHash,
        input: MarketplacePreparationInput<'_>,
        row: PreparationRow,
    ) -> Result<MarketplacePreparationResult, PersistenceError> {
        if stored_hash(&row.operation_lookup_hash)? != operation_hash {
            return Err(PersistenceError::CorruptOrMissing);
        }
        if stored_hash(&row.request_lookup_hash)? != request_hash {
            return Err(PersistenceError::Conflict);
        }
        if stored_hash(&row.reader_lookup_hash)? != reader_hash {
            return Err(PersistenceError::CorruptOrMissing);
        }
        let plaintext = self
            .crypto
            .decrypt(
                &EnvelopeContext::marketplace_preparation(creator_hash, row.id),
                &EncryptedEnvelope::from_bytes(row.preparation_envelope),
            )
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        let envelope: PreparationEnvelopeV1 =
            postcard::from_bytes(&plaintext).map_err(|_| PersistenceError::CorruptOrMissing)?;
        let stored_ttl = i64::try_from(envelope.prepare_ttl_seconds)
            .map(Duration::seconds)
            .map_err(|_| PersistenceError::CorruptOrMissing)?;
        if envelope.version != 1
            || envelope.operation_id != input.operation_id
            || envelope.request_binding != input.request_binding
            || envelope.reader != input.reader.to_string()
            || envelope.reference != input.reference
            || envelope.total_sats != input.total_sats
            || envelope.payment_window_seconds != input.payment_window_seconds
            || envelope.prepared_at != row.prepared_at
            || envelope.prepare_expires_at != row.prepare_expires_at
            || envelope.prepared_at.checked_add(stored_ttl) != Some(envelope.prepare_expires_at)
            || envelope.child_index < 0
        {
            return Err(PersistenceError::CorruptOrMissing);
        }
        validate_intent(
            &envelope.payment_request_intent,
            input.reader,
            input.reference,
            input.total_sats,
        )?;
        Ok(MarketplacePreparationResult {
            invoice_id: row.id,
            total_sats: envelope.total_sats,
            prepared_at: row.prepared_at,
            prepare_expires_at: row.prepare_expires_at,
            replayed: true,
        })
    }
}

fn validate_intent(
    intent: &DeliveryIntentV1,
    reader: &ReaderPubky,
    reference: &str,
    total_sats: u64,
) -> Result<(), PersistenceError> {
    intent
        .validate()
        .map_err(|_| PersistenceError::InvalidInput)?;
    if parse_reader(intent.reader_pubky()).ok().as_ref() != Some(reader) {
        return Err(PersistenceError::InvalidInput);
    }
    let terms = intent.terms().map_err(|_| PersistenceError::InvalidInput)?;
    let expected = format!(
        "{}.{:08}",
        total_sats / 100_000_000,
        total_sats % 100_000_000
    );
    if terms.asset != "btc" || terms.amount != expected || terms.payment_reference != reference {
        return Err(PersistenceError::InvalidInput);
    }
    Ok(())
}

fn stored_hash(bytes: &[u8]) -> Result<LookupHash, PersistenceError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| PersistenceError::CorruptOrMissing)?;
    Ok(LookupHash::from_bytes(bytes))
}

fn classify_preparation_insert_error(error: sqlx::Error) -> PersistenceError {
    match error {
        sqlx::Error::Database(error)
            if error.is_unique_violation()
                && error.constraint() == Some(OPERATION_BINDING_UNIQUE_CONSTRAINT) =>
        {
            PersistenceError::Conflict
        }
        sqlx::Error::Database(_) => PersistenceError::CorruptOrMissing,
        _ => PersistenceError::Unavailable,
    }
}

fn classify_settlement_insert_error(error: sqlx::Error) -> PersistenceError {
    match error {
        sqlx::Error::Database(_) => PersistenceError::CorruptOrMissing,
        _ => PersistenceError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_transport_failure_is_unavailable() {
        assert_eq!(
            classify_preparation_insert_error(sqlx::Error::PoolClosed),
            PersistenceError::Unavailable
        );
    }
}
