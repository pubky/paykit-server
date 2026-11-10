//! The Payment Request attempts each USDT receipt was verified for.
//!
//! A receipt is verified against one exact Payment Request record at a time.
//! Every attempt it is verified for is attested once, append-only, sealed to
//! the receipt row and its owner, while the receipt itself keeps its one
//! owner. The receipt pays when the proposal record time of any attested
//! attempt bounds it; a receipt with no attested attempt pays nothing.

use std::collections::HashMap;

use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::PersistenceError;
use crate::crypto::{Crypto, EncryptedEnvelope, EnvelopeContext, LookupHash};

/// The invoice or Marketplace preparation that owns a USDT receipt.
#[derive(Clone, Copy)]
pub(super) enum ReceiptOwner {
    Invoice(Uuid),
    Preparation(Uuid),
}

impl ReceiptOwner {
    fn id(self) -> Uuid {
        match self {
            Self::Invoice(id) | Self::Preparation(id) => id,
        }
    }
}

/// The keyed lookup hash that makes each attestation of an attempt unique.
pub(super) fn attempt_hash(crypto: &Crypto, payment_request_id: &str) -> LookupHash {
    let mut logical = b"usdt-observation-attempt\0".to_vec();
    logical.extend_from_slice(payment_request_id.as_bytes());
    crypto.lookup_hash(&logical)
}

/// Attests that receipt `observation_id` verified for `payment_request_id`.
/// An attempt already attested is left as it is.
pub(super) async fn attest(
    tx: &mut Transaction<'_, Postgres>,
    crypto: &Crypto,
    creator_hash: LookupHash,
    observation_id: Uuid,
    owner: ReceiptOwner,
    payment_request_id: &str,
) -> Result<(), PersistenceError> {
    let envelope = crypto
        .encrypt(
            &EnvelopeContext::usdt_observation_attempt(creator_hash, observation_id, owner.id()),
            payment_request_id.as_bytes(),
        )
        .map_err(|_| PersistenceError::CorruptOrMissing)?;
    sqlx::query(
        "INSERT INTO usdt_observation_attempts (observation_id, attempt_lookup_hash, attempt_envelope)
         VALUES ($1, $2, $3)
         ON CONFLICT (observation_id, attempt_lookup_hash) DO NOTHING",
    )
    .bind(observation_id)
    .bind(
        attempt_hash(crypto, payment_request_id)
            .as_bytes()
            .as_slice(),
    )
    .bind(envelope.as_bytes())
    .execute(&mut **tx)
    .await
    .map_err(|_| PersistenceError::Unavailable)?;
    Ok(())
}

fn open(
    crypto: &Crypto,
    creator_hash: LookupHash,
    observation_id: Uuid,
    owner: ReceiptOwner,
    lookup_hash: &[u8],
    envelope: Vec<u8>,
) -> Result<String, PersistenceError> {
    let plaintext = crypto
        .decrypt(
            &EnvelopeContext::usdt_observation_attempt(creator_hash, observation_id, owner.id()),
            &EncryptedEnvelope::from_bytes(envelope),
        )
        .map_err(|_| PersistenceError::CorruptOrMissing)?;
    let attempt = String::from_utf8(plaintext).map_err(|_| PersistenceError::CorruptOrMissing)?;
    if attempt_hash(crypto, &attempt).as_bytes() != lookup_hash {
        return Err(PersistenceError::CorruptOrMissing);
    }
    Ok(attempt)
}

/// The attempts attested for one receipt.
async fn attested(
    tx: &mut Transaction<'_, Postgres>,
    crypto: &Crypto,
    creator_hash: LookupHash,
    observation_id: Uuid,
    owner: ReceiptOwner,
) -> Result<Vec<String>, PersistenceError> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT attempt_lookup_hash, attempt_envelope FROM usdt_observation_attempts
         WHERE observation_id = $1 ORDER BY attempt_lookup_hash",
    )
    .bind(observation_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| PersistenceError::Unavailable)?;
    rows.into_iter()
        .map(|(lookup_hash, envelope)| {
            open(
                crypto,
                creator_hash,
                observation_id,
                owner,
                &lookup_hash,
                envelope,
            )
        })
        .collect()
}

/// When a receipt's proposal-time bound starts.
pub(super) struct ReceiptBound {
    /// The earliest recorded proposal time among its attested attempts: any
    /// attested attempt's own window accepting the receipt pays.
    pub(super) proposal_recorded_at: Option<OffsetDateTime>,
    /// Whether an attested attempt's proposal time is not recorded yet.
    pub(super) pending: bool,
}

/// Proposal record times of one owner's attempts, read once per attempt.
pub(super) struct ProposalTimes {
    owner: ReceiptOwner,
    known: HashMap<String, Option<OffsetDateTime>>,
}

impl ProposalTimes {
    pub(super) fn new(owner: ReceiptOwner) -> Self {
        Self {
            owner,
            known: HashMap::new(),
        }
    }

    /// The bound of receipt `observation_id` from its attested attempts.
    pub(super) async fn bound(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        crypto: &Crypto,
        creator_hash: LookupHash,
        observation_id: Uuid,
    ) -> Result<ReceiptBound, PersistenceError> {
        let mut bound = ReceiptBound {
            proposal_recorded_at: None,
            pending: false,
        };
        for attempt in attested(tx, crypto, creator_hash, observation_id, self.owner).await? {
            match self.of(tx, &attempt).await? {
                Some(time) => {
                    bound.proposal_recorded_at = Some(
                        bound
                            .proposal_recorded_at
                            .map_or(time, |earliest| earliest.min(time)),
                    );
                }
                None => bound.pending = true,
            }
        }
        Ok(bound)
    }

    /// The proposal record time of `payment_request_id` on this owner, `None`
    /// while it is unknown or when the owner has no such attempt.
    async fn of(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        payment_request_id: &str,
    ) -> Result<Option<OffsetDateTime>, PersistenceError> {
        if let Some(known) = self.known.get(payment_request_id) {
            return Ok(*known);
        }
        let query = match self.owner {
            ReceiptOwner::Invoice(_) => {
                "SELECT proposal_recorded_at FROM payment_request_lifecycles
                 WHERE invoice_id = $1 AND sdk_payment_request_id = $2"
            }
            ReceiptOwner::Preparation(_) => {
                "SELECT proposal_recorded_at FROM payment_request_lifecycles
                 WHERE marketplace_preparation_id = $1 AND sdk_payment_request_id = $2"
            }
        };
        let time: Option<Option<OffsetDateTime>> = sqlx::query_scalar(query)
            .bind(self.owner.id())
            .bind(payment_request_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|_| PersistenceError::Unavailable)?;
        let time = time.flatten();
        self.known.insert(payment_request_id.to_owned(), time);
        Ok(time)
    }
}

fn owner_of(
    invoice_id: Option<Uuid>,
    preparation_id: Option<Uuid>,
) -> Result<ReceiptOwner, PersistenceError> {
    match (invoice_id, preparation_id) {
        (Some(invoice), None) => Ok(ReceiptOwner::Invoice(invoice)),
        (None, Some(preparation)) => Ok(ReceiptOwner::Preparation(preparation)),
        _ => Err(PersistenceError::CorruptOrMissing),
    }
}

fn stored_hash(bytes: Vec<u8>) -> Result<LookupHash, PersistenceError> {
    Ok(LookupHash::from_bytes(
        bytes
            .try_into()
            .map_err(|_| PersistenceError::CorruptOrMissing)?,
    ))
}

const RECEIPT_OWNERS: &str = "FROM usdt_observations observation
     LEFT JOIN invoices invoice ON invoice.id = observation.invoice_id
     LEFT JOIN marketplace_settlements settlement
       ON settlement.preparation_id = observation.marketplace_preparation_id
     JOIN creators ON creators.id = COALESCE(invoice.creator_id, settlement.creator_id)";

#[derive(sqlx::FromRow)]
struct StoredAttempt {
    observation_id: Uuid,
    invoice_id: Option<Uuid>,
    marketplace_preparation_id: Option<Uuid>,
    creator_lookup_hash: Vec<u8>,
    attempt_lookup_hash: Vec<u8>,
    attempt_envelope: Vec<u8>,
}

/// Authenticates every attestation against its receipt, owner and hash.
pub(super) async fn scan_integrity(pool: &PgPool, crypto: &Crypto) -> Result<(), PersistenceError> {
    let rows: Vec<StoredAttempt> = sqlx::query_as(&format!(
        "SELECT attempt.observation_id, observation.invoice_id,
                observation.marketplace_preparation_id, creators.creator_lookup_hash,
                attempt.attempt_lookup_hash, attempt.attempt_envelope
         {RECEIPT_OWNERS}
         JOIN usdt_observation_attempts attempt ON attempt.observation_id = observation.id"
    ))
    .fetch_all(pool)
    .await
    .map_err(|_| PersistenceError::Unavailable)?;
    for row in rows {
        open(
            crypto,
            stored_hash(row.creator_lookup_hash)?,
            row.observation_id,
            owner_of(row.invoice_id, row.marketplace_preparation_id)?,
            &row.attempt_lookup_hash,
            row.attempt_envelope,
        )?;
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct LegacyReceipt {
    id: Uuid,
    invoice_id: Option<Uuid>,
    marketplace_preparation_id: Option<Uuid>,
    creator_lookup_hash: Vec<u8>,
    sdk_payment_request_id: String,
}

/// Attests each receipt stored before attempts were recorded whose owner has
/// exactly one Payment Request attempt: receipts were only ever resolved
/// through their owner's attempts, so that attempt is the one it was verified
/// for. A receipt on an owner with several attempts gains attestations only
/// as it is verified again for each.
pub(super) async fn attribute_legacy_receipts(
    pool: &PgPool,
    crypto: &Crypto,
) -> Result<(), PersistenceError> {
    let rows: Vec<LegacyReceipt> = sqlx::query_as(&format!(
        "SELECT observation.id, observation.invoice_id, observation.marketplace_preparation_id,
                creators.creator_lookup_hash, lifecycle.sdk_payment_request_id
         {RECEIPT_OWNERS}
         JOIN payment_request_lifecycles lifecycle
           ON lifecycle.invoice_id = observation.invoice_id
           OR lifecycle.marketplace_preparation_id = observation.marketplace_preparation_id
         WHERE NOT EXISTS (
                   SELECT 1 FROM usdt_observation_attempts attempt
                   WHERE attempt.observation_id = observation.id)
           AND (SELECT COUNT(*) FROM payment_request_lifecycles other
                WHERE other.invoice_id = observation.invoice_id
                   OR other.marketplace_preparation_id = observation.marketplace_preparation_id) = 1"
    ))
    .fetch_all(pool)
    .await
    .map_err(|_| PersistenceError::Unavailable)?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
    for row in rows {
        attest(
            &mut tx,
            crypto,
            stored_hash(row.creator_lookup_hash)?,
            row.id,
            owner_of(row.invoice_id, row.marketplace_preparation_id)?,
            &row.sdk_payment_request_id,
        )
        .await?;
    }
    tx.commit().await.map_err(|_| PersistenceError::Unavailable)
}
