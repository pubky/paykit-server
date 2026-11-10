//! The Payment Request attempt each USDT receipt was verified for.
//!
//! A receipt is verified against one exact Payment Request record, and that
//! attempt's own proposal record time bounds the evidence. The attempt id is
//! sealed to the receipt row and its owner, written once and never changed.
//! A receipt with no attempt pays nothing.

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

pub(super) fn seal(
    crypto: &Crypto,
    creator_hash: LookupHash,
    row_id: Uuid,
    owner: ReceiptOwner,
    payment_request_id: &str,
) -> Result<Vec<u8>, PersistenceError> {
    crypto
        .encrypt(
            &EnvelopeContext::usdt_observation_attempt(creator_hash, row_id, owner.id()),
            payment_request_id.as_bytes(),
        )
        .map(|envelope| envelope.as_bytes().to_vec())
        .map_err(|_| PersistenceError::CorruptOrMissing)
}

pub(super) fn open(
    crypto: &Crypto,
    creator_hash: LookupHash,
    row_id: Uuid,
    owner: ReceiptOwner,
    envelope: Vec<u8>,
) -> Result<String, PersistenceError> {
    let plaintext = crypto
        .decrypt(
            &EnvelopeContext::usdt_observation_attempt(creator_hash, row_id, owner.id()),
            &EncryptedEnvelope::from_bytes(envelope),
        )
        .map_err(|_| PersistenceError::CorruptOrMissing)?;
    String::from_utf8(plaintext.to_vec()).map_err(|_| PersistenceError::CorruptOrMissing)
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

    /// The proposal record time of `payment_request_id` on this owner, `None`
    /// while it is unknown or when the owner has no such attempt.
    pub(super) async fn of(
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

#[derive(sqlx::FromRow)]
struct LegacyReceipt {
    id: Uuid,
    invoice_id: Option<Uuid>,
    marketplace_preparation_id: Option<Uuid>,
    creator_lookup_hash: Vec<u8>,
    sdk_payment_request_id: String,
}

/// Attributes each receipt stored before attempts were recorded whose owner
/// has exactly one Payment Request attempt: receipts were only ever resolved
/// through their owner's attempts, so that attempt is the one it was verified
/// for. A receipt on an owner with several attempts stays unattributed and
/// pays nothing.
pub(super) async fn attribute_legacy_receipts(
    pool: &PgPool,
    crypto: &Crypto,
) -> Result<(), PersistenceError> {
    let rows: Vec<LegacyReceipt> = sqlx::query_as(
        "SELECT observation.id, observation.invoice_id, observation.marketplace_preparation_id,
                creators.creator_lookup_hash, lifecycle.sdk_payment_request_id
         FROM usdt_observations observation
         JOIN payment_request_lifecycles lifecycle
           ON lifecycle.invoice_id = observation.invoice_id
           OR lifecycle.marketplace_preparation_id = observation.marketplace_preparation_id
         LEFT JOIN invoices invoice ON invoice.id = observation.invoice_id
         LEFT JOIN marketplace_settlements settlement
           ON settlement.preparation_id = observation.marketplace_preparation_id
         JOIN creators ON creators.id = COALESCE(invoice.creator_id, settlement.creator_id)
         WHERE observation.request_attempt_envelope IS NULL
           AND (SELECT COUNT(*) FROM payment_request_lifecycles attempt
                WHERE attempt.invoice_id = observation.invoice_id
                   OR attempt.marketplace_preparation_id = observation.marketplace_preparation_id) = 1",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| PersistenceError::Unavailable)?;
    for row in rows {
        let owner = match (row.invoice_id, row.marketplace_preparation_id) {
            (Some(invoice), None) => ReceiptOwner::Invoice(invoice),
            (None, Some(preparation)) => ReceiptOwner::Preparation(preparation),
            _ => return Err(PersistenceError::CorruptOrMissing),
        };
        let creator_hash = LookupHash::from_bytes(
            row.creator_lookup_hash
                .try_into()
                .map_err(|_| PersistenceError::CorruptOrMissing)?,
        );
        let envelope = seal(
            crypto,
            creator_hash,
            row.id,
            owner,
            &row.sdk_payment_request_id,
        )?;
        sqlx::query(
            "UPDATE usdt_observations SET request_attempt_envelope = $2
             WHERE id = $1 AND request_attempt_envelope IS NULL",
        )
        .bind(row.id)
        .bind(envelope)
        .execute(pool)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
    }
    Ok(())
}
