//! Readers a Creator has invoiced, the scope of the server's Allowance intake.

use super::*;

impl InvoiceStore {
    /// The candidates this Creator has invoiced, in their given order. Every
    /// Locks invoice records its Reader assignment in the same transaction, so
    /// a Reader without one never received a request from this server.
    pub async fn invoiced_readers(
        &self,
        creator_id: Uuid,
        candidates: &[ReaderPubky],
    ) -> Result<Vec<ReaderPubky>, PersistenceError> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let hashes = candidates
            .iter()
            .map(|reader| self.crypto.lookup_hash(reader.to_string().as_bytes()))
            .collect::<Vec<_>>();
        let invoiced: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT DISTINCT reader_lookup_hash FROM reader_assignments
             WHERE creator_id = $1 AND reader_lookup_hash = ANY($2)",
        )
        .bind(creator_id)
        .bind(
            hashes
                .iter()
                .map(|hash| hash.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|_| PersistenceError::Unavailable)?;
        Ok(candidates
            .iter()
            .zip(&hashes)
            .filter(|(_, hash)| {
                invoiced
                    .iter()
                    .any(|stored| stored.as_slice() == hash.as_bytes().as_slice())
            })
            .map(|(reader, _)| reader.clone())
            .collect())
    }
}
