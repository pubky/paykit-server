use paykit_sdk::{
    ContactRecord, ContactUpdate, LinkedPeerState, PaykitProfile, PaykitSdkError, PubkyPublicKey,
    PublicationStatus, StorageAdapter, StorageTransaction,
};

use super::PaykitAdapter;

impl PaykitAdapter {
    /// Saves a verified buyer privately, preferring their Paykit profile to Pubky.app.
    /// Missing profiles, existing contacts and blocked peers are terminal no-ops.
    pub async fn save_buyer_contact(&self, buyer: PubkyPublicKey) -> paykit_sdk::Result<()> {
        let owner = PubkyPublicKey::from_raw_or_app_key(self.creator.to_string())?;
        if buyer == owner {
            return Ok(());
        }
        let Some(resolved) = self.sdk.resolve_profile(buyer.clone(), true).await? else {
            return Ok(());
        };
        let profile = resolved.paykit_profile.unwrap_or(PaykitProfile {
            display_name: resolved.display_name,
            image_uri: resolved.image_uri,
            extra: None,
        });
        profile.validate()?;
        let update = ContactUpdate {
            public_key: buyer,
            label: profile
                .display_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned),
        };
        update.validate()?;
        let now = std::time::SystemTime::now().into();
        let contact = ContactRecord {
            public_key: update.public_key,
            label: update.label,
            profile: Some(profile),
            profile_fetched_at: Some(now),
            created_at: now,
            updated_at: now,
            public_contact_marker_status: PublicationStatus::NotPublished,
            public_contact_published_at: None,
            public_contact_removed_at: None,
            public_contact_last_error: None,
        };
        // Recheck after the public read, in the same transaction as the insert.
        // Never overwrite a wallet edit, publish a public marker or unblock a peer.
        let _guard = self.mutation_lock.lock().await;
        self.storage
            .transaction(move |tx| insert_buyer_contact(tx, &owner, contact))
            .await
    }
}

fn insert_buyer_contact(
    tx: &mut dyn StorageTransaction,
    owner: &PubkyPublicKey,
    contact: ContactRecord,
) -> paykit_sdk::Result<()> {
    if tx
        .load_identity_state()
        .and_then(|identity| identity.public_key)
        .as_ref()
        != Some(owner)
    {
        return Err(PaykitSdkError::Identity {
            context: "buyer contact identity does not match Creator".into(),
            source: None,
        });
    }
    if &contact.public_key == owner
        || tx.contact_record(&contact.public_key).is_some()
        || tx
            .linked_peer(&contact.public_key)
            .is_some_and(|peer| peer.state == LinkedPeerState::Blocked)
    {
        return Ok(());
    }
    tx.save_contact_record(contact);
    Ok(())
}
