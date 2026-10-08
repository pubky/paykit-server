use std::{sync::Arc, time::Duration};

use paykit_sdk::{
    ContactUpdate, PaykitProfile, PubkyLocalSecretKey, PubkyPublicKey, PubkySessionBootstrap,
    PublicationStatus,
};
use paykit_server::{
    config::{PaykitConfig, PaykitNetwork},
    crypto::Crypto,
    domain::locks::parse_creator,
    paykit::{CreatorSessionProvider, PaykitAdapter},
    persistence::{CreatorCredentials, CreatorStore, run_migrations},
};
use paykit_server_e2e::postgres::TestDatabase;
use pubky_testnet::EphemeralTestnet;

#[path = "fixtures/sdk.rs"]
mod sdk_fixtures;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buyer_profiles_become_private_wallet_contacts_without_overwriting_or_unblocking() {
    let database = TestDatabase::create().await;
    run_migrations(database.pool()).await.unwrap();
    let postgres = std::env::var("TEST_DATABASE_URL").unwrap();
    let testnet = EphemeralTestnet::builder()
        .postgres(pubky_testnet::pubky_homeserver::ConnectionString::new(&postgres).unwrap())
        .build()
        .await
        .unwrap();
    let client = testnet.sdk().unwrap();
    let home = PubkyPublicKey::from_public_key(&testnet.homeserver_app().public_key());
    let root = PubkyLocalSecretKey::new(pubky::Keypair::random().secret_key());
    let bootstrap = PubkySessionBootstrap::with_pubky(client.clone(), "app.bitkit.wallet").unwrap();
    let wallet_auth = bootstrap
        .sign_up(
            &root,
            &home,
            None,
            paykit_sdk::PAYKIT_AUTHORIZER_SESSION_CAPABILITIES,
        )
        .await
        .unwrap();
    let owner = wallet_auth.public_key.clone();
    let wallet = sdk_fixtures::hosted_sdk(wallet_auth.access, "bitkit", 0).await;
    let grant = PubkySessionBootstrap::with_pubky(client.clone(), "app.paykit.server")
        .unwrap()
        .sign_in(&root, paykit_sdk::PAYKIT_SESSION_CAPABILITIES)
        .await
        .unwrap();
    let creator = parse_creator(&owner.to_app_key()).unwrap();
    let creators = CreatorStore::new(
        database.pool(),
        Arc::new(Crypto::from_master_key(&[7; 32]).unwrap()),
    );
    let creator_id = creators
        .create(&CreatorCredentials::new(
            creator.clone(),
            grant
                .export_session_secret()
                .await
                .unwrap()
                .as_str()
                .to_owned(),
            root.derive_paykit_identity_secret_key(1).unwrap(),
            None,
            None,
        ))
        .await
        .unwrap()
        .id();
    let config = PaykitConfig {
        client_id: pubky::ClientId::new("app.paykit.server").unwrap(),
        app_id: paykit_lib::PaykitAppId::new("paykit-server").unwrap(),
        network: PaykitNetwork::Testnet,
        proposal_acceptance_window: Duration::from_secs(3600),
        payment_window: Duration::from_secs(86400),
        conversion_payment_window: Duration::from_secs(3600),
    };
    let adapter = PaykitAdapter::new(
        creator_id,
        CreatorSessionProvider::with_pubky(creators, creator, client.clone(), &config),
        &config,
    )
    .unwrap();
    let buyer_root = PubkyLocalSecretKey::new(pubky::Keypair::random().secret_key());
    let buyer_auth = bootstrap
        .sign_up(&buyer_root, &home, None, "/pub/:rw")
        .await
        .unwrap();
    let buyer_key = buyer_auth.public_key.clone();
    let buyer_session = buyer_auth.access.session.clone();
    let buyer = sdk_fixtures::hosted_sdk(buyer_auth.access, "bitkit", 0).await;

    // An absent public profile does not create an anonymous contact.
    adapter.save_buyer_contact(buyer_key.clone()).await.unwrap();
    assert!(wallet.contact_records().await.unwrap().is_empty());
    buyer_session
        .storage()
        .put(
            paykit_sdk::PUBKY_PROFILE_PATH,
            br#"{"name":"Pubky Buyer","image":"https://example.com/avatar.png"}"#.to_vec(),
        )
        .await
        .unwrap();
    let published = buyer
        .publish_paykit_profile(
            PaykitProfile {
                display_name: Some("Bitkit Buyer".into()),
                image_uri: None,
                extra: None,
            },
            None,
        )
        .await
        .unwrap();
    adapter.save_buyer_contact(buyer_key.clone()).await.unwrap();
    let contact = wallet.contact_record(&buyer_key).await.unwrap().unwrap();
    assert_eq!(contact.label.as_deref(), Some("Bitkit Buyer"));
    assert_eq!(
        contact.public_contact_marker_status,
        PublicationStatus::NotPublished
    );

    wallet
        .save_contact(ContactUpdate {
            public_key: buyer_key.clone(),
            label: Some("My label".into()),
        })
        .await
        .unwrap();
    let edited = wallet.contact_record(&buyer_key).await.unwrap().unwrap();
    adapter.save_buyer_contact(buyer_key.clone()).await.unwrap();
    assert_eq!(
        wallet.contact_record(&buyer_key).await.unwrap(),
        Some(edited)
    );

    wallet.remove_contact(&buyer_key).await.unwrap();
    buyer
        .delete_paykit_profile(published.revision)
        .await
        .unwrap();
    adapter.save_buyer_contact(buyer_key.clone()).await.unwrap();
    let fallback = wallet.contact_record(&buyer_key).await.unwrap().unwrap();
    assert_eq!(fallback.label.as_deref(), Some("Pubky Buyer"));
    assert_eq!(
        fallback.profile.unwrap().image_uri.as_deref(),
        Some("https://example.com/avatar.png")
    );

    wallet.block_peer(buyer_key.clone()).await.unwrap();
    wallet.remove_contact(&buyer_key).await.unwrap();
    adapter.save_buyer_contact(buyer_key.clone()).await.unwrap();
    assert!(wallet.contact_record(&buyer_key).await.unwrap().is_none());
    adapter.save_buyer_contact(owner).await.unwrap();
    assert!(wallet.contact_records().await.unwrap().is_empty());
    database.cleanup().await;
}
