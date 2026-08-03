use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use paykit_sdk::{
    PaymentAdapter, PaymentTarget, PubkySessionAccess, PubkySessionProvider,
    PublicPaymentEndpointCandidate, PublicPaymentEndpointSelectionRequest, PublicReceivingDetail,
};

#[derive(Clone)]
pub struct TestSessionProvider {
    access: Arc<Mutex<Option<PubkySessionAccess>>>,
}

impl TestSessionProvider {
    pub fn new(access: PubkySessionAccess) -> Self {
        Self {
            access: Arc::new(Mutex::new(Some(access))),
        }
    }
}

#[async_trait]
impl PubkySessionProvider for TestSessionProvider {
    async fn load_session_access(&self) -> paykit_sdk::Result<Option<PubkySessionAccess>> {
        Ok(self.access.lock().unwrap().clone())
    }

    async fn load_public_storage(
        &self,
    ) -> paykit_sdk::Result<Option<pubky_testnet::pubky::PublicStorage>> {
        Ok(self
            .access
            .lock()
            .unwrap()
            .as_ref()
            .map(|access| access.outbox_client.public_storage()))
    }

    async fn clear_session_access(&self) -> paykit_sdk::Result<()> {
        *self.access.lock().unwrap() = None;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct TestPaymentAdapter;

#[async_trait]
impl PaymentAdapter for TestPaymentAdapter {
    async fn current_public_receiving_details(
        &self,
    ) -> paykit_sdk::Result<Vec<PublicReceivingDetail>> {
        Ok(Vec::new())
    }

    async fn select_public_payment_endpoints(
        &self,
        request: &PublicPaymentEndpointSelectionRequest,
    ) -> paykit_sdk::Result<Vec<PublicPaymentEndpointCandidate>> {
        Ok(request.candidates.clone())
    }

    async fn build_public_payment_target(
        &self,
        endpoint: &PublicPaymentEndpointCandidate,
    ) -> paykit_sdk::Result<PaymentTarget> {
        Ok(PaymentTarget {
            payload: endpoint.payload.clone(),
        })
    }
}
