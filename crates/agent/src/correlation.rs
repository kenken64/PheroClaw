use std::collections::HashMap;
use std::sync::Mutex;

use pheroclaw_messaging::ClawMessage;
use tokio::sync::oneshot;
use uuid::Uuid;

/// Registry for in-flight request-reply correlations.
///
/// When the agent sends a message and expects a reply (e.g., P2P request-reply),
/// it registers the correlation ID here. When the reply arrives in the inbox,
/// the handler resolves it, delivering the message to the waiting future.
pub struct CorrelationRegistry {
    pending: Mutex<HashMap<Uuid, oneshot::Sender<ClawMessage>>>,
}

impl Default for CorrelationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrelationRegistry {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Register a correlation ID and return a receiver that will yield the reply.
    pub fn register(&self, id: Uuid) -> oneshot::Receiver<ClawMessage> {
        let (tx, rx) = oneshot::channel();
        let mut pending = self.pending.lock().expect("correlation lock poisoned");
        pending.insert(id, tx);
        rx
    }

    /// Attempt to resolve a pending correlation. Returns true if a waiter was found.
    pub fn resolve(&self, id: &Uuid, msg: ClawMessage) -> bool {
        let mut pending = self.pending.lock().expect("correlation lock poisoned");
        if let Some(tx) = pending.remove(id) {
            // Receiver may have been dropped (timeout); that's fine.
            let _ = tx.send(msg);
            true
        } else {
            false
        }
    }
}
