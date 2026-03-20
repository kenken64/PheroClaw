use std::sync::Arc;
use std::time::Duration;

use pheroclaw_messaging as msg;
use tracing::warn;

use crate::handler;
use crate::AgentState;

/// Background loop that polls the gateway for new messages and dispatches them.
pub async fn inbox_loop(state: Arc<AgentState>) {
    let group = msg::consumer_group(&state.agent_id);

    loop {
        match state.gateway.poll_inbox(&state.agent_id, &group).await {
            Ok(messages) => {
                for poll_msg in messages {
                    match msg::decode_message(&poll_msg.raw) {
                        Ok(claw_msg) => {
                            let st = state.clone();
                            let stream = poll_msg.stream.clone();
                            let entry_id = poll_msg.entry_id.clone();
                            let group = group.clone();

                            tokio::spawn(async move {
                                handler::handle_message(&st, &claw_msg).await;
                                if let Err(e) = st.gateway.ack(&stream, &group, &entry_id).await {
                                    warn!(error = %e, entry_id, "failed to ack message");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to decode message from poll");
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "poll failed, retrying after backoff");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
