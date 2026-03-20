use pheroclaw_messaging as msg;

/// Build list of all streams we should be consuming.
pub async fn discover_streams(redis: &fred::prelude::Client) -> anyhow::Result<Vec<String>> {
    let mut streams = vec![
        msg::BROADCAST_STREAM.to_string(),
        msg::DLQ_STREAM.to_string(),
    ];

    let roster = msg::get_roster(redis).await?;
    for agent_id in &roster {
        streams.push(msg::agent_inbox(agent_id));
        streams.push(msg::agent_outbox(agent_id));
        streams.extend(msg::all_channel_streams(agent_id));
    }

    streams.sort();
    streams.dedup();
    Ok(streams)
}

/// Return (added, removed) streams since last discovery.
pub fn diff_streams(current: &[String], previous: &[String]) -> (Vec<String>, Vec<String>) {
    let current_set: std::collections::HashSet<&String> = current.iter().collect();
    let previous_set: std::collections::HashSet<&String> = previous.iter().collect();

    let added: Vec<String> = current_set
        .difference(&previous_set)
        .map(|s| (*s).clone())
        .collect();
    let removed: Vec<String> = previous_set
        .difference(&current_set)
        .map(|s| (*s).clone())
        .collect();

    (added, removed)
}
