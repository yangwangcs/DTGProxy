use std::collections::{BTreeMap, BTreeSet};

use raft::eraftpb::Message;

struct QueuedMessage {
    sequence: u64,
    deliver_at: u64,
    message: Message,
}

#[derive(Default)]
pub struct DeterministicTransport {
    clock: u64,
    next_sequence: u64,
    queue: Vec<QueuedMessage>,
    isolated_nodes: BTreeSet<u64>,
    blocked_links: BTreeSet<(u64, u64)>,
    link_delays: BTreeMap<(u64, u64), u64>,
    drop_counts: BTreeMap<(u64, u64), usize>,
    duplicate_counts: BTreeMap<(u64, u64), usize>,
    reverse_ready_order: bool,
}

impl DeterministicTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn send(&mut self, message: Message) {
        let link = (message.from, message.to);
        if self.isolated_nodes.contains(&message.from)
            || self.isolated_nodes.contains(&message.to)
            || self.blocked_links.contains(&link)
        {
            return;
        }
        if let Some(remaining) = self.drop_counts.get_mut(&link)
            && *remaining > 0
        {
            *remaining -= 1;
            return;
        }
        let delay = self.link_delays.get(&link).copied().unwrap_or(0);
        let copies = self.duplicate_counts.remove(&link).unwrap_or(0) + 1;
        for _ in 0..copies {
            self.queue.push(QueuedMessage {
                sequence: self.next_sequence,
                deliver_at: self.clock.saturating_add(delay),
                message: message.clone(),
            });
            self.next_sequence = self.next_sequence.saturating_add(1);
        }
    }

    pub fn send_all(&mut self, messages: impl IntoIterator<Item = Message>) {
        for message in messages {
            self.send(message);
        }
    }

    #[must_use]
    pub fn take_ready(&mut self) -> Vec<Message> {
        let mut pending = Vec::with_capacity(self.queue.len());
        let mut ready = Vec::new();
        for queued in self.queue.drain(..) {
            if queued.deliver_at <= self.clock {
                ready.push(queued);
            } else {
                pending.push(queued);
            }
        }
        self.queue = pending;
        ready.sort_by_key(|queued| queued.sequence);
        if self.reverse_ready_order {
            ready.reverse();
        }
        ready.into_iter().map(|queued| queued.message).collect()
    }

    pub fn advance(&mut self) {
        self.clock = self.clock.saturating_add(1);
    }

    pub fn isolate(&mut self, node_id: u64) {
        self.isolated_nodes.insert(node_id);
    }

    pub fn heal(&mut self, node_id: u64) {
        self.isolated_nodes.remove(&node_id);
    }

    pub fn block_link(&mut self, from: u64, to: u64) {
        self.blocked_links.insert((from, to));
    }

    pub fn heal_link(&mut self, from: u64, to: u64) {
        self.blocked_links.remove(&(from, to));
    }

    pub fn delay_link(&mut self, from: u64, to: u64, ticks: u64) {
        self.link_delays.insert((from, to), ticks);
    }

    pub fn clear_link_delay(&mut self, from: u64, to: u64) {
        self.link_delays.remove(&(from, to));
    }

    pub fn drop_next(&mut self, from: u64, to: u64, count: usize) {
        self.drop_counts.insert((from, to), count);
    }

    pub fn duplicate_next(&mut self, from: u64, to: u64, extra_copies: usize) {
        self.duplicate_counts.insert((from, to), extra_copies);
    }

    pub const fn set_reverse_ready_order(&mut self, enabled: bool) {
        self.reverse_ready_order = enabled;
    }

    #[must_use]
    pub const fn clock(&self) -> u64 {
        self.clock
    }

    #[must_use]
    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }
}
