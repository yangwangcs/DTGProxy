# Fine-Grained Request Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Attribute four-process Fjall request time to the gateway, Meta, Data lock/Raft, read-view cache, and temporal evaluation boundaries without changing query or storage semantics.

**Architecture:** Keep the ten existing request stages as stable coarse counters. Add a schema-v2 `details` collection of named cumulative histograms, record timing at existing RPC and storage boundaries, and let the artifact reader accept both legacy schema-v1 and complete schema-v2 snapshots.

**Tech Stack:** Rust, Tokio, Tonic, Fjall, serde JSON.

## Tasks

- [ ] Add test-covered schema-v2 detail metrics and backward-compatible artifact parsing.
- [ ] Instrument Gateway query encode/collect/decode/materialization and CREATE Meta/Data RPC phases.
- [ ] Instrument Data route lock versus lookup, Raft propose versus drive-ready, and read-view cache/open phases.
- [ ] Expose Fjall read-view point and scan sub-operation counters through the existing storage trait's optional diagnostics hook.
- [ ] Run focused unit/process tests, release four-process Fjall diagnostic, and report totals, calls, means, and cache-hit rate.
