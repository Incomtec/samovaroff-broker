# Samovaroff Broker

Experimental distributed message broker written in Rust.

## Goal
Learning sprint: from Rust basics to a working message broker
with persistence and clustering in 14 days.

## Scope (v1)
- TCP-based protocol
- Topics and consumer groups
- At-least-once delivery
- Persistent log (WAL)
- Basic clustering (leader/follower)

## Non-goals
- Production readiness
- Full Raft implementation
- Exactly-once semantics

## Status
Day 0 - environment setup complete.
