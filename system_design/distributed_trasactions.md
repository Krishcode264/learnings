# Distributed Transactions Explained: 2-Phase Commit vs Saga Pattern

> 🎥 **Video**: [Distributed Transactions Explained — 2 Phase Commit vs Saga Pattern](https://youtu.be/DOFflggE_0Q)

---

**Tags:** `#distributed-systems` `#microservices` `#transactions` `#2pc` `#saga-pattern` `#eventual-consistency` `#system-design` `#backend` `#kafka` `#temporal` `#outbox-pattern`

---

## ⚡ TL;DR (2-minute revision)

- A **distributed transaction** spans multiple independent databases/services — you can't use a single DB `ROLLBACK`.
- **2PC (Two-Phase Commit)** gives you strong consistency but is a **blocking protocol** — a coordinator crash leaves all participants stuck with locks held indefinitely.
- **Saga Pattern** breaks the work into independent local transactions with **compensating actions** (refunds, cancellations) instead of rollbacks — accepts **eventual consistency**.
- Sagas have two flavors: **Choreography** (event-driven, decentralized, simple flows) and **Orchestration** (central controller, complex flows, what most teams use at scale).
- The **Dual Write Problem** (DB write + event publish can fail independently) is fixed with the **Transactional Outbox Pattern**.
- Industry standard at scale: **Saga + Orchestration + Transactional Outbox** — used by Uber, Netflix, Amazon.

---

## 📖 Full Notes

### 1. The Starting Point — Single DB Transactions

When your app uses a **single database**, transactions are simple and automatic. The DB gives you **ACID guarantees**:

| Property | What it means |
|---|---|
| **Atomicity** | All writes succeed together or none do |
| **Isolation** | Partial state is invisible to other queries mid-transaction |
| **Consistency** | DB goes from one valid state to another |
| **Durability** | Committed writes survive crashes |

```sql
BEGIN TRANSACTION;
  INSERT INTO payments ...;    -- charge card
  UPDATE inventory ...;        -- reserve stock
  INSERT INTO ledger ...;      -- record accounting entry
COMMIT; -- or ROLLBACK if any step fails
```

This works because all three operations hit the **same database**. The engine handles atomicity for you.

---

### 2. The Problem — When You Scale Out

As traffic grows, you **split data** across machines:
- **Database sharding** — spread write load across nodes
- **Microservices** — each service owns its own DB

```
┌──────────────────────────────────────────────────┐
│                   Order Flow                      │
│                                                   │
│  Payment DB      Inventory DB      Ledger DB      │
│  ┌─────────┐    ┌─────────────┐  ┌────────────┐  │
│  │ charge  │    │  reserve    │  │  record    │  │
│  │  card   │    │  stock      │  │  entry     │  │
│  └─────────┘    └─────────────┘  └────────────┘  │
│                                                   │
│  ← These are now 3 separate commits on 3 machines │
└──────────────────────────────────────────────────┘
```

**The problem:** If the card charge commits but the inventory reservation fails (out of stock), there's **no cross-DB rollback**. The charge is already permanent on a completely different machine.

This is a **Distributed Transaction** — a single logical operation that must span multiple independent databases.

---

### 3. Two-Phase Commit (2PC)

#### How it works

2PC introduces a **coordinator** that ensures all participants agree before anything is made permanent.

```
Phase 1 — PREPARE
─────────────────────────────────────────────────────
Coordinator ──► Payment DB:   "Can you commit?"
             ──► Inventory DB: "Can you commit?"
             ──► Ledger DB:   "Can you commit?"

Each participant:
  1. Does the work
  2. Durably records changes (won't lose if it crashes)
  3. Locks the affected rows
  4. Replies YES or NO

─────────────────────────────────────────────────────
Phase 2 — COMMIT or ABORT
─────────────────────────────────────────────────────
If ALL vote YES → Coordinator sends COMMIT to all
If ANY votes NO → Coordinator sends ABORT to all

Each participant:
  - On COMMIT: makes changes permanent, releases locks
  - On ABORT:  discards changes, releases locks
```

#### The Fatal Flaw — Coordinator Crash

```
Timeline of disaster:

Coordinator collects 3 YES votes...
          │
          ▼
      CRASH 💥  ← right here, before sending COMMIT/ABORT
          │
          ▼
All participants sitting with LOCKS HELD, waiting forever.
They can't self-commit (maybe others were told to abort).
They can't self-abort (maybe others were told to commit).

Result: Every other transaction touching those rows = BLOCKED.
```

#### Other 2PC Problems

| Problem | What happens |
|---|---|
| **Slow participant** | Entire transaction waits at the speed of the slowest node |
| **Network partition** | Coordinator can't tell if message was delivered → no safe default |
| **Lock amplification** | Locks held across all participants the entire duration |

> **Real-world usage of 2PC:** It exists inside tightly-coupled distributed databases like **Google Spanner** and **YugabyteDB** — where the coordinator and participants are the same system and they handle the complexity internally. You should **never implement 2PC yourself across independent microservices**.
>
> 📄 Pat Helland's paper *"Life Beyond Distributed Transactions"* argues this point definitively.

---

### 4. The Saga Pattern

#### Core Philosophy

> "You don't need all-or-nothing atomicity across services. You just need to **eventually reach a consistent state**."

Instead of one big distributed transaction:
- Break work into a **chain of independent local transactions**
- Each service commits to its own DB on its own terms
- On failure → run **compensating actions** (business-level undos)

#### Compensating Actions vs Rollbacks

| Database Rollback | Compensating Action |
|---|---|
| Invisible to users | May be visible (e.g. charge then refund) |
| Instant, atomic | Async, may take time |
| Always reversible | Some actions can't be truly undone (emails!) |
| Free | Requires explicit business logic |

```
Happy path:
charge card ──► reserve stock ──► record ledger ✅

Failure at inventory step:
charge card ──► reserve stock ❌
                    │
                    ▼
            issue REFUND ◄── compensating action
```

#### Eventual Consistency Trade-off

The customer **might briefly see** a charge before the refund goes through. But the system **always converges** to a correct state, and **nothing is blocked** during that convergence period.

---

### 5. Saga Flavors — Choreography vs Orchestration

#### 5A. Choreography (Decentralized, Event-Driven)

Each service broadcasts events and reacts to others' events. No central controller.

```
┌──────────────────────────────────────────────────────────┐
│                  Event Bus / Message Broker               │
│                                                          │
│  Card Service                                            │
│    ──► publishes: card_charged                           │
│                        │                                 │
│                        ▼                                 │
│  Inventory Service (listening for card_charged)          │
│    ──► reserves stock                                    │
│    ──► publishes: inventory_reserved                     │
│                        │                                 │
│                        ▼                                 │
│  Ledger Service (listening for inventory_reserved)       │
│    ──► records entry                                     │
│                                                          │
│  On failure:                                             │
│    Failing service ──► publishes: inventory_failed       │
│    Card Service reacts ──► runs compensation (refund)    │
└──────────────────────────────────────────────────────────┘
```

**Pros:**
- Simple to implement for 2–3 step flows
- Services are loosely coupled
- No single point of failure

**Cons:**
- At 5–6 services: hard to track state of any given transaction
- No central place to ask "where exactly did this order fail?"
- Compensations scattered across multiple services
- Debugging = digging through logs across dozens of services

**Best for:** Simple, independent flows. E.g. order placed → send email notification.

---

#### 5B. Orchestration (Centralized Controller)

A dedicated **Orchestrator service** drives the entire flow step-by-step.

```
                  ┌─────────────────────┐
                  │     Orchestrator     │
                  │  (e.g. Temporal,     │
                  │   AWS Step Func.)    │
                  └──────────┬──────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌─────────────┐ ┌─────────────┐ ┌──────────────┐
     │ Card Service│ │ Inventory   │ │ Ledger       │
     │  charge()   │ │  reserve()  │ │  record()    │
     └─────────────┘ └─────────────┘ └──────────────┘

Flow:
1. Orchestrator → Card Service: "charge the card" → waits
2. Orchestrator → Inventory: "reserve stock" → waits
3. If failure: Orchestrator knows exact state → triggers right compensations in right order
```

**Critical difference from 2PC:** If the orchestrator crashes:
- **2PC coordinator**: leaves dangling locks across all participants ❌
- **Orchestrator**: is **durable** — restarts, reads its own state from DB, picks up exactly where it left off ✅. No rows locked, no other transactions blocked.

**Tools:**
- **[Temporal](https://temporal.io/)** — created by engineers behind Uber's Cadence workflow engine
- **[AWS Step Functions](https://aws.amazon.com/step-functions/)** — managed orchestration as a service

**Pros:**
- Single source of truth for transaction state
- Easy to see where a saga is stuck
- Compensations defined centrally, not scattered
- Durable recovery after crashes

**Best for:** Complex flows with branching logic, flows requiring visibility, tricky compensation logic.

---

### 6. The Dual Write Problem

When a service finishes work it must:
1. Write result to its **own database**
2. Publish an **event to the message broker** (so next service proceeds)

These are **two separate writes to two separate systems** — both can fail independently.

```
                 ┌─────────────────────┐
                 │   Card Service      │
                 └──────────┬──────────┘
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
       ┌──────────────┐        ┌──────────────────┐
       │  Payment DB  │        │  Message Broker   │
       │  (write ✅)  │        │  (publish ❌)     │
       └──────────────┘        └──────────────────┘

Result: Inventory service never triggered. Flow stalls.
        Or vice versa: event published but DB write failed → downstream
        services react to something that didn't happen.
```

#### Fix: Transactional Outbox Pattern

```
                 ┌─────────────────────────────────────────┐
                 │           Payment DB (same DB)          │
                 │                                         │
                 │  ┌────────────────┐  ┌───────────────┐ │
                 │  │  payments      │  │  outbox       │ │
                 │  │  (your data)   │  │  (events)     │ │
                 │  └────────────────┘  └───────────────┘ │
                 │      ← single local transaction →       │
                 └────────────────────┬────────────────────┘
                                      │
                              ┌───────▼───────────┐
                              │  Background Worker │
                              │  (polls / CDC)     │
                              └───────┬────────────┘
                                      │
                              ┌───────▼────────────┐
                              │   Message Broker    │
                              └────────────────────┘
```

**How it works:**
1. Write your data **and** the outgoing event into the **same DB, same local transaction** — to an `outbox` table
2. Either both commit or neither do (local ACID!)
3. A **background process** (poller or CDC) reads the outbox and publishes to the broker

**CDC (Change Data Capture):** Tails the database's own transaction logs to detect new outbox entries in near-real-time. Tools: Debezium, AWS DMS.

---

### 7. Decision Framework — What Should You Use?

```
Start here:
────────────────────────────────────────────────────────────
Can you put the data that transacts together into ONE DB?
  │
  ├─ YES → Do it. Local transaction is simpler, faster, more reliable.
  │         (Move inventory + ledger into payments DB if they always update together)
  │
  └─ NO (genuinely distributed) → Use a Saga
              │
              ├─ Simple flow (≤3-4 steps, truly independent services,
              │   no need for central visibility)?
              │       └─ Choreography
              │
              └─ Complex flow (branching logic, tricky compensations,
                  need visibility into state, 5+ services)?
                      └─ Orchestration (Temporal / Step Functions)
                              │
                              └─ + Transactional Outbox for reliable event publishing

Need STRONG consistency and can't accept eventual consistency at all?
  └─ Use a distributed DB like Spanner or YugabyteDB (they handle 2PC internally)
     NOT rolling your own 2PC across services.
```

---

## 📊 Comparison Table

| | **2PC** | **Saga (Choreography)** | **Saga (Orchestration)** |
|---|---|---|---|
| **Consistency model** | Strong (ACID) | Eventual | Eventual |
| **Blocking** | Yes — locks held across all participants | No | No |
| **Coordinator crash** | Participants stuck with locks indefinitely | N/A | Orchestrator restarts, picks up from last state |
| **Failure visibility** | Coordinator knows | Scattered in event logs | Centralized in orchestrator |
| **Complexity** | Protocol complexity | Flow logic scattered | Flow logic centralized |
| **Compensations** | Not needed (rollback) | Service-level, decentralized | Centralized, explicit |
| **Scalability** | Poor (blocking, slow participant = all slow) | Good | Good |
| **Used by** | Spanner, YugabyteDB (internally) | Simple flows | Uber, Netflix, Amazon |
| **Tooling** | Built into distributed DBs | Kafka, RabbitMQ | Temporal, AWS Step Functions |
| **Best for** | Tightly coupled distributed DBs | Simple 2–3 step flows | Complex multi-service flows |

---

## 🃏 Interview Cheat Sheet

| Concept | One-liner |
|---|---|
| **Distributed transaction** | Single logical op spanning multiple independent DBs where all steps must succeed or be cleaned up |
| **2PC** | Coordinator asks all participants "can you commit?" (Phase 1), then sends final decision (Phase 2) |
| **Why 2PC fails** | Blocking protocol — coordinator crash leaves participants stuck with locks held, no safe default |
| **Saga** | Chain of local transactions with compensating actions (refunds, cancellations) instead of rollbacks |
| **Compensating action** | Business-level undo — "a refund instead of a rollback" |
| **Eventual consistency** | System may be temporarily inconsistent but always converges; no blocking during convergence |
| **Choreography** | Services publish events and react to others; no central controller; good for simple flows |
| **Orchestration** | Central orchestrator drives each step; durable; best for complex flows |
| **Temporal / Step Functions** | Tools purpose-built for Saga orchestration |
| **Dual write problem** | DB write + event publish are two separate systems that can fail independently |
| **Transactional Outbox** | Write data + event to same DB in one local transaction; background worker publishes the event |
| **CDC** | Change Data Capture — tails DB transaction logs to detect outbox entries |
| **Idempotency** | Compensation ops must be idempotent — same result whether run 1x or 10x (no double-refunds) |
| **Pat Helland paper** | "Life Beyond Distributed Transactions" — argues 2PC doesn't work at internet scale |

---

## 🧠 Interview Questions This Topic Covers

1. What is a distributed transaction and why is it hard?
2. Explain how 2-Phase Commit works. What are its failure modes?
3. Why don't most companies use 2PC across microservices?
4. What is the Saga pattern and how does it differ from 2PC?
5. What are compensating transactions? Give a real example.
6. Compare Saga Choreography vs Orchestration — when would you use each?
7. What is the dual write problem and how does the Transactional Outbox pattern solve it?
8. What is eventual consistency? When is it acceptable?
9. What tools would you use to implement Saga Orchestration?
10. How does an orchestrator handle crashes differently than a 2PC coordinator?
11. What does it mean for a compensating action to be idempotent and why does it matter?
12. Where is 2PC actually used in production today?

---

## 🔗 Related Topics to Explore

- [ ] **CAP Theorem** — why distributed systems must choose between Consistency, Availability, Partition tolerance
- [ ] **BASE vs ACID** — the theoretical underpinning of eventual consistency
- [ ] **Change Data Capture (CDC)** — Debezium, AWS DMS, streaming DB logs
- [ ] **Temporal.io deep dive** — workflow engine internals, durable execution model
- [ ] **Apache Kafka** — message broker backbone for choreography-based sagas
- [ ] **Idempotency patterns** — idempotency keys, deduplication in APIs
- [ ] **Outbox Pattern implementations** — Debezium + Kafka vs polling
- [ ] **Google Spanner** — how it achieves external consistency with TrueTime
- [ ] **Event Sourcing** — storing state as a sequence of events, related to choreography
- [ ] **CQRS** — often paired with event-driven Saga architectures
- [ ] **Pat Helland — "Life Beyond Distributed Transactions"** (paper)
- [ ] **Distributed locks** — Redlock, etcd, when you actually need them

---

*Notes based on: [Distributed Transactions Explained — 2PC vs Saga Pattern](https://youtu.be/DOFflggE_0Q)*