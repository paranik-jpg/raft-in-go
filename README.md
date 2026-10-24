# Raft in Go

A production-grade, modular, race-free implementation of the **Raft Distributed Consensus Algorithm** and replicated fault-tolerant key-value store built in Go from scratch.

This implementation strictly adheres to the specifications defined in the [Stanford Raft Paper](https://raft.github.io/raft.pdf) (*"In Search of an Understandable Consensus Algorithm"* by Diego Ongaro and John Ousterhout).

---

## Architecture & Subsystems

The consensus engine is decomposed into modular components following production systems engineering practices:

```
raft-in-go/
├── raft/
│   ├── raft.go            # Core Raft state, roles (Follower, Candidate, Leader), lifecycle & transitions
│   ├── raft_test.go       # Persistent/volatile state invariant tests & role transition tests
│   ├── log.go             # LogEntry struct, 1-indexed sentinel layout, log index & term helpers
│   ├── event_loop.go      # Central run() event loop, timer drivers, safe timer resets
│   ├── event_loop_test.go # Anti-split-vote randomized timeout tests & heartbeat reset tests
│   ├── rpc_vote.go        # RequestVote RPC structs, handler, election restriction & parallel fan-out
│   ├── rpc_vote_test.go   # Vote validation tests, parallel fan-out tests, election outcome tests
│   ├── rpc_append.go      # AppendEntries RPC structs, heartbeat handler, leader broadcast
│   └── rpc_append_test.go # (Upcoming) Log replication and consistency check tests
├── .gitignore
├── go.mod
├── LICENSE                # MIT License
└── README.md
```

---

## Core Raft Safety Invariants

This implementation maintains strict safety guarantees:

1. **Election Safety**:
   - Quorum rule: $\lfloor N / 2 \rfloor + 1$.
   - At most one leader can be elected in any given term.
   - Enforced by single-vote-per-term persistence (`votedFor`).

2. **Leader Completeness (§5.4.1)**:
   - Candidates must have logs at least as up-to-date as voters to receive a vote:
     $$\text{Candidate more up-to-date} \iff (T_{\text{last}}^{\text{cand}} > T_{\text{last}}^{\text{voter}}) \lor (T_{\text{last}}^{\text{cand}} == T_{\text{last}}^{\text{voter}} \land I_{\text{last}}^{\text{cand}} \ge I_{\text{last}}^{\text{voter}})$$
   - Prevents uncommitted entries from overwriting committed history.

3. **Log Matching Invariant (§5.3)**:
   - If two logs contain an entry with the same index and term, they are identical up to that index.
   - 1-indexed log layout anchored by a dummy sentinel entry at `log[0] = {Index: 0, Term: 0, Command: nil}`.

4. **Split-Vote Prevention**:
   - Randomized election timeout: $T_{\text{election}} \in [150\text{ms}, 300\text{ms})$.
   - Heartbeat interval: $T_{\text{heartbeat}} \approx 50\text{ms}$ ($T_{\text{heartbeat}} \ll T_{\text{election}}$).
   - Safe channel draining on `time.Timer` resets to eliminate race conditions and spurious early triggers.

---

## Implemented Milestones

- [x] **Milestone 1: Storage Layout & In-Memory State Definition**
  - Persistent state (`currentTerm`, `votedFor`, 1-indexed `log`).
  - Volatile state on all nodes (`commitIndex`, `lastApplied`).
  - Volatile state on leaders (`nextIndex`, `matchIndex`).
- [x] **Milestone 2: Three-State FSM Transitions**
  - `becomeFollower(term)`, `becomeCandidate()`, `becomeLeader()`.
  - Non-blocking channel signal dispatcher.
- [x] **Milestone 3: Event-Driven Unified Event Loop**
  - Continuous `select` loop over `electionTimer.C`, `heartbeatTimer.C`, and `stopCh`.
  - Safe timer reset helper `resetTimer()` with non-blocking channel draining.
- [x] **Milestone 4: Leader Election & RequestVote Subsystem**
  - `RequestVoteArgs` and `RequestVoteReply` wire protocols.
  - Term validation, vote granting idempotency, and election restriction checks.
  - Parallel vote fan-out with concurrent peer goroutines and quorum evaluation.
- [x] **Milestone 5: Heartbeat Mechanism (`AppendEntries`)**
  - `AppendEntriesArgs` and `AppendEntriesReply` wire protocols.
  - Recognition of valid leaders, candidate demotion, and heartbeat timer resets.
  - Parallel heartbeat dispatching from leader.
- [ ] **Milestone 6: Log Replication & Consistency Backtracking** *(Next)*
- [ ] **Milestone 7: State Machine Application (`applyCh`) & Commit Quorum**
- [ ] **Milestone 8: Replicated Key-Value Store (`Put`, `Append`, `Get`)**

---

## Running the Tests

All tests are verified with the Go race detector enabled:

```bash
# Run all tests with race detector
go test -v -race ./raft/...

# Run specific test suites
go test -v -race ./raft -run TestRequestVoteValidation
go test -v -race ./raft -run TestParallelVoteFanOut
```

---

## License

This project is licensed under the [MIT License](LICENSE).

