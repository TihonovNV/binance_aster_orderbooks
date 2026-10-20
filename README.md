# Binance + Aster order books → cross-venue signal (ETHUSDT perpetual)

A Rust service that maintains two independent L2 order books, **Binance USD-M futures** and **Aster futures**, for **ETHUSDT perpetual**. Each book is built from the public REST snapshot plus the diff-depth WebSocket stream, with every update sequence-checked. A toy lead-lag signal reads both books and outputs `Hold / LongBias / ShortBias` with its numbers.

Public market data only: no API keys, no signing, no account or trading endpoints, no orders (not even dry-run).

```bash
cargo run --release                                  # run (defaults: --book btree --idle spin)
cargo run --release -- --timing --idle block         # per-stage timing on; laptop-friendly idle mode
cargo run --release -- record --secs 60              # refresh fixtures/ from the live venues
cargo test                                           # unit + integration (offline)
cargo test --test live -- --ignored --nocapture      # against the real venues
cargo bench --bench book                             # BTreeMap vs Vec on recorded data
```

Status server (default `127.0.0.1:8080`):

| Endpoint | What |
|---|---|
| `GET /status` | per venue: state, reason, time in state, tradable/valid/crossed, last `u`, book age, depth, I/O counters (msgs, resyncs, gaps, reconnects, stale events, parse errors, ring overflows, REST latency, exchange→local lag); engine telemetry freshness; dropped log records |
| `GET /signal` | latest decision with every input number |
| `GET /book/{binance\|aster}` | top 10 levels per side |
| `GET /timing` · `POST /timing?enabled=true\|false` | per-stage latency percentiles (10 s windows), toggled at runtime |
| `GET /health` | 200 if both books are tradable and the engine is publishing, else 503 |

## Venues and streams

| | Binance USD-M futures | Aster futures |
|---|---|---|
| WS diff depth | `wss://fstream.binance.com/public/stream?streams=ethusdt@depth@100ms` | `wss://fstream.asterdex.com/stream?streams=ethusdt@depth@100ms` |
| REST snapshot | `https://fapi.binance.com/fapi/v1/depth?symbol=ETHUSDT&limit=1000` | `https://fapi.asterdex.com/fapi/v1/depth?symbol=ETHUSDT&limit=1000` |
| Stream name | `ethusdt@depth@100ms` (combined-stream envelope) | `ethusdt@depth@100ms` (combined-stream envelope) |
| Tick / step | 0.01 / 0.001 | 0.01 / 0.001 |

- Aster's futures API is Binance-compatible. It uses the same `U / u / pu` sequencing, so one feed implementation serves both venues.
- Tick and step sizes are checked against `exchangeInfo` at startup. A mismatch is fatal.

## Architecture

```
 io-binance (spin)                      io-aster (spin)
  non-blocking WS → zero-alloc parse      (same code, own config)
  → i64 fixed point → sequence check
  → Begin / Levels×N / Commit ──┐  ┌── control ring (engine → io: Resync)
                                ▼  │
                     SPSC ring ×2 (rtrb, fixed-size Copy entries)
                                ▼
 engine (spin): owns BOTH books · applies transactions · invariants · signal inline
                                ▼ SPSC (telemetry, 10 Hz)
 http (tokio current_thread, parked): /status /signal /book /timing /health

 logger (polls): drains one SPSC ring of typed LogEvents per hot thread → tracing → stdout + logs/orderbooks.log.*
```

| Thread | Mode | Owns |
|---|---|---|
| `io-binance`, `io-aster` | busy-spin on a non-blocking socket | WS, parser, sequencer, REST recovery, backoff, watchdog |
| `engine` | busy-spin on both rings | both books, invariants, signal, timing histograms |
| `http` | tokio, parked | latest telemetry copy |
| `logger` | polls every 1 ms | formatting and I/O for every hot-thread log record |
| `main` | waits for Ctrl-C, then joins everything | — |

**Why no async on the hot path.**
- Each feed has exactly one I/O source, so an async runtime adds a scheduler and reactor hop and buys nothing.
- Latency-sensitive threads never sleep or block: the socket is non-blocking and `WouldBlock` goes to `spin_loop()`, and the watchdog and backoff are deadlines checked inside the spin loop.
- Writers never wake readers. There are no channels or futexes, only SPSC ring pushes, which are plain stores with Release ordering.
- Cost: 3 cores at 100%. `--idle block` switches to blocking reads and short sleeps.
- On macOS, spinning threads request the `USER_INTERACTIVE` QoS class, which keeps them on P-cores (macOS has no hard affinity). On Linux you would pin them with `isolcpus`/`nohz_full`.

**Variable-size messages over a fixed-size ring.**
- A parsed message has 2–2000 levels. It is sent as a **transaction**: `Begin { kind, header, recv time }`, then `Levels { side, n, [Level; 32] }` × ⌈n/32⌉, then `Commit { stamps }`.
- The producer checks free slots first and pushes all of it or nothing, so the engine never sees half a message.
- If the engine falls behind and the ring is full, the I/O thread drops data and resyncs. It never blocks and never drops data silently.

## Book synchronisation (per venue, in the I/O thread)

1. Connect the WS (frames queue in the kernel), then `GET` the REST snapshot (`lastUpdateId = L`) and send it to the engine as a `Snapshot` transaction.
2. Drop events with `u < L`. The first applied event must satisfy `U ≤ L ≤ u`. If the first event has `U > L`, the snapshot is too old and is fetched again (bounded retries, then reconnect).
3. While Live:

| Condition | Action |
|---|---|
| `pu == last_u` | forward to the engine |
| `u ≤ last_u` (stale or duplicate) | skip and count, **no resync** |
| anything else: gap, out-of-order or partial overlap | **immediate resync** on the same connection |

The sequencer (`feed/sync.rs`) is a pure state machine with no I/O and no book, and is unit-tested for every case above. In a 150 s probe and every run since, neither venue produced a gap.

## Storage and consistency

- **Who writes:** only the engine thread. It is the sole owner of both books, which are plain structs with no locks or atomics. The I/O threads never touch a book; they produce messages.
- **Who reads:** the signal, which is a function the engine calls between transactions. HTTP only ever sees `Telemetry` copies (top 10 per side, states, counters, decision, timing) pushed every 100 ms.
- **Reader vs writer race:** there is no concurrent book access, so a torn book is impossible by construction.
  - The only cross-thread handoff is the SPSC ring: write the slot, Release-store the tail; Acquire-load the tail, read the slot.
  - A book is flagged `processing_transaction` between `Begin` and `Commit`, and the signal is never evaluated on a book in that state.
- **Gap or resync on one venue while the other is alive:**
  - The I/O thread sends `Status(Resyncing)` in order with its data. The engine marks that book non-tradable, and the signal immediately returns `Hold` with a reason such as `not_live(binance)`.
  - The snapshot and bridging diff follow, then `Status(Live)`.
  - The other ring keeps flowing: the engine drains round-robin, at most 64 entries per ring per pass, so one venue's burst or snapshot can't starve the other.
  - An I/O thread that is blocked in REST or dead just goes stale. The book's age grows, the signal holds and `/status` shows it.
- **Cross-book consistency:** the signal reads both books at the same engine instant. Their exchange clocks are independent, so each book's age and the age difference are gated.
- **Defence in depth:**
  - The engine re-checks the `pu` chain on every diff. A break marks the book invalid and sends `Resync` back over the control ring.
  - A crossed book (`best_bid ≥ best_ask`) is counted, logged and gated by the signal, but does **not** trigger a resync. Correct sequencing makes it impossible, so it would point to a bug (ours or the venue's), and resync loops would only burn REST weight.

## Fault tolerance

| Failure | Detection | Reaction |
|---|---|---|
| network drop / IO error / close frame | `read()` error or `Close` | `Disconnected`, then exponential backoff (200 ms → 30 s, jittered, reset after 60 s stable), then reconnect |
| silent stall | no frame for 5 s (deadline in the spin loop) | reconnect |
| 24 h connection limit | age > 23 h | proactive reconnect |
| gap / out-of-order event | `pu` check | immediate REST resync on the same socket |
| snapshot older than stream | `U > L` on bridge | refetch (bounded), then reconnect |
| bad frame | parse error | count + log + skip; the next `pu` check catches any missing diff |
| REST 429/418 | status code | back off for at least `Retry-After` |
| engine too slow | ring full | drop, then resync once there is room |
| panic in an I/O thread | `catch_unwind` | log, restart the session |

Every transition is logged with venue, from/to state and reason, and is visible on `/status`.

## Signal: lead-lag microprice

Binance is treated as the leader and Aster as the follower. Each evaluation (after every committed update, on any feed state change, and at least every 250 ms) runs:

1. **Gates:** both books Live, valid, not crossed, not mid-transaction; each book younger than 1000 ms; age difference < 500 ms; each spread < 5 bps. Otherwise `Hold` with the specific reason.
2. `μB = (bidB·qAskB + askB·qBidB)/(qBidB + qAskB)` is Binance's microprice and `mA` is Aster's mid. `gap = (μB − mA)/mA` in bps.
3. The venues trade at a persistent small basis, so a time-based EMA (τ = 30 s) of the gap is subtracted: `dev = gap − EMA(gap)`.
4. `imbA` is Aster's top-5 depth imbalance and acts as a veto.
5. Decision:
   - `dev > θ` and `imbA > −κ` → **LongBias** (Aster expected to move up toward Binance)
   - the mirror case → **ShortBias**
   - otherwise **Hold**
   - With hysteresis, the bias is kept until `dev` falls below θ/2.

   Defaults: θ = 1 bps, κ = 0.3. All values are CLI flags.

The decision is logged when it changes and every second:

```
signal bias="LONG_BIAS" why=Signal dev_bps=+1.37 gap_bps=+1.03 ema_bps=-0.34 imb_a=+0.28 micro_b=2386.462 mid_a=2386.215 bbo_b=2386.46/2386.47 bbo_a=2386.20/2386.23 age_ms=405/0
```

`decide()` is a pure function and is unit-tested for each gate, basis removal, entry, the imbalance veto and hysteresis. Profitability is not claimed.

## Measurements (Apple M4 Pro, macOS, release build, live data)

**Hot path, per stage** (`--timing`, one 10 s window; stamps come from the CPU counter via `quanta`):

| Stage | Aster p50 / p99 | Binance p50 / p99 |
|---|---|---|
| parse (JSON → fixed-point levels) | 3.5 / 6.3 µs | 7.0 / 18.5 µs |
| sequence check | 0.21 / 0.42 µs | 0.17 / 0.54 µs |
| encode + push to ring | 0.38 / 8.1 µs | 0.62 / 4.8 µs |
| ring hop (last push → `Commit` popped) | 0.50 / 6.8 µs | 0.50 / 7.1 µs |
| apply (both sides, at `Commit`) | 1.3 / 4.7 µs | 6.8 / 20.7 µs |
| signal | 0.83 / 2.0 µs | 1.0 / 2.0 µs |
| **frame read → decision** | **7.6 / 18 µs** | **16 / 46 µs** |

The engine buffers each transaction's levels per side and applies them once at `Commit`, so the stages don't overlap. Live numbers are about 2× the benchmark (apply: 6.8 µs vs 2.9 µs on Binance), because at 10 messages a second the data and branch predictors are cold for every message.

**Coordinated omission:** `t0` is stamped when the frame is *read*, not when it *arrived*. Time spent queued in the kernel is therefore not measured: while the thread is preempted, after a resync burst, or behind another frame batched in the same read. The same applies to the freshness gate, which uses read time. The stages after `t0` are free of it, because each handoff is stamped by the producer. The fix is kernel or NIC receive timestamps (`SO_TIMESTAMP` / `SO_TIMESTAMPING`), plus exchange-time age with a clock-offset correction.

**Parsing allocates nothing.**
- sonic-rs's lazy API (`to_object_iter`, `get`) allocated about 13 times per message.
- The parser is instead a hand-written serde `DeserializeSeed` over sonic-rs's `Deserializer`. It writes borrowed `&str` pairs straight into reused buffers.
- `tests/alloc.rs` uses a counting global allocator and asserts **0 allocations per parse**.

**Book structure** (`cargo bench`, 60 s recorded per venue):

| per message | Updates within top 5 | 100+ levels deep | BTreeMap | Vec, linear scan per level | Vec, sorted merge per side | top-20 read BTree / Vec |
|---|---|---|---|---|---|---|
| Binance | 4.4% | 59.6% | **2.9 µs** | 15.9 µs | 5.2 µs | 33 ns / **6 ns** |
| Aster | 35.5% | 8.8% | **0.25 µs** | 0.36 µs | 0.39 µs | 33 ns / **6 ns** |

- **Per-level Vec:** a deep update pays O(depth) twice, once to scan from the best price and once to memmove the better levels. With ~130 mostly deep updates per Binance diff, that is effectively O(m·n) per message.
- **Sorted merge** (`apply_batch`):
  - Every recorded diff has both sides sorted by ascending price (1174/1174 Binance, 1010/1010 Aster). Unsorted input is still handled, by a stable sort.
  - So each side is applied in one merge pass over the touched suffix of the ladder, which is O(region + m) and allocates nothing.
  - That makes it 3× faster than the per-level Vec, but it can't beat the BTree on Binance. Binance diffs reach far from the top (p99 is 52% from mid), so the touched region is nearly the whole ladder: ~2,400 levels, ~38 KB per side copied every message. m·log n BTree hops are less work.
  - On Aster the diffs are tiny, and the merge's fixed overhead outweighs a couple of near-top inserts.
- **`btree` is the default.** `--book vec` uses the merge path, and its top-N reads are 5× faster.
- The remaining lever is the size of the ladder:
  - a bounded book (1000 levels per side, matching the snapshot; deeper Binance levels are incomplete anyway, see below)
  - or a hybrid: a Vec for the hot top-K levels plus a BTree for the tail

## Testing

- `src/**` unit tests:
  - fixed-point parser
  - both books against each other (proptest)
  - wire parser
  - every sequencer rule
  - chunked transaction round-trip and all-or-nothing push
  - signal gates and decisions
- `tests/engine.rs` drives the real engine thread through its rings:
  - snapshot + diffs → tradable book
  - broken chain → invalid book + `Resync` on the control ring
  - crossed book flagged, not resynced
  - one venue resyncing → signal holds while the other stays tradable
- `tests/replay.rs` replays `fixtures/`, real recordings: unbroken chain, never crossed, both book types identical at the end.
- `tests/alloc.rs` proves the parser allocates nothing. `tests/live.rs` (ignored by default) runs against the real venues.

## Limitations and production notes

- **Exchange→local lag** (`lag_ms`) depends on the local wall clock and shows negative values on a skewed clock. Correcting it against `/fapi/v1/time` would be the fix.
- **Deep levels:** Binance's 1000-level snapshot covers only ±0.4–0.5% around mid, but its diff stream reports changes at any price, out to the price limits.
  - In 60 s, 16% of Binance updates fell outside the snapshot's range, and the book grew to ~2,400 levels per side.
  - Beyond the snapshot edge we only know the levels that changed, so that part of the book is incomplete.
  - Aster's snapshot spans ±5%, and every update stayed inside it.
  - The signal uses the top 5 levels, which are always reliable.
- In production you would add:
  - pinned, isolated cores on Linux, kernel-bypass NICs, servers in the exchange's cloud region (AWS Tokyo for Binance)
  - redundant A/B connections per venue, arbitrated by sequence number
  - the `@bookTicker` stream overlaid on the 100 ms depth top for sub-ms reaction
  - a seqlock to publish books to other processes
