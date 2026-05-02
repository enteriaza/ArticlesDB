# HistoryDB capacity planning

This document explains how **generations**, **shards**, **slot tables**, **load factor**, **Bloom filters**, and **`maxRetainedGenerations`** interact. It gives **closed-form estimates** so you can size `HistoryDatabase` / `ShardedHistoryWriter` for target insert rates, disk footprint, and—critically—**how long a Message-ID (or other hashed key) remains visible to lookup and duplicate checks**.

> **Scope:** On-disk and in-process structures owned by `ShardedHistoryWriter` and `HistoryShard`. Optional **walk journal** / metadata growth is mentioned separately; it is not part of the mmap generation math.

---

## 1. Symbols (tuning knobs)

| Symbol | Meaning | Typical source |
|--------|---------|------------------|
| \(S\) | **Shard count** | `HistoryDatabase` ctor `shardCount` (default **128**) |
| \(N\) | **Slots per shard** (power of two) | `slotsPerShard` (default **\(2^{22}\)** = 4 194 304) |
| \(L\) | **Max load factor** (percent, 1–99) | `maxLoadFactorPercent` (default **75**) |
| \(G\) | **Max retained generations** | `maxRetainedGenerations` (default **8**) |
| \(H\) | Shard file header bytes | Fixed **64** (`HistoryShard`) |
| \(E\) | Bytes per occupied slot row | Fixed **32** (`HistoryShard`) |
| \(b_{\mathrm{target}}\) | Target Bloom bits per *expected* inserted entry | `DefaultBloomBitsPerEntry` = **10** |
| \(f\) | Bloom RAM fraction of detected physical RAM | Default **0.80** (`DefaultBloomMemoryFraction`) |
| \(B\) | Optional explicit Bloom budget (bytes); `0` = derive from RAM × \(f\) | `bloomMemoryBudgetBytes` on writer |
| \(g\) | Generation index when sizing Bloom for the *next* open generation | Writer uses `nextGenerationCount =` current open count **+ 1** |

**Integer division** in the implementation matches C# `/` on non-negative integers: \(\lfloor x \rfloor\) for quotients.

---

## 2. Shard file geometry (`.dat`)

Each shard is one memory-mapped open-addressing table.

### 2.1 Load limit (inserts rejected as “full” per shard)

\[
N_{\mathrm{lim}} = \left\lfloor \frac{N \cdot L}{100} \right\rfloor
\]

Implemented as `(tableSize * maxLoadFactor) / 100` in `HistoryShard` (integer division).

### 2.2 Shard file size on disk (preallocated)

The file is **fully allocated** at creation:

\[
\mathrm{bytes}_{\mathrm{shard.dat}} = H + E \cdot N = 64 + 32\,N
\]

### 2.3 One generation — total `.dat` bytes (all shards)

\[
\mathrm{Disk}_{\mathrm{dat}}(S,N) = S \cdot (64 + 32\,N)
\]

For defaults \(S=128\), \(N=2^{22}\):

\[
64 + 32 \cdot 4\,194\,304 = 134\,217\,792 \text{ bytes per shard } \approx 128\ \mathrm{MiB}
\]

\[
128 \cdot 134\,217\,792 = 17\,179\,889\,152 \text{ bytes} = 16\ \mathrm{GiB\ (exactly,\ binary)}
\]

---

## 3. Logical capacity (distinct Message-IDs per generation)

Each **new** insert occupies **at most one slot** in **one** shard (chosen by `hashLo`). Inserts that dedupe to an existing row consume no additional capacity.

### 3.1 Maximum inserted rows before the generation is “full”

Under uniform routing, the soft limit is when **every** shard reaches \(N_{\mathrm{lim}}\):

\[
K_{\mathrm{gen}} = S \cdot N_{\mathrm{lim}} = S \cdot \left\lfloor \frac{N \cdot L}{100} \right\rfloor
\]

**Defaults:**

\[
N_{\mathrm{lim}} = \left\lfloor \frac{4\,194\,304 \cdot 75}{100} \right\rfloor = 3\,145\,728
\]

\[
K_{\mathrm{gen}} = 128 \cdot 3\,145\,728 = 402\,653\,184 \approx 4.03 \times 10^8
\]

So **one full generation** holds on the order of **400 M** distinct stored hashes (Message-IDs, after hashing), not counting duplicates rejected before insert.

### 3.2 Total distinct keys visible across **all retained** generations

At any time the writer keeps at most **\(G\)** generations open. A given Message-ID should exist in **at most one** generation (dedupe prevents re-insert elsewhere). In the worst case (every generation filled to its load limit with **disjoint** keys):

\[
K_{\mathrm{hot,max}} \le G \cdot K_{\mathrm{gen}}
\]

**Defaults:** \(8 \cdot 402\,653\,184 \approx 3.22 \times 10^9\) distinct keys.

That is an **upper bound**; real unions are smaller if generations roll before filling, or if traffic is duplicate-heavy.

---

## 4. Bloom sidecars (`.idx`) — bits and bytes

### 4.1 Target bits from expected entries per shard

Expected inserted entries per shard at load limit:

\[
E_{\mathrm{shard}} = N_{\mathrm{lim}} = \left\lfloor \frac{N \cdot L}{100} \right\rfloor
\]

Target Bloom bits (before budget / floor / power-of-two rounding):

\[
B_{\mathrm{target}} = \left\lceil E_{\mathrm{shard}} \cdot b_{\mathrm{target}} \right\rceil
\]

(`BloomSizingPolicy.ComputeTargetBitsByEntries`.)

### 4.2 RAM budget bits **per shard** for the **next** generation opening

When `bloomMemoryBudgetBytes` is **0**, the writer uses a fraction \(f\) of installed physical RAM (see `SystemMemoryReader.GetInstalledMemoryBytesForBloomBudget`). Let \(R_{\mathrm{ram}}\) be that byte estimate.

\[
B_{\mathrm{budget}} = 8 \cdot \begin{cases}
B & B > 0 \text{ (explicit budget)} \\
\lfloor f \cdot R_{\mathrm{ram}} \rfloor & \text{otherwise}
\end{cases}
\quad\text{(bits)}
\]

Shard-generation product for the **denominator** when opening the next generation:

\[
P = S \cdot g \quad\text{where}\quad g = \text{``current generation count + 1''}
\]

\[
B_{\mathrm{perShardBudget}} = \left\lfloor \frac{B_{\mathrm{budget}}}{P} \right\rfloor
\]

(`BloomSizingPolicy.ComputeBudgetBitsPerShard`.)

### 4.3 Desired bits and hard floor

\[
B_{\mathrm{raw}} = \min(B_{\mathrm{target}},\ B_{\mathrm{perShardBudget}})
\]

\[
B_{\mathrm{clamped}} = \max(B_{\mathrm{raw}},\ 2^{24}) \quad\text{(minimum }2^{24}\text{ bits = 16\,777\,216)}
\]

(`MinBloomBitsPerShard`.)

### 4.4 Final Bloom bit width (power of two)

\[
B_{\mathrm{final}} = \text{largest power of two } \le B_{\mathrm{clamped}}
\]

(`BitOperationsUtil.RoundDownToPowerOfTwo` → `bloomBitsPerShard`.)

### 4.5 On-disk Bloom file size (version 2 sidecar)

Layout (see `BloomSidecarFile`): 24-byte header + `wordCount` × 8 bytes + 4-byte CRC, with `wordCount = B_{\mathrm{final}} / 64`.

\[
\mathrm{bytes}_{\mathrm{shard.idx}} = 24 + 8 \cdot \frac{B_{\mathrm{final}}}{64} + 4 = 28 + \frac{B_{\mathrm{final}}}{8}
\]

**Per generation:**

\[
\mathrm{Disk}_{\mathrm{idx}}(S, B_{\mathrm{final}}) = S \cdot \left(28 + \frac{B_{\mathrm{final}}}{8}\right)
\]

For defaults, \(B_{\mathrm{final}}\) is typically **`2^24`** after rounding down from the \(\approx 3.15 \times 10^7\) target—so \(\approx 2\ \mathrm{MiB}\) per shard, \(\approx 256\ \mathrm{MiB}\) per generation across 128 shards (order of magnitude).

### 4.6 Important coupling

Bloom bits per shard **shrink** as **`g` grows** if RAM budget is fixed: opening many generations **without** increasing RAM budget **dilutes** Bloom bits per shard, which **raises false-positive rate** and can increase **shard probes** on `Exists` / duplicate checks.

---

## 5. Total generation footprint (mmap + Bloom only)

\[
\mathrm{Disk}_{\mathrm{gen}}(S,N,B_{\mathrm{final}}) = \mathrm{Disk}_{\mathrm{dat}}(S,N) + \mathrm{Disk}_{\mathrm{idx}}(S,B_{\mathrm{final}})
\]

**Order of magnitude, defaults:** \(\mathrm{Disk}_{\mathrm{dat}} \approx 16\ \mathrm{GiB}\) + Bloom sidecars \(\approx 0.25\ \mathrm{GiB}\) → **\(\approx 16.25\ \mathrm{GiB}\)** per generation (binary gigabytes).

---

## 6. What “retention” means in this library

### 6.1 Logical retention (Message-ID still answers “yes”)

A Message-ID is visible to **`HistoryLookup`** / duplicate checks **iff** the generation that holds it is still in the writer’s **retained generation list** (at most **\(G\)** generations). When the oldest generation is removed from that list, **lookups stop seeing keys that existed only there**, even if raw files may still exist on disk until asynchronous retire cleanup finishes.

So:

\[
\text{Wall-clock retention} \neq \text{Disk size alone}
\]

It is bounded by **how long a key stays inside the sliding window of \(\le G\) generations** the process keeps open.

### 6.2 Relating wall time to rollover (engineering estimate)

Let:

- \(\lambda_{\mathrm{ins}}\) = average rate of **successful inserts** (new rows) across all shards, in inserts/sec.
- Ignore duplicates for a **worst-case fill-time upper bound**: duplicates do not consume new slots.

**Time to fill one generation to its load limit (rough order):**

\[
T_{\mathrm{fill}} \approx \frac{K_{\mathrm{gen}}}{\lambda_{\mathrm{ins}}}
\]

**Time until the oldest of \(G\) generations could be retired** (worst case, rolling as fast as generations fill, one active writer path):

\[
T_{\mathrm{window}} \approx G \cdot T_{\mathrm{fill}} \approx \frac{G \cdot K_{\mathrm{gen}}}{\lambda_{\mathrm{ins}}}
\]

That \(T_{\mathrm{window}}\) is **not** a guarantee from the code—it is a **capacity planning heuristic**: if you need Message-IDs to remain dedupe-visible for **at least** \(T_{\mathrm{req}}\) seconds, you need the **hot** key set and rollover pattern such that keys are not evicted earlier—typically by increasing \(G\), and/or increasing \(K_{\mathrm{gen}}\) (larger \(N\) and/or \(S\) and/or \(L\)), and/or reducing how fast **new** keys consume slots (lower \(\lambda_{\mathrm{ins}}\)).

### 6.3 Total historical generations on disk (cold storage)

If you never delete retired folders externally, **cumulative** generation count \(n_{\mathrm{cum}}\) can be large. **Disk** for all `.dat` files (if all kept) scales:

\[
\mathrm{Disk}_{\mathrm{all.dat}} \approx n_{\mathrm{cum}} \cdot S \cdot (64 + 32\,N)
\]

Only **up to \(G\)** of those generations participate in **live** lookup at once.

---

## 7. Insert and lookup work (scaling)

### 7.1 Duplicate check before enqueue (`HistoryCrossGenerationDuplicateCheck.Full`)

For each candidate insert, the writer can walk **up to the number of open generations** (≤ \(G\)), and for **one shard per generation** (the shard selected by `hashLo`), using Bloom short-circuit then possibly `Exists` on mmap.

**Order (retained generations):** \(O(\min(G, g_{\mathrm{open}}))\) Bloom checks + up to the same order of shard probes in the worst Bloom-ambiguous case.

**Not** \(O(\text{total historical inserts})\).

### 7.2 Probe path inside one shard

Cost grows with **local occupancy** as tables approach \(L\%\). That is **per-generation**, not directly with total cold disk.

---

## 8. Sizing recipes (worked patterns)

### 8.1 “How big must \(N\) and \(S\) be for one generation to hold \(K_{\mathrm{target}}\) inserts?”

You need:

\[
S \cdot \left\lfloor \frac{N \cdot L}{100} \right\rfloor \ge K_{\mathrm{target}}
\]

Pick **power-of-two** \(N\), choose \(S\), verify \(L\). Remember **\(S\) and \(N\) are fixed for a given root**; changing them effectively requires a **new** dataset layout.

### 8.2 “How large must \(G\) be so the hot union can hold \(U\) distinct keys?” (upper bound sizing)

Need:

\[
G \cdot K_{\mathrm{gen}} \gtrsim U
\quad\Rightarrow\quad
G \gtrsim \left\lceil \frac{U}{K_{\mathrm{gen}}} \right\rceil
\]

**Caution:** large \(G\) increases **RAM** (Bloom per generation-shard, mmap resident set under access), **duplicate-check latency**, and **Bloom budget dilution** per §4.6.

### 8.3 “How much mmap disk for \(G\) full generations?”

\[
\mathrm{Disk}_{\mathrm{hot.dat}} \approx G \cdot S \cdot (64 + 32\,N)
\]

Add Bloom sidecars using §4–5 with your machine’s RAM budget plugged into \(B_{\mathrm{final}}\).

---

## 9. Defaults quick reference

| Quantity | Default-based value |
|----------|---------------------|
| \(S\) | 128 |
| \(N\) | \(2^{22}\) |
| \(L\) | 75 |
| \(G\) | 8 |
| \(K_{\mathrm{gen}}\) | 402 653 184 |
| \(K_{\mathrm{hot,max}}\) (bound) | \(\approx 3.22 \times 10^9\) |
| \(\mathrm{Disk}_{\mathrm{dat}}\) per generation | 16 GiB (exact, binary) |
| Bloom / `.idx` per generation | \(\approx\) hundreds of MiB (depends on \(B_{\mathrm{final}}\), RAM budget, \(g\)) |

---

## 10. Walk journal and metadata (not mmap generations)

When `enableWalkJournal` is **true** (default on `HistoryDatabase`):

- Successful inserts append to **`history-journal.log`** (unbounded **disk** growth with insert count unless rotated/archived externally).
- An in-memory walk ring is bounded by `maxWalkEntriesInMemory` (default **1 048 576** entries).

**Capacity planning for long retention should treat the journal as a separate storage problem** from generation mmap sizing.

---

## 11. Verification checklist

1. **Choose** \(S\), \(N\), \(L\) → compute \(K_{\mathrm{gen}}\).  
2. **Choose** \(G\) from required **hot** distinct-key horizon vs. §6.2 heuristic.  
3. **Compute** \(\mathrm{Disk}_{\mathrm{dat}}\), then **Bloom** \(B_{\mathrm{final}}\) using §4 with your **RAM** and expected **\(g\)**.  
4. **Check** duplicate-check / lookup latency budget vs. \(G\) and Bloom false positives.  
5. **Plan** journal / cold storage if “retention” must exceed the hot \(G\)-generation window.

---

## 12. References in code

- Shard geometry: `HistoryDB/HistoryShard.cs` (`HeaderSize`, `EntrySize`, `LoadLimitSlots`, mmap `fileSize`).
- Generation layout and Bloom sizing: `HistoryDB/ShardedHistoryWriter.cs` (`ComputeBloomBitsPerShard`, `CreateGeneration`, `MinBloomBitsPerShard`, retire path).
- Bloom policy math: `HistoryDB/Core/Policies/BloomSizingPolicy.cs`.
- Bloom sidecar bytes: `HistoryDB/Utilities/BloomSidecarFile.cs`.
- Public defaults: `HistoryDB/HistoryDatabase.cs` constructor parameters.

This document is descriptive of the implementation as of the repository version in which it was added; re-verify constants if you fork or change defaults.
