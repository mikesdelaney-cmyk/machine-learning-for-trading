# Alpaca PAPER deployment record

> **Scope:** PAPER trading only. Nothing in this record authorizes live-money
> trading.
>
> **Status (2026-09-24):** runtime 04 is qualified and frozen. Notebook 02 has
> passed frozen-data and refreshed-data offline dry runs. Its first scheduled
> PAPER basket remains a future, one-time event. A one-shot agent wake is
> scheduled for that event; it does not launch the notebook directly.

## 1. Supported deployment boundary

This deployment qualified two deliberately different Chapter 25 paths:

1. `04_alpaca_paper_trading_full_session_runtime.py` is a supervised,
   event-driven Alpaca PAPER runtime using `AlpacaDataFeed`, `SafeBroker`, and
   `LiveEngine`. Its unchanged five-bar ETF momentum strategy was qualified
   through a full core session, and its existing BUY and SELL branches were
   exercised in a separate bounded one-share rehearsal. The tracked runtime
   script is now frozen.
2. `02_etfs_deployment_loop.ipynb` is a scheduled Ridge/top-five ETF
   deployment notebook. It refreshes the data root named by `ML4T_DATA_PATH`
   (operationally, the separate deployment copy), refits on the fixed pre-live
   training window, builds an offline reference tape, checks target-basket
   parity, optionally stages one BUY-only Alpaca PAPER basket, writes a run
   record, and stops.

Notebook 02 is **not** an always-on portfolio manager. It has no live SELL or
cross-rebalance holdings-management path. Persistent autonomous portfolio
management, unattended exits, generalized rebalancing, multi-strategy
orchestration, high availability, and live-money operation are outside the
repository-supported boundary.

## 2. Reproducible environment

| Component | Qualified identity |
|---|---|
| Python | CPython 3.14.7 |
| Deployment `uv` | `/home/coolermichael/.local/share/ml4t-tools/uv-0.12.5/uv` (`uv 0.12.5`) |
| `ml4t-live` | `0.1.1.dev11+g8aa1849`, commit `8aa18493d111f03991f2df9c44923f27acc9b03c` |
| `ml4t-data` | `0.1.dev131+gce0e7e8b5`, fork commit `ce0e7e8b5c3b6978690e211a1dad08d3f73944d9` |
| `alpaca-py` | `0.44.0` |

Live-import and installed-source evidence is retained locally in
`25_live_trading/output/deployment_completion_audit/validations/environment-2026-09-24.md`.

Qualified source identities:

- notebook 02 Python SHA-256:
  `f4edb3cc5251d8517923d4e181587c5f4a8e0130328c1320bc60ff0ca5fb95e5`;
- notebook 02 IPYNB SHA-256:
  `2ab4aa346df261db8fad770c67ba86f34352e8c8acf3b288ae9a128a70ea6c33`;
- `pyproject.toml` SHA-256:
  `3873d626f86927ecc79fa0e67363394e641b96644cf9ade508e2ae54c7e1ec54`;
- `uv.lock` SHA-256:
  `11c7f31ee1623129cd6a15050a5cd6edb55f472905337456e8a14ee8e9463440`.

The qualified frozen and refreshed runs used exactly those notebook bytes;
the pre-commit hashes and focused checks are retained in
`25_live_trading/output/deployment_completion_audit/reviews/c3_replay_fix/packet/`.

Deployment commands use the existing `.venv` and exact `uv` binary with
`uv run --no-sync`. Do not use plain `uv run`: it may synchronize the
environment and reinstall the local project before executing. Do not run
`uv sync` as part of normal operation.

## 3. Branch, upstream, and deployment deviations

- Upstream base: `291d447`.
- Deployment branch: `deployment/alpaca-compatibility-clean`.
- Reviewed deployment commits through `73f88b70`:
  `3a0e8b30`, `4c211d48`, `9d3a52b2`, `3ccfd7e0`, `66bcb342`,
  `95a5b1d9`, `8af0c3ae`, `b526ac26`, `0aa223a3`, and `73f88b70`.
- The branch pins `ml4t-data` to fork commit `ce0e7e8b...` and
  `ml4t-live` to commit `8aa18493...` under `[tool.uv.sources]`.
- Runtime 04 explicitly uses `AlpacaDataFeed(..., experimental=True)` and
  `execution_mode="paper"`.
- A failed SSD/storage path was replaced; the replacement passed the required
  health and durability gates before deployment work resumed.
- The frozen research data is never refreshed in place. Mutable operation
  uses `/home/coolermichael/ml4t-deployment-data`.

No wholesale upstream merge was used. Later upstream work was treated as
verification information and only a required, isolated fix was selected:

- Upstream `01c1fba5` / #936 documented that notebook 02's offline replay
  could reject some of its own orders. The local frozen replay reproduced the
  defect: 79 orders, 71 fills, and 8 true refusals.
- The minimal backport adds a 2% sizing cash buffer and explicit replay-order
  disposition validation, while distinguishing legitimate final-bar
  `NEXT_BAR` pending orders from refusals. It does not wholesale replace the
  notebook or import unrelated registry/prose changes.
- Post-fix frozen replay produced 84 orders, 84 fills, zero final-bar pending,
  and zero refused orders.
- Later notebook-related upstream commits such as `92ccfaad` and `426ecbe1`
  were inspected but not merged wholesale because the qualified local path
  did not require their broader changes.

## 4. Accepted execution evidence

### 4.1 A-MIN provider-path validation

- Attempt: `6ce96dc2-1905-4cbe-b83b-9456971211e7`.
- One SPY BUY LIMIT/DAY order filled for one share.
- Provider, adapter, SafeBroker, and persisted views agreed in three
  sequential observations; cleanup completed successfully.
- The diagnostic share was later sold once through Alpaca PAPER and the
  provider account ended flat with zero open orders.
- Evidence is retained under
  `25_live_trading/output/alpaca_paper_entry_min/`.

### 4.2 Runtime 04 full-session qualification

- The unchanged production runtime completed one normal NYSE PAPER session
  through the core close.
- It accepted 805 market-data bars.
- The unchanged `lookback=5`, `threshold=0.02`, `position_size=10` strategy
  emitted no signals or orders, which is a valid strategy outcome.
- Startup reconciliation, persistence, and reviewed shutdown completed
  without a trading-state ambiguity.

The tracked production runtime SHA-256 is
`801e5662fe09b429e8a803e1260baee34d09b08a594fffb867312091ba6f32ba`.

### 4.3 Strategy-generated BUY/SELL qualification

A temporary, reviewed SPY-only copy exercised the repository's existing
`ETFMomentumStrategy`, not a new strategy:

- Historical IEX calibration selected a test-only threshold of `0.0005`
  (8/10 sessions completed within 90 minutes; 42-minute median).
- One strategy BUY filled one SPY share at $768.35.
- The existing strategy SELL branch then filled one SPY share at $767.85.
- No partial fill, overlap, risk error, or reconciliation error occurred.
- The account ended flat with zero open orders and the temporary source was
  removed.
- Evidence is retained under
  `25_live_trading/output/alpaca_roundtrip_rehearsal/`.

The tracked runtime script `04_alpaca_paper_trading_full_session_runtime.py`
is frozen. Do not lower its production threshold, add symbols to force
activity, or repeat its accepted qualification without a concrete regression.

## 5. Notebook 02 qualified configuration

The synchronized `.py`/`.ipynb` pair uses:

| Parameter | Value |
|---|---:|
| `RIDGE_ALPHA` | `1_000_000.0` |
| `PRIMARY_LABEL` | `fwd_ret_21d` |
| `LIVE_WINDOW_START` | `2025-01-01` |
| `FORWARD_HORIZON_DAYS` | `21` |
| `TOP_K` | `5` |
| `CASH_BUFFER` | `0.02` |
| `REBALANCE_EVERY_N_DAYS` | `21` trading days |
| `INITIAL_CASH` | `$100,000` (offline replay) |
| `COMMISSION_RATE` | `0.0005` (offline replay) |
| `NOTIONAL_PER_LEG_USD` | `$5,000` |
| Default execution switches | `REFRESH_DATA=False`, `SUBMIT_PAPER_ORDERS=False` |

Only `REFRESH_DATA` and `SUBMIT_PAPER_ORDERS` vary operationally. The fixed
`LIVE_WINDOW_START` also anchors the 21-trading-day schedule and must not be
moved to manufacture eligibility.

The completion checks keep the paired sources synchronized and include a
Jupytext pair-diff/synchronization check, Ruff lint/format checks, Python
compilation, and executed Papermill evidence. Results are retained in
`.../reviews/c3_replay_fix/packet/checks.txt`. A notebook execution is not
accepted merely because the tracked `.py` file compiles.

### 5.1 Frozen-data dry run

- Frozen ETF panel: 470,662 rows, 100 symbols, through 2025-12-31.
- Features: 57 model columns; joined training/prediction panel complete.
- Training: 443,562 rows through the lookahead-safe cutoff 2024-12-31;
  labels available as of 2024-11-29.
- Ridge fit: coefficient L2 norm `0.0025`, intercept `0.006148`.
- Predictions: 25,000 rows on 250 dates.
- Post-fix replay: 84 signals/orders, all 84 filled, zero refused.
- Latest frozen scheduled basket (2025-12-04): EWY, ITB, SMH, SOXX, XME.
- Basket parity: 5/5; execution plane: five dry-run legs and no broker
  connection.

Evidence is retained locally under
`25_live_trading/output/deployment_completion_audit/runs/frozen-postfix/`.

### 5.2 Refreshed-data dry run

The first refreshed run completed after the 18:30 ET gate against the
independent deployment copy:

- Refresh appended 18,216 rows across 100 symbols.
- ETF panel: 488,878 rows, 100 symbols, 2006-01-03 through 2026-09-24.
- Features: 57 model columns; 468,562 joined rows.
- There were zero duplicate `(symbol, timestamp)` keys and zero null core bar
  fields; the regenerated combined parquet exactly equalled the concatenated
  symbol partitions.
- All 470,662 frozen-prefix rows remained value-identical to the protected
  2025-12-31 source.
- The fixed training window still reported 443,562 rows. Model, imputer,
  scaler, and feature-list artefacts were byte-identical to the frozen run;
  coefficients and intercept were exactly equal (reported L2 norm `0.0025`,
  intercept `0.006148`).
- Predictions: 43,216 rows on 433 dates.
- Replay: 140 signals/orders, all 140 filled, zero pending and zero refused.
- Latest scheduled rebalance: 2026-09-08.
- Latest staged dry-run basket: EWY, ITB, SMH, SOXX, UNG.
- Basket parity: 5/5; execution plane: five dry-run legs, zero attempts, and
  no broker connection.
- FRED macro coverage ended 2026-08-20. Backward as-of joins carried that
  last observation across 24 later ETF dates / 2,316 joined rows.
- At the frozen/append boundary, absolute close-to-close return magnitudes
  had median `0.513%`, 95th percentile `3.725%`, and maximum `5.143%`; only
  BIL exceeded the audit's three-sigma diagnostic threshold.
- The protected ETF, macro, label, sidecar, source, lock, and runtime hashes
  all remained unchanged.

Evidence and detailed integrity metrics are retained locally under
`25_live_trading/output/deployment_completion_audit/runs/refreshed-2026-09-24/`
and `.../validations/refreshed-2026-09-24.md`.

## 6. Data policy

### 6.1 Immutable research baseline

Define the local operating paths once:

```bash
REPO=/home/coolermichael/ml4t-deployment-clean
ML4T_UV=/home/coolermichael/.local/share/ml4t-tools/uv-0.12.5/uv
FROZEN_DATA=/home/coolermichael/machine-learning-for-trading/data
DEPLOY_DATA=/home/coolermichael/ml4t-deployment-data
```

The preserved source is rooted at `$FROZEN_DATA` with the ETF snapshot
`etfs/market/etf_universe.parquet.bak-2025-12-31`. Its recorded SHA-256 is
`14a055e7fac091bc03b927c2d46e3596ebbbc60504008b03d43e2472cde1db9d`.
The frozen FRED parquet SHA-256 is
`bc8c8bdd718f14a8508fae3353d906f0f2b38ab8156855273ef5db1810560454`.
The required `fwd_ret_21d` label parquet and its digest sidecar have SHA-256
values `44ee2744395114415ccee00a79f7b4f2b12bf6c41a55cc6b99c8063e26449cfe`
and `3380370afabb903a45f6a84c20b1f2d7a347bffea58fdb993b0c4d30bac330a7`.
They are notebook prerequisites and are not refreshed by notebook 02.

Never set `ML4T_DATA_PATH` to the frozen research root for a refreshed run.
The source manifests are retained in
`25_live_trading/output/deployment_completion_audit/data_manifests/`.

### 6.2 Mutable deployment copy

$DEPLOY_DATA is a real independent copy, with
no symlinks or hard links to the frozen source. It contains the ETF combined
file, 100 per-symbol partitions, and the macro inputs notebook 02 consumes.
Refreshes may mutate only this copy.

Before every refresh, verify inside the same process context that
`ML4T_DATA_PATH` resolves to the deployment copy and not inside the frozen
root. If this copy becomes invalid, verify the frozen hashes first, then
discard and recreate only the deployment copy.

### 6.3 Refresh limitations

- Refresh is allowed only after the final session's data is complete. On a
  trading day, do not start before 18:30 America/New_York.
- `ETFDataManager` appends Yahoo auto-adjusted bars. Historical adjustments
  can have different vintages across download dates, so the return at the
  frozen/append boundary must be measured and reported rather than assumed
  continuous.
- Notebook 02 does not refresh FRED macro data. Backward as-of joins carry
  the last available macro observation into later ETF dates. Record the macro
  coverage end and this carry-forward behavior; do not silently describe the
  resulting features as freshly observed macro data.
- Refreshing extends predictions. It does not extend the training cutoff;
  model coefficients should therefore remain identical if historical inputs
  are unchanged.

## 7. Operator procedure

Run from `$REPO`. The examples use Bash ANSI-C quoting so
Papermill receives actual YAML booleans. **Do not use**
`-p REFRESH_DATA false`: that injects the truthy string `"false"`.
The deployment-specific `--no-sync` rule overrides the repository's general
Papermill example in `docs/running-notebooks.md`.

### 7.1 Frozen offline qualification

Use a read-only frozen data view and empty credential variables. Construct the
view by exposing the preserved `.bak-2025-12-31` ETF snapshot as
`etfs/market/etf_universe.parquet` and copying the frozen macro inputs, without
modifying either source. The retained audit run used a filesystem/network
sandbox; the essential invocation is:

```bash
env -u ALPACA_API_KEY -u ALPACA_SECRET_KEY \
  -u APCA_API_KEY_ID -u APCA_API_SECRET_KEY \
  ML4T_DATA_PATH=/path/to/read-only-frozen-view \
  PYTHONPATH=25_live_trading \
  ML4T_CHAPTER_OUTPUT_DIR=/path/to/frozen-run-output \
  "$ML4T_UV" \
  run --no-sync papermill \
  25_live_trading/02_etfs_deployment_loop.ipynb \
  /path/to/frozen-run-output/02_etfs_deployment_loop.executed.ipynb \
  --cwd . -k python3 \
  -y $'REFRESH_DATA: false\nSUBMIT_PAPER_ORDERS: false'
```

### 7.2 Refresh plus dry run

Run only after the refresh timing gate and before any armed attempt:

```bash
env -u ALPACA_API_KEY -u ALPACA_SECRET_KEY \
  -u APCA_API_KEY_ID -u APCA_API_SECRET_KEY \
  ML4T_DATA_PATH="$DEPLOY_DATA" \
  PYTHONPATH=25_live_trading \
  ML4T_CHAPTER_OUTPUT_DIR=/path/to/date-stamped-dry-run-output \
  "$ML4T_UV" \
  run --no-sync papermill \
  25_live_trading/02_etfs_deployment_loop.ipynb \
  /path/to/date-stamped-dry-run-output/02_etfs_deployment_loop.executed.ipynb \
  --cwd . -k python3 \
  -y $'REFRESH_DATA: true\nSUBMIT_PAPER_ORDERS: false'
```

Require all of the following before arming:

1. latest data date is the completed scheduled rebalance session;
2. prediction coverage reaches that date;
3. replay has no true refusals;
4. target-basket parity passes;
5. no prior armed attempt exists for that rebalance date;
6. Alpaca PAPER identity is explicit, account is ACTIVE/unblocked, all
   positions are flat, all-symbol open orders are zero, and buying power
   exceeds the intended basket;
7. intended symbols, scores, reference prices, quantities, and total notional
   have been recorded;
8. Claude C4 review is PASS or PASS WITH NOTES with only non-blocking notes.

### 7.3 One armed PAPER run

Inject `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` through the approved host
mechanism without printing them. The `.env` form below identifies the
reviewed mechanism, not secret contents:

```bash
ML4T_DATA_PATH="$DEPLOY_DATA" \
PYTHONPATH=25_live_trading \
ML4T_CHAPTER_OUTPUT_DIR=/path/to/date-stamped-armed-run-output \
"$ML4T_UV" \
run --no-sync --env-file .env papermill \
25_live_trading/02_etfs_deployment_loop.ipynb \
/path/to/date-stamped-armed-run-output/02_etfs_deployment_loop.executed.ipynb \
--cwd . -k python3 \
-y $'REFRESH_DATA: true\nSUBMIT_PAPER_ORDERS: true'
```

Arm **at most once** for a scheduled rebalance date. If any leg may have
reached the provider, never rerun the armed notebook because one leg failed,
Papermill failed, or the connection became ambiguous. Reconcile every
intended leg read-only using provider order and position state.

Because the authorized wake is after the regular close, accepted market DAY
orders should be expected to queue for the next regular open; their realized
notional can differ from `quantity × reference_close`. After any possible
submission, a one-time read-only check at about 10:00 ET on the next normal
session is mandatory. No order submission, cancellation, replacement, or exit
is part of that check.

After the armed run, compare its `latest_cross_section_ts`,
`intended_basket`, `ref_price`, and each recorded quantity with the immediately
preceding dry-run intent. Derive each dry-run quantity from the retained dry-run
`execution[].ref_price` using `max(int(5000 // ref_price), 1)`, because the
notebook does not populate `qty` in dry-run mode. Any difference triggers
read-only provider reconciliation and reporting only—never a rerun.

If the armed run stops before writing its JSON run record, treat the one
attempt as consumed. Use the executed notebook's `exec_summary` output plus
read-only provider orders, positions, and open orders; do not rerun to obtain a
record.

## 8. Schedule and eligibility

Notebook 02's own `rebalance_log` is authoritative. Submission is eligible
only when all four notebook conditions hold:

1. Alpaca credentials are present;
2. `SUBMIT_PAPER_ORDERS=True`;
3. `REFRESH_DATA=True`;
4. latest replay rebalance date equals the latest prediction date.

The refreshed 2026-09-24 dry run reported the latest scheduled rebalance as
2026-09-08, so it correctly remained in dry-run mode. The next expected
21-trading-day rebalance is 2026-10-07; recompute and confirm it from the
fresh dry run before any order-enabled execution.

The currently projected sequence is 2026-09-08, 2026-10-07, 2026-11-05,
and 2026-12-07. These dates are operational hints only; the freshly executed
notebook's `rebalance_log` remains authoritative. The 2026-10-07 event is the
only armed run authorized by the completion mandate. Later dates are dry-run
schedule checks only unless a separate reviewed decision addresses the open
BUY-only basket and explicitly authorizes another attempt.

If the fresh `rebalance_log` does not report 2026-10-07 as the eligible latest
rebalance, the scheduled workflow stops without an armed run; changing the
event date requires a new reviewed decision.

A single agent wake-up around 18:30 ET on the actual next eligible date may
run refresh, dry-run validation, account preflight, C4 review, and—only if
eligible—the one armed attempt. It must wake an agent, not invoke Papermill
directly. Do not create a recurring armed schedule.

> **One-shot task identity:** `0707ae89-c982-40f3-b8a6-720fc2535721`  
> **Wake:** 2026-10-07 at 18:30 America/New_York (`2026-10-07T22:30:00Z`)  
> **Manual fallback:** run the refresh/dry-run procedure above after 18:30 ET
> on the authoritative rebalance date, then proceed to the armed command only
> after every listed gate passes.

## 9. Actual execution and risk path

Notebook 02 constructs `AlpacaBroker(paper=True)` directly and submits each
leg with `submit_order_async`. It does **not** wrap basket submission with
`SafeBroker`.

Consequences:

- orders are BUY-only market equity orders (the qualified installed adapter
  maps equities to DAY);
- each quantity is `max(int(5000 // reference_close), 1)`;
- the intended five-leg basket is roughly $25,000, subject to prices and the
  one-share minimum;
- legs are submitted sequentially, and the notebook continues after an
  individual leg exception;
- a local `submitted` result means broker acknowledgement, not a fill;
- the notebook's local `order_id` capture may fall back to the order's string
  representation with the installed order object, so provider read-only
  reconciliation is authoritative;
- basket parity runs after submission, making the immediately preceding dry
  run mandatory;
- there is no SafeBroker position cap, order-value cap, daily-loss limit,
  kill switch, duplicate suppression, buying-power gate, or persisted startup
  reconciliation in this path.

The operative safety boundary is therefore procedural: PAPER-only routing,
whole-account flat/open-order preflight, refresh/dry-run parity immediately
before execution, exact basket/notional review, one attempt per rebalance,
and no automatic retry after any possible submission.

The default policy is to leave an attributed PAPER basket open because this
repository path is BUY-only. It must not be described as continuous
portfolio management. A one-time housekeeping exit requires separate,
quantity-specific review and evidence.

A flat-account gate is required for the one authorized 2026-10-07 rehearsal.
If that run leaves a basket open, the same gate intentionally blocks later
armed rebalances; do not relax it or stack another BUY basket automatically.

## 10. Chapter 26 governance mapping

Only the right-sized Chapter 26 concepts apply:

- **Notebook/operator technical gates:** missing/stale data, invalid feature
  coverage, rejected replay orders, broker exceptions, wrong account mode,
  open-order conflicts, persistence failures, and environment/source drift.
- **Concept-only statistical review:** changed coefficients under a fixed
  training window, changing score distributions, rank/basket churn, degrading
  forward IC, and performance drift. Notebook 02 does not compute a continuous
  drift monitor; these are manual review items, not automatic retraining or
  trading triggers.
- **Operator data-integrity gates:** immutable frozen hashes, an independent
  mutable data root, no current incomplete daily bar, prefix-value checks,
  append-boundary diagnostics, macro-coverage disclosure, and
  prediction-to-feature coverage.
- **Notebook plus audit lineage:** source and lock hashes, package commit
  identities, data manifests, Papermill parameters, executed notebooks, fitted
  artefacts, replay dispositions, basket parity, and per-run metadata are
  retained in `25_live_trading/output/deployment_completion_audit/`.
- **Provider-truth execution evidence:** provider order IDs, statuses, filled
  quantities, fill prices, positions, and open orders—not the notebook's local
  `submitted` label—support execution claims.
- **Operator model rollback boundary:** keep the frozen baseline and prior run
  artefacts; stop when fixed-window coefficients change unexpectedly. Rollback
  means restoring code/data inputs for a later qualified run, never reversing
  broker positions automatically.
- **Procedural circuit-breaker boundary:** preflight gates and the no-rerun rule
  stop the workflow. Notebook 02 does not implement Chapter 26's automated
  breaker state machine.

This deployment does not add Feast infrastructure, MLflow servers,
dashboards, CI/CD systems, daemons, or custom monitoring services merely for
completeness.

## 11. Known limitations

- The governing deployment mandate records 17 pre-existing repository test
  failures outside this deployment's scope; it is retained at
  `.../reviews/c1_initial/packet/MANDATE.md`. The entire repository is **not**
  claimed green.
- Notebook 02's live submission is BUY-only and does not manage existing
  holdings across rebalances.
- Acknowledged orders are not equivalent to filled orders; provider
  reconciliation is required after an armed run.
- The notebook has no built-in one-attempt ledger, so audit history and
  provider order history enforce the one-armed-run rule.
- The provider submission occurs before the later parity assertion.
- Yahoo adjustment vintages can create an append-boundary return
  discontinuity.
- FRED macro data is not refreshed by notebook 02 and later ETF rows use the
  latest available macro observation through backward as-of joins.
- The Ridge model is refit on each notebook invocation, but its training
  cutoff remains pre-`LIVE_WINDOW_START`; this is not a configured periodic
  model-update system.
- No live-money path has been qualified or authorized.
- The deployment is not high-availability and does not provide unattended
  recovery from ambiguous orders.
- A-MIN source/runbook files and all execution evidence remain local and
  intentionally untracked/ignored; this document is the only primary tracked
  deployment record.

## 12. Current ordinary next action

At the scheduled one-time wake on 2026-10-07 at 18:30 ET, run a fresh
refresh/dry-run, PAPER account preflight, and Claude C4 review.
Execute the armed notebook at most once only if the notebook itself reports
eligibility.

Until then, the supported state is: runtime 04 frozen; notebook 02 qualified
offline; future scheduled PAPER basket pending.
