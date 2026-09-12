# Phase 0 Implementation Plan: Binance Perp Collector + freqtrade Skeleton

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Dockerized always-on collector that lands Binance USDⓈ-M perpetual derivatives metrics, liquidations, funding and Deribit DVOL into a local Parquet lake (with Binance Vision backfill), plus a freqtrade dry-run skeleton proving download → backtest → dry-run → FreqUI works.

**Architecture:** One Python package `collector` (uv-managed) with pure parser functions, a Parquet/DuckDB lake module, a rate-limited Binance REST client, a reconnecting WebSocket runner, and small per-task modules supervised by an asyncio scheduler; a typer CLI exposes `run / backfill / compact / status`. freqtrade runs from the official image with a checked-in config and sample strategy. Both run under one docker-compose file.

**Tech Stack:** Python 3.12, uv, httpx, websockets, pyarrow, duckdb, pydantic-settings, typer, tenacity; pytest + pytest-asyncio + respx; Docker Compose; freqtrade official image.

**Spec:** `docs/superpowers/specs/2026-09-13-phase0-collector-freqtrade-design.md`

## Global Constraints

- Python `>=3.12,<3.13`; dependencies pinned via `uv.lock`; Docker image base `python:3.12-slim`.
- All timestamp columns are `int64` milliseconds UTC and end in `_ms`; numeric columns are `float64`; `symbol` is `string`; every dataset has `ingest_ms`.
- Lake path layout: `<data_dir>/lake/<dataset>/date=YYYY-MM-DD/part-<ingest_ms>.parquet`; compact writes `compacted-<ingest_ms>.parquet` and deletes the older files.
- Environment variables use prefix `TRADER_`; defaults must work with no `.env` present.
- Rate limits: `/futures/data/*` token bucket 800 per 300 s; other `/fapi/*` bucket 2000 weight per 60 s; 429 honours `Retry-After` (default 60 s); 418 sleeps 300 s.
- WebSocket reconnect backoff starts at 1 s, doubles, caps at 60 s.
- No exchange API keys anywhere; freqtrade `dry_run` must be `true`.
- Commit messages: plain conventional style, no AI attribution lines.
- Tests run with `uv run pytest` from repo root; network tests are marked `network` and excluded by default.

---

## File Structure

| Path | Responsibility |
|---|---|
| `pyproject.toml` | uv project, package metadata, pytest config, `collector` console script |
| `collector/src/collector/__init__.py` | version string |
| `collector/src/collector/config.py` | `Settings` from env (`TRADER_` prefix) |
| `collector/src/collector/lake.py` | dataset schema registry, `write_rows`, `compact`, `view`, `latest_partition` |
| `collector/src/collector/state.py` | `StateStore` for `state.json` health tracking |
| `collector/src/collector/binance/parsers.py` | pure functions: Binance JSON → normalized rows |
| `collector/src/collector/binance/rest.py` | `TokenBucket`, `BinanceRest` with 429/418 handling |
| `collector/src/collector/binance/ws.py` | `backoff_delays`, `run_stream` reconnect loop |
| `collector/src/collector/binance/vision.py` | Binance Vision URL builder, checksum verify, CSV parsers, async downloader |
| `collector/src/collector/deribit.py` | `parse_dvol`, `fetch_dvol` |
| `collector/src/collector/scheduler.py` | `run_periodic`, `supervise` |
| `collector/src/collector/tasks/context.py` | `Context` dataclass shared by tasks |
| `collector/src/collector/tasks/symbols.py` | exchangeInfo snapshot + `load_symbols` |
| `collector/src/collector/tasks/metrics.py` | five `/futures/data` endpoints |
| `collector/src/collector/tasks/premium.py` | premiumIndex all symbols |
| `collector/src/collector/tasks/funding.py` | settled funding history |
| `collector/src/collector/tasks/liquidations.py` | forceOrder stream → lake |
| `collector/src/collector/tasks/dvol.py` | Deribit DVOL hourly |
| `collector/src/collector/backfill.py` | Binance Vision backfill orchestration |
| `collector/src/collector/cli.py` | typer app: `run`, `backfill`, `compact`, `status` |
| `collector/Dockerfile` | image build |
| `docker-compose.yml`, `.env.example` | services |
| `freqtrade/user_data/config.json` | Binance futures dry-run config |
| `freqtrade/user_data/strategies/EmaCrossSample.py` | flow-validation strategy |
| `collector/tests/**` | one test module per source module, fixtures under `collector/tests/fixtures/` |

Note on scope: the spec has two subsystems. The freqtrade skeleton is three files with no Python logic, so it is kept in this plan as the final two tasks rather than a separate plan.

---

### Task 1: Project scaffold with uv

**Files:**
- Create: `pyproject.toml`
- Create: `collector/src/collector/__init__.py`
- Create: `collector/tests/__init__.py`
- Create: `collector/tests/test_version.py`
- Modify: `.gitignore`

**Interfaces:**
- Produces: importable package `collector` with `__version__ = "0.1.0"`; `uv run pytest` works from repo root.

- [ ] **Step 1: Write pyproject.toml**

```toml
[project]
name = "collector"
version = "0.1.0"
description = "Binance USDS-M perpetual derivatives data collector"
requires-python = ">=3.12,<3.13"
dependencies = [
    "httpx>=0.27",
    "websockets>=13",
    "pyarrow>=17",
    "duckdb>=1.1",
    "pydantic-settings>=2.5",
    "typer>=0.12",
    "tenacity>=9",
]

[project.scripts]
collector = "collector.cli:app"

[dependency-groups]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.24",
    "respx>=0.21",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["collector/src/collector"]

[tool.pytest.ini_options]
testpaths = ["collector/tests"]
asyncio_mode = "auto"
markers = ["network: hits real external APIs; excluded by default"]
addopts = "-m 'not network'"
```

- [ ] **Step 2: Create package and failing test**

`collector/src/collector/__init__.py`:
```python
__version__ = "0.1.0"
```

`collector/tests/__init__.py`: empty file.

`collector/tests/test_version.py`:
```python
from collector import __version__


def test_version_is_semver():
    parts = __version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)
```

- [ ] **Step 3: Sync and run test**

Run: `uv sync` then `uv run pytest -v`
Expected: 1 passed. If `uv sync` complains about the package path, check `[tool.hatch.build.targets.wheel] packages` matches `collector/src/collector`.

- [ ] **Step 4: Extend .gitignore**

Append to `.gitignore`:
```
# uv
.venv/

# freqtrade runtime
freqtrade/user_data/data/
freqtrade/user_data/logs/
freqtrade/user_data/backtest_results/
freqtrade/user_data/hyperopt_results/
freqtrade/user_data/*.sqlite*
freqtrade/user_data/plot/
```

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock collector/ .gitignore
git commit -m "Scaffold collector package with uv and pytest"
```

---

### Task 2: Settings from environment

**Files:**
- Create: `collector/src/collector/config.py`
- Test: `collector/tests/test_config.py`

**Interfaces:**
- Produces: `Settings` (pydantic BaseSettings) with fields `data_dir: Path`, `binance_fapi_base: str`, `binance_ws_base: str`, `deribit_base: str`, `metrics_interval_s: int`, `premium_interval_s: int`, `dvol_interval_s: int`, `log_level: str`; `get_settings() -> Settings`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_config.py`:
```python
import os
from pathlib import Path

from collector.config import Settings


def test_defaults_when_env_empty(monkeypatch):
    for k in list(os.environ):
        if k.startswith("TRADER_"):
            monkeypatch.delenv(k)
    s = Settings(_env_file=None)
    assert s.data_dir == Path("/data")
    assert s.binance_fapi_base == "https://fapi.binance.com"
    assert s.binance_ws_base == "wss://fstream.binance.com/market"
    assert s.deribit_base == "https://www.deribit.com"
    assert s.metrics_interval_s == 3600
    assert s.premium_interval_s == 60
    assert s.dvol_interval_s == 3600
    assert s.log_level == "INFO"


def test_env_prefix_overrides(monkeypatch):
    monkeypatch.setenv("TRADER_DATA_DIR", "C:/tmp/lake")
    monkeypatch.setenv("TRADER_PREMIUM_INTERVAL_S", "15")
    s = Settings(_env_file=None)
    assert s.data_dir == Path("C:/tmp/lake")
    assert s.premium_interval_s == 15
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.config'`

- [ ] **Step 3: Implement config.py**

```python
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TRADER_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    data_dir: Path = Path("/data")
    binance_fapi_base: str = "https://fapi.binance.com"
    binance_ws_base: str = "wss://fstream.binance.com/market"
    deribit_base: str = "https://www.deribit.com"
    metrics_interval_s: int = 3600
    premium_interval_s: int = 60
    dvol_interval_s: int = 3600
    log_level: str = "INFO"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_config.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/config.py collector/tests/test_config.py
git commit -m "Add Settings loaded from TRADER_ environment variables"
```

---

### Task 3: Lake schema registry and partitioned Parquet writes

**Files:**
- Create: `collector/src/collector/lake.py`
- Test: `collector/tests/test_lake_write.py`

**Interfaces:**
- Produces: `DatasetSpec(name, schema: pa.Schema, keys: tuple[str, ...], time_col: str | None, date_col: str | None)`; `DATASETS: dict[str, DatasetSpec]` with the 13 datasets from spec §4.3; `date_from_ms(ms: int) -> str`; `write_rows(data_dir: Path, dataset: str, rows: list[dict], ingest_ms: int | None = None) -> list[Path]`; `lake_dir(data_dir, dataset) -> Path`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_lake_write.py`:
```python
from datetime import datetime, timezone

import pyarrow.parquet as pq

from collector import lake


def ms(y, m, d, h=0):
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


def test_registry_has_all_datasets_with_ingest_ms():
    expected = {
        "symbols", "oi_hist", "ls_global_account", "ls_top_position", "ls_top_account",
        "taker_ratio", "premium_index", "funding_rate", "liquidations", "dvol",
        "klines_1h", "klines_5m", "vision_metrics", "vision_funding",
    }
    assert set(lake.DATASETS) == expected
    for spec in lake.DATASETS.values():
        assert "ingest_ms" in spec.schema.names
        assert spec.keys
        assert (spec.time_col is None) != (spec.date_col is None)


def test_date_from_ms():
    assert lake.date_from_ms(ms(2026, 9, 13, 5)) == "2026-09-13"


def test_write_rows_splits_by_utc_date(tmp_path):
    rows = [
        {"symbol": "BTCUSDT", "timestamp_ms": ms(2026, 9, 12, 23), "sum_open_interest": 1.0,
         "sum_open_interest_value": 2.0, "ingest_ms": 1},
        {"symbol": "BTCUSDT", "timestamp_ms": ms(2026, 9, 13, 0), "sum_open_interest": 3.0,
         "sum_open_interest_value": 4.0, "ingest_ms": 1},
    ]
    paths = lake.write_rows(tmp_path, "oi_hist", rows, ingest_ms=1)
    assert len(paths) == 2
    assert {p.parent.name for p in paths} == {"date=2026-09-12", "date=2026-09-13"}
    assert all(p.name == "part-1.parquet" for p in paths)
    t = pq.read_table(paths[0])
    assert t.schema.field("timestamp_ms").type == "int64"
    assert t.schema.field("sum_open_interest").type == "double"


def test_write_rows_uses_date_col_for_symbols(tmp_path):
    rows = [{"snapshot_date": "2026-09-13", "symbol": "BTCUSDT", "pair": "BTCUSDT",
             "contract_type": "PERPETUAL", "status": "TRADING", "onboard_date_ms": 0,
             "price_precision": 2, "quantity_precision": 3, "ingest_ms": 5}]
    paths = lake.write_rows(tmp_path, "symbols", rows, ingest_ms=5)
    assert paths[0].parent.name == "date=2026-09-13"


def test_write_rows_empty_is_noop(tmp_path):
    assert lake.write_rows(tmp_path, "oi_hist", []) == []


def test_write_rows_never_overwrites(tmp_path):
    row = {"symbol": "BTCUSDT", "timestamp_ms": ms(2026, 9, 13), "sum_open_interest": 1.0,
           "sum_open_interest_value": 2.0, "ingest_ms": 7}
    a = lake.write_rows(tmp_path, "oi_hist", [row], ingest_ms=7)[0]
    b = lake.write_rows(tmp_path, "oi_hist", [row], ingest_ms=7)[0]
    assert a != b and a.exists() and b.exists()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_lake_write.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.lake'`

- [ ] **Step 3: Implement lake.py (registry + write)**

```python
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

S, I, F = pa.string(), pa.int64(), pa.float64()


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    schema: pa.Schema
    keys: tuple[str, ...]
    time_col: str | None = None
    date_col: str | None = None


def _schema(*cols: tuple[str, pa.DataType]) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t in cols] + [pa.field("ingest_ms", I)])


_LS = _schema(("symbol", S), ("timestamp_ms", I), ("long_short_ratio", F),
              ("long_account", F), ("short_account", F))
_KLINE = _schema(("symbol", S), ("open_time_ms", I), ("open", F), ("high", F), ("low", F),
                 ("close", F), ("volume", F), ("close_time_ms", I), ("quote_volume", F),
                 ("trade_count", I), ("taker_buy_base", F), ("taker_buy_quote", F))

DATASETS: dict[str, DatasetSpec] = {
    "symbols": DatasetSpec("symbols", _schema(
        ("snapshot_date", S), ("symbol", S), ("pair", S), ("contract_type", S), ("status", S),
        ("onboard_date_ms", I), ("price_precision", I), ("quantity_precision", I)),
        keys=("snapshot_date", "symbol"), date_col="snapshot_date"),
    "oi_hist": DatasetSpec("oi_hist", _schema(
        ("symbol", S), ("timestamp_ms", I), ("sum_open_interest", F), ("sum_open_interest_value", F)),
        keys=("symbol", "timestamp_ms"), time_col="timestamp_ms"),
    "ls_global_account": DatasetSpec("ls_global_account", _LS, ("symbol", "timestamp_ms"), "timestamp_ms"),
    "ls_top_position": DatasetSpec("ls_top_position", _LS, ("symbol", "timestamp_ms"), "timestamp_ms"),
    "ls_top_account": DatasetSpec("ls_top_account", _LS, ("symbol", "timestamp_ms"), "timestamp_ms"),
    "taker_ratio": DatasetSpec("taker_ratio", _schema(
        ("symbol", S), ("timestamp_ms", I), ("buy_sell_ratio", F), ("buy_vol", F), ("sell_vol", F)),
        keys=("symbol", "timestamp_ms"), time_col="timestamp_ms"),
    "premium_index": DatasetSpec("premium_index", _schema(
        ("symbol", S), ("time_ms", I), ("mark_price", F), ("index_price", F),
        ("estimated_settle_price", F), ("last_funding_rate", F), ("interest_rate", F),
        ("next_funding_time_ms", I)),
        keys=("symbol", "time_ms"), time_col="time_ms"),
    "funding_rate": DatasetSpec("funding_rate", _schema(
        ("symbol", S), ("funding_time_ms", I), ("funding_rate", F), ("mark_price", F), ("rate_type", S)),
        keys=("symbol", "funding_time_ms"), time_col="funding_time_ms"),
    "liquidations": DatasetSpec("liquidations", _schema(
        ("event_time_ms", I), ("symbol", S), ("side", S), ("order_type", S), ("time_in_force", S),
        ("orig_qty", F), ("price", F), ("avg_price", F), ("status", S), ("last_filled_qty", F),
        ("filled_accum_qty", F), ("trade_time_ms", I)),
        keys=("symbol", "trade_time_ms", "side", "orig_qty", "price"), time_col="trade_time_ms"),
    "dvol": DatasetSpec("dvol", _schema(
        ("currency", S), ("timestamp_ms", I), ("open", F), ("high", F), ("low", F), ("close", F)),
        keys=("currency", "timestamp_ms"), time_col="timestamp_ms"),
    "klines_1h": DatasetSpec("klines_1h", _KLINE, ("symbol", "open_time_ms"), "open_time_ms"),
    "klines_5m": DatasetSpec("klines_5m", _KLINE, ("symbol", "open_time_ms"), "open_time_ms"),
    "vision_metrics": DatasetSpec("vision_metrics", _schema(
        ("symbol", S), ("create_time_ms", I), ("sum_open_interest", F), ("sum_open_interest_value", F),
        ("count_toptrader_long_short_ratio", F), ("sum_toptrader_long_short_ratio", F),
        ("count_long_short_ratio", F), ("sum_taker_long_short_vol_ratio", F)),
        keys=("symbol", "create_time_ms"), time_col="create_time_ms"),
    "vision_funding": DatasetSpec("vision_funding", _schema(
        ("symbol", S), ("calc_time_ms", I), ("funding_interval_hours", F), ("last_funding_rate", F)),
        keys=("symbol", "calc_time_ms"), time_col="calc_time_ms"),
}


def now_ms() -> int:
    return int(time.time() * 1000)


def date_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def lake_dir(data_dir: Path, dataset: str) -> Path:
    return Path(data_dir) / "lake" / dataset


def _row_date(spec: DatasetSpec, row: dict) -> str:
    if spec.date_col:
        return str(row[spec.date_col])
    return date_from_ms(int(row[spec.time_col]))


def _unique_path(directory: Path, stem: str) -> Path:
    candidate = directory / f"{stem}.parquet"
    n = 1
    while candidate.exists():
        candidate = directory / f"{stem}-{n}.parquet"
        n += 1
    return candidate


def write_rows(data_dir: Path, dataset: str, rows: list[dict], ingest_ms: int | None = None) -> list[Path]:
    spec = DATASETS[dataset]
    if not rows:
        return []
    stamp = ingest_ms if ingest_ms is not None else now_ms()
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_date[_row_date(spec, r)].append(r)
    written: list[Path] = []
    for date, group in sorted(by_date.items()):
        part_dir = lake_dir(data_dir, dataset) / f"date={date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(group, schema=spec.schema)
        path = _unique_path(part_dir, f"part-{stamp}")
        pq.write_table(table, path, compression="zstd")
        written.append(path)
    return written
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_lake_write.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/lake.py collector/tests/test_lake_write.py
git commit -m "Add lake dataset registry and partitioned Parquet writer"
```

---

### Task 4: Lake compact and deduplicated view

**Files:**
- Modify: `collector/src/collector/lake.py` (append functions)
- Test: `collector/tests/test_lake_compact.py`

**Interfaces:**
- Consumes: `DATASETS`, `write_rows`, `lake_dir`, `now_ms` from Task 3.
- Produces: `compact(data_dir: Path, dataset: str, date: str, ingest_ms: int | None = None) -> tuple[int, int]` returning `(files_removed, rows_written)`; `view(data_dir: Path, dataset: str, con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyRelation` (deduplicated, includes hive `date` column); `partitions(data_dir, dataset) -> list[str]`; `file_count(data_dir, dataset) -> int`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_lake_compact.py`:
```python
from datetime import datetime, timezone

import duckdb

from collector import lake


def ms(y, m, d, h=0):
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp() * 1000)


def oi(ts, value, ingest):
    return {"symbol": "BTCUSDT", "timestamp_ms": ts, "sum_open_interest": value,
            "sum_open_interest_value": 0.0, "ingest_ms": ingest}


def seed(tmp_path):
    t1, t2, t3 = ms(2026, 9, 13, 1), ms(2026, 9, 13, 2), ms(2026, 9, 13, 3)
    lake.write_rows(tmp_path, "oi_hist", [oi(t1, 1.0, 100), oi(t2, 2.0, 100)], ingest_ms=100)
    lake.write_rows(tmp_path, "oi_hist", [oi(t2, 22.0, 200), oi(t3, 3.0, 200)], ingest_ms=200)
    return t1, t2, t3


def test_view_dedups_before_compact(tmp_path):
    t1, t2, t3 = seed(tmp_path)
    rel = lake.view(tmp_path, "oi_hist")
    rows = rel.order("timestamp_ms").fetchall()
    cols = [d[0] for d in rel.description]
    got = {r[cols.index("timestamp_ms")]: r[cols.index("sum_open_interest")] for r in rows}
    assert got == {t1: 1.0, t2: 22.0, t3: 3.0}
    assert "date" in cols


def test_compact_rewrites_partition(tmp_path):
    seed(tmp_path)
    assert lake.file_count(tmp_path, "oi_hist") == 2
    removed, rows = lake.compact(tmp_path, "oi_hist", "2026-09-13", ingest_ms=300)
    assert (removed, rows) == (2, 3)
    part = lake.lake_dir(tmp_path, "oi_hist") / "date=2026-09-13"
    files = sorted(p.name for p in part.glob("*.parquet"))
    assert files == ["compacted-300.parquet"]
    rel = lake.view(tmp_path, "oi_hist")
    assert rel.count("*").fetchone()[0] == 3


def test_compact_is_idempotent(tmp_path):
    seed(tmp_path)
    lake.compact(tmp_path, "oi_hist", "2026-09-13", ingest_ms=300)
    removed, rows = lake.compact(tmp_path, "oi_hist", "2026-09-13", ingest_ms=400)
    assert (removed, rows) == (1, 3)


def test_compact_missing_partition_is_noop(tmp_path):
    assert lake.compact(tmp_path, "oi_hist", "1999-01-01") == (0, 0)


def test_view_empty_dataset_returns_empty_relation(tmp_path):
    rel = lake.view(tmp_path, "dvol")
    assert rel.count("*").fetchone()[0] == 0


def test_partitions_sorted(tmp_path):
    lake.write_rows(tmp_path, "oi_hist", [oi(ms(2026, 9, 14), 1.0, 1)], ingest_ms=1)
    lake.write_rows(tmp_path, "oi_hist", [oi(ms(2026, 9, 12), 1.0, 1)], ingest_ms=1)
    assert lake.partitions(tmp_path, "oi_hist") == ["2026-09-12", "2026-09-14"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_lake_compact.py -v`
Expected: FAIL with `AttributeError: module 'collector.lake' has no attribute 'view'`

- [ ] **Step 3: Append to lake.py**

```python
import duckdb  # add to imports at top of lake.py


def _dedup_sql(spec: DatasetSpec, source: str) -> str:
    keys = ", ".join(spec.keys)
    return (
        "SELECT * EXCLUDE (rn) FROM ("
        f"SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY ingest_ms DESC) AS rn "
        f"FROM {source}) WHERE rn = 1"
    )


def partitions(data_dir: Path, dataset: str) -> list[str]:
    root = lake_dir(data_dir, dataset)
    if not root.exists():
        return []
    return sorted(p.name.split("=", 1)[1] for p in root.iterdir() if p.is_dir() and p.name.startswith("date="))


def file_count(data_dir: Path, dataset: str) -> int:
    root = lake_dir(data_dir, dataset)
    return sum(1 for _ in root.rglob("*.parquet")) if root.exists() else 0


def view(data_dir: Path, dataset: str, con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyRelation:
    spec = DATASETS[dataset]
    con = con or duckdb.connect()
    if file_count(data_dir, dataset) == 0:
        empty = spec.schema.append(pa.field("date", S)).empty_table()
        return con.from_arrow(empty)
    glob = (lake_dir(data_dir, dataset) / "**" / "*.parquet").as_posix()
    source = f"read_parquet('{glob}', hive_partitioning=true, union_by_name=true)"
    return con.sql(_dedup_sql(spec, source))


def compact(data_dir: Path, dataset: str, date: str, ingest_ms: int | None = None) -> tuple[int, int]:
    spec = DATASETS[dataset]
    part_dir = lake_dir(data_dir, dataset) / f"date={date}"
    files = sorted(part_dir.glob("*.parquet")) if part_dir.exists() else []
    if not files:
        return (0, 0)
    stamp = ingest_ms if ingest_ms is not None else now_ms()
    con = duckdb.connect()
    glob = (part_dir / "*.parquet").as_posix()
    source = f"read_parquet('{glob}', union_by_name=true)"
    table = con.sql(_dedup_sql(spec, source)).arrow()
    table = table.select(spec.schema.names).cast(spec.schema)
    out = _unique_path(part_dir, f"compacted-{stamp}")
    pq.write_table(table, out, compression="zstd")
    for f in files:
        if f != out:
            f.unlink()
    return (len(files), table.num_rows)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_lake_compact.py collector/tests/test_lake_write.py -v`
Expected: 12 passed. On Windows, if `unlink` raises `PermissionError`, make sure the DuckDB result was fully materialized with `.arrow()` before deleting (it is in the code above) and that no other connection holds the files.

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/lake.py collector/tests/test_lake_compact.py
git commit -m "Add lake compact and deduplicated DuckDB view"
```

---

### Task 5: StateStore for task health

**Files:**
- Create: `collector/src/collector/state.py`
- Test: `collector/tests/test_state.py`

**Interfaces:**
- Produces: `StateStore(path: Path)` with `mark_success(task: str, now_ms: int | None = None, detail: dict | None = None) -> None`, `mark_error(task: str, error: str, now_ms: int | None = None) -> None`, `get(task: str) -> dict`, `all() -> dict[str, dict]`, `is_healthy(task: str, interval_s: float, now_ms: int | None = None) -> bool`. Stored record shape: `{"last_success_ms": int | None, "last_error": str | None, "last_error_ms": int | None, "consecutive_errors": int, "detail": dict}`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_state.py`:
```python
import json

from collector.state import StateStore


def test_missing_file_reads_as_empty(tmp_path):
    s = StateStore(tmp_path / "state.json")
    assert s.all() == {}
    assert s.get("metrics")["last_success_ms"] is None


def test_mark_success_resets_errors_and_persists(tmp_path):
    p = tmp_path / "state.json"
    s = StateStore(p)
    s.mark_error("metrics", "boom", now_ms=1000)
    s.mark_error("metrics", "boom2", now_ms=2000)
    assert s.get("metrics")["consecutive_errors"] == 2
    s.mark_success("metrics", now_ms=3000, detail={"rows": 42})
    rec = json.loads(p.read_text())["metrics"]
    assert rec["last_success_ms"] == 3000
    assert rec["consecutive_errors"] == 0
    assert rec["last_error"] == "boom2"
    assert rec["detail"] == {"rows": 42}


def test_is_healthy_within_two_intervals(tmp_path):
    s = StateStore(tmp_path / "state.json")
    assert s.is_healthy("x", 60, now_ms=0) is False
    s.mark_success("x", now_ms=100_000)
    assert s.is_healthy("x", 60, now_ms=100_000 + 119_000) is True
    assert s.is_healthy("x", 60, now_ms=100_000 + 121_000) is False


def test_write_is_atomic_no_tmp_left(tmp_path):
    p = tmp_path / "state.json"
    s = StateStore(p)
    s.mark_success("x", now_ms=1)
    assert [f.name for f in tmp_path.iterdir()] == ["state.json"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_state.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.state'`

- [ ] **Step 3: Implement state.py**

```python
from __future__ import annotations

import json
import os
import time
from pathlib import Path

_EMPTY = {"last_success_ms": None, "last_error": None, "last_error_ms": None,
          "consecutive_errors": 0, "detail": {}}


class StateStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    def _load(self) -> dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save(self, data: dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def get(self, task: str) -> dict:
        return {**_EMPTY, **self._load().get(task, {})}

    def all(self) -> dict[str, dict]:
        return self._load()

    def mark_success(self, task: str, now_ms: int | None = None, detail: dict | None = None) -> None:
        data = self._load()
        rec = {**_EMPTY, **data.get(task, {})}
        rec["last_success_ms"] = now_ms if now_ms is not None else int(time.time() * 1000)
        rec["consecutive_errors"] = 0
        if detail is not None:
            rec["detail"] = detail
        data[task] = rec
        self._save(data)

    def mark_error(self, task: str, error: str, now_ms: int | None = None) -> None:
        data = self._load()
        rec = {**_EMPTY, **data.get(task, {})}
        rec["last_error"] = error[:500]
        rec["last_error_ms"] = now_ms if now_ms is not None else int(time.time() * 1000)
        rec["consecutive_errors"] = int(rec["consecutive_errors"]) + 1
        data[task] = rec
        self._save(data)

    def is_healthy(self, task: str, interval_s: float, now_ms: int | None = None) -> bool:
        last = self.get(task)["last_success_ms"]
        if last is None:
            return False
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        return (now - last) <= 2 * interval_s * 1000
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_state.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/state.py collector/tests/test_state.py
git commit -m "Add StateStore for per-task health tracking"
```

---

### Task 6: Binance REST response parsers

**Files:**
- Create: `collector/src/collector/binance/__init__.py` (empty)
- Create: `collector/src/collector/binance/parsers.py`
- Create: `collector/tests/fixtures/__init__.py` (empty)
- Create: `collector/tests/fixtures/binance.py`
- Test: `collector/tests/test_parsers.py`

**Interfaces:**
- Produces (all pure, return rows matching `lake.DATASETS[...]` schemas):
  - `parse_exchange_info(payload: dict, snapshot_date: str, ingest_ms: int) -> list[dict]` → `symbols` rows, filtered to `contractType == "PERPETUAL"`, `status == "TRADING"`, `quoteAsset == "USDT"`
  - `parse_oi_hist(payload: list[dict], symbol: str, ingest_ms: int) -> list[dict]` → `oi_hist`
  - `parse_ls_ratio(payload: list[dict], symbol: str, ingest_ms: int) -> list[dict]` → `ls_*`
  - `parse_taker_ratio(payload: list[dict], symbol: str, ingest_ms: int) -> list[dict]` → `taker_ratio`
  - `parse_premium_index(payload: list[dict] | dict, ingest_ms: int) -> list[dict]` → `premium_index`
  - `parse_funding_rate(payload: list[dict], ingest_ms: int) -> list[dict]` → `funding_rate`
  - `to_float(x) -> float | None` (empty string / None → None)

- [ ] **Step 1: Write fixtures**

`collector/tests/fixtures/binance.py`:
```python
EXCHANGE_INFO = {
    "symbols": [
        {"symbol": "BTCUSDT", "pair": "BTCUSDT", "contractType": "PERPETUAL",
         "deliveryDate": 4133404800000, "onboardDate": 1569398400000, "status": "TRADING",
         "pricePrecision": 2, "quantityPrecision": 3, "baseAsset": "BTC", "quoteAsset": "USDT"},
        {"symbol": "BTCUSDT_260925", "pair": "BTCUSDT", "contractType": "CURRENT_QUARTER",
         "deliveryDate": 1790000000000, "onboardDate": 1780000000000, "status": "TRADING",
         "pricePrecision": 1, "quantityPrecision": 3, "baseAsset": "BTC", "quoteAsset": "USDT"},
        {"symbol": "OLDUSDT", "pair": "OLDUSDT", "contractType": "PERPETUAL",
         "deliveryDate": 4133404800000, "onboardDate": 1600000000000, "status": "SETTLING",
         "pricePrecision": 4, "quantityPrecision": 0, "baseAsset": "OLD", "quoteAsset": "USDT"},
        {"symbol": "ETHUSDC", "pair": "ETHUSDC", "contractType": "PERPETUAL",
         "deliveryDate": 4133404800000, "onboardDate": 1700000000000, "status": "TRADING",
         "pricePrecision": 2, "quantityPrecision": 3, "baseAsset": "ETH", "quoteAsset": "USDC"},
    ]
}

OI_HIST = [
    {"symbol": "BTCUSDT", "sumOpenInterest": "20403.63700000",
     "sumOpenInterestValue": "150570784.07809979", "timestamp": 1583127900000},
    {"symbol": "BTCUSDT", "sumOpenInterest": "20401.36700000",
     "sumOpenInterestValue": "149940752.14464448", "timestamp": 1583128200000},
]

LS_RATIO = [
    {"symbol": "BTCUSDT", "longShortRatio": "0.1960", "longAccount": "0.6622",
     "shortAccount": "0.3378", "timestamp": 1583139600000},
]

TAKER_RATIO = [
    {"buySellRatio": "1.5586", "buyVol": "387.3300", "sellVol": "248.5030", "timestamp": 1585614900000},
]

PREMIUM_INDEX = [
    {"symbol": "BTCUSDT", "markPrice": "11793.63104562", "indexPrice": "11781.80495970",
     "estimatedSettlePrice": "11781.16138815", "lastFundingRate": "0.00038246",
     "interestRate": "0.00010000", "nextFundingTime": 1597392000000, "time": 1597370495002},
    {"symbol": "NEWUSDT", "markPrice": "1.5", "indexPrice": "1.49", "estimatedSettlePrice": "",
     "lastFundingRate": "0.0001", "interestRate": "0.0001", "nextFundingTime": 1597392000000,
     "time": 1597370495002},
]

FUNDING_RATE = [
    {"symbol": "BTCUSDT", "fundingTime": 1698768000000, "fundingRate": "0.00010000",
     "markPrice": "34287.54619963"},
    {"symbol": "BTCUSDT", "fundingTime": 1698796800000, "fundingRate": "0.00012000",
     "markPrice": "34400.1", "rateType": "Regular"},
]

FORCE_ORDER = {
    "e": "forceOrder", "E": 1568014460893,
    "o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.014", "p": "9910",
          "ap": "9910", "X": "FILLED", "l": "0.014", "z": "0.014", "T": 1568014460893},
}
```

- [ ] **Step 2: Write failing tests**

`collector/tests/test_parsers.py`:
```python
import pyarrow as pa

from collector import lake
from collector.binance import parsers as p
from collector.tests.fixtures import binance as fx


def assert_matches(dataset: str, rows: list[dict]):
    pa.Table.from_pylist(rows, schema=lake.DATASETS[dataset].schema)


def test_to_float():
    assert p.to_float("1.5") == 1.5
    assert p.to_float("") is None
    assert p.to_float(None) is None


def test_exchange_info_filters_to_usdt_trading_perps():
    rows = p.parse_exchange_info(fx.EXCHANGE_INFO, "2026-09-13", ingest_ms=9)
    assert [r["symbol"] for r in rows] == ["BTCUSDT"]
    r = rows[0]
    assert r == {"snapshot_date": "2026-09-13", "symbol": "BTCUSDT", "pair": "BTCUSDT",
                 "contract_type": "PERPETUAL", "status": "TRADING",
                 "onboard_date_ms": 1569398400000, "price_precision": 2,
                 "quantity_precision": 3, "ingest_ms": 9}
    assert_matches("symbols", rows)


def test_oi_hist():
    rows = p.parse_oi_hist(fx.OI_HIST, "BTCUSDT", ingest_ms=1)
    assert len(rows) == 2
    assert rows[0]["sum_open_interest"] == 20403.637
    assert rows[0]["timestamp_ms"] == 1583127900000
    assert_matches("oi_hist", rows)


def test_ls_ratio_uses_given_symbol():
    rows = p.parse_ls_ratio(fx.LS_RATIO, "ETHUSDT", ingest_ms=1)
    assert rows[0]["symbol"] == "ETHUSDT"
    assert rows[0]["long_short_ratio"] == 0.196
    assert_matches("ls_global_account", rows)


def test_taker_ratio_injects_symbol():
    rows = p.parse_taker_ratio(fx.TAKER_RATIO, "BTCUSDT", ingest_ms=1)
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["buy_vol"] == 387.33
    assert_matches("taker_ratio", rows)


def test_premium_index_list_and_empty_settle():
    rows = p.parse_premium_index(fx.PREMIUM_INDEX, ingest_ms=1)
    assert len(rows) == 2
    assert rows[1]["estimated_settle_price"] is None
    assert rows[0]["next_funding_time_ms"] == 1597392000000
    assert_matches("premium_index", rows)


def test_premium_index_single_dict():
    rows = p.parse_premium_index(fx.PREMIUM_INDEX[0], ingest_ms=1)
    assert len(rows) == 1


def test_funding_rate_optional_rate_type():
    rows = p.parse_funding_rate(fx.FUNDING_RATE, ingest_ms=1)
    assert rows[0]["rate_type"] is None
    assert rows[1]["rate_type"] == "Regular"
    assert rows[1]["funding_time_ms"] == 1698796800000
    assert_matches("funding_rate", rows)
```

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest collector/tests/test_parsers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.binance'`

- [ ] **Step 4: Implement parsers.py**

```python
from __future__ import annotations

from typing import Any


def to_float(x: Any) -> float | None:
    if x is None or x == "":
        return None
    return float(x)


def parse_exchange_info(payload: dict, snapshot_date: str, ingest_ms: int) -> list[dict]:
    rows = []
    for s in payload.get("symbols", []):
        if s.get("contractType") != "PERPETUAL" or s.get("status") != "TRADING" or s.get("quoteAsset") != "USDT":
            continue
        rows.append({
            "snapshot_date": snapshot_date,
            "symbol": s["symbol"],
            "pair": s.get("pair", s["symbol"]),
            "contract_type": s["contractType"],
            "status": s["status"],
            "onboard_date_ms": int(s.get("onboardDate", 0)),
            "price_precision": int(s.get("pricePrecision", 0)),
            "quantity_precision": int(s.get("quantityPrecision", 0)),
            "ingest_ms": ingest_ms,
        })
    return rows


def parse_oi_hist(payload: list[dict], symbol: str, ingest_ms: int) -> list[dict]:
    return [{
        "symbol": symbol,
        "timestamp_ms": int(r["timestamp"]),
        "sum_open_interest": to_float(r["sumOpenInterest"]),
        "sum_open_interest_value": to_float(r["sumOpenInterestValue"]),
        "ingest_ms": ingest_ms,
    } for r in payload]


def parse_ls_ratio(payload: list[dict], symbol: str, ingest_ms: int) -> list[dict]:
    return [{
        "symbol": symbol,
        "timestamp_ms": int(r["timestamp"]),
        "long_short_ratio": to_float(r["longShortRatio"]),
        "long_account": to_float(r["longAccount"]),
        "short_account": to_float(r["shortAccount"]),
        "ingest_ms": ingest_ms,
    } for r in payload]


def parse_taker_ratio(payload: list[dict], symbol: str, ingest_ms: int) -> list[dict]:
    return [{
        "symbol": symbol,
        "timestamp_ms": int(r["timestamp"]),
        "buy_sell_ratio": to_float(r["buySellRatio"]),
        "buy_vol": to_float(r["buyVol"]),
        "sell_vol": to_float(r["sellVol"]),
        "ingest_ms": ingest_ms,
    } for r in payload]


def parse_premium_index(payload: list[dict] | dict, ingest_ms: int) -> list[dict]:
    items = payload if isinstance(payload, list) else [payload]
    return [{
        "symbol": r["symbol"],
        "time_ms": int(r["time"]),
        "mark_price": to_float(r.get("markPrice")),
        "index_price": to_float(r.get("indexPrice")),
        "estimated_settle_price": to_float(r.get("estimatedSettlePrice")),
        "last_funding_rate": to_float(r.get("lastFundingRate")),
        "interest_rate": to_float(r.get("interestRate")),
        "next_funding_time_ms": int(r.get("nextFundingTime", 0)),
        "ingest_ms": ingest_ms,
    } for r in items]


def parse_funding_rate(payload: list[dict], ingest_ms: int) -> list[dict]:
    return [{
        "symbol": r["symbol"],
        "funding_time_ms": int(r["fundingTime"]),
        "funding_rate": to_float(r.get("fundingRate")),
        "mark_price": to_float(r.get("markPrice")),
        "rate_type": r.get("rateType"),
        "ingest_ms": ingest_ms,
    } for r in payload]
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest collector/tests/test_parsers.py -v`
Expected: 8 passed

- [ ] **Step 6: Commit**

```bash
git add collector/src/collector/binance/ collector/tests/fixtures/ collector/tests/test_parsers.py
git commit -m "Add Binance REST response parsers"
```

---

### Task 7: forceOrder (liquidation) message parser

**Files:**
- Modify: `collector/src/collector/binance/parsers.py` (append)
- Test: `collector/tests/test_parsers_force_order.py`

**Interfaces:**
- Consumes: `FORCE_ORDER` fixture, `to_float`.
- Produces: `parse_force_order(msg: dict, ingest_ms: int) -> dict | None` → one `liquidations` row, or `None` when `msg["e"] != "forceOrder"` or the `o` block is missing.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_parsers_force_order.py`:
```python
import pyarrow as pa

from collector import lake
from collector.binance import parsers as p
from collector.tests.fixtures import binance as fx


def test_force_order_row():
    row = p.parse_force_order(fx.FORCE_ORDER, ingest_ms=5)
    assert row == {
        "event_time_ms": 1568014460893, "symbol": "BTCUSDT", "side": "SELL",
        "order_type": "LIMIT", "time_in_force": "IOC", "orig_qty": 0.014, "price": 9910.0,
        "avg_price": 9910.0, "status": "FILLED", "last_filled_qty": 0.014,
        "filled_accum_qty": 0.014, "trade_time_ms": 1568014460893, "ingest_ms": 5,
    }
    pa.Table.from_pylist([row], schema=lake.DATASETS["liquidations"].schema)


def test_force_order_wrapped_in_combined_stream_envelope():
    msg = {"stream": "!forceOrder@arr", "data": fx.FORCE_ORDER}
    assert p.parse_force_order(msg, ingest_ms=1)["symbol"] == "BTCUSDT"


def test_non_force_order_returns_none():
    assert p.parse_force_order({"e": "aggTrade"}, ingest_ms=1) is None
    assert p.parse_force_order({"e": "forceOrder"}, ingest_ms=1) is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_parsers_force_order.py -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'parse_force_order'`

- [ ] **Step 3: Append to parsers.py**

```python
def parse_force_order(msg: dict, ingest_ms: int) -> dict | None:
    if "data" in msg and isinstance(msg["data"], dict):
        msg = msg["data"]
    if msg.get("e") != "forceOrder":
        return None
    o = msg.get("o")
    if not isinstance(o, dict):
        return None
    return {
        "event_time_ms": int(msg["E"]),
        "symbol": o["s"],
        "side": o.get("S"),
        "order_type": o.get("o"),
        "time_in_force": o.get("f"),
        "orig_qty": to_float(o.get("q")),
        "price": to_float(o.get("p")),
        "avg_price": to_float(o.get("ap")),
        "status": o.get("X"),
        "last_filled_qty": to_float(o.get("l")),
        "filled_accum_qty": to_float(o.get("z")),
        "trade_time_ms": int(o["T"]),
        "ingest_ms": ingest_ms,
    }
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_parsers_force_order.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/binance/parsers.py collector/tests/test_parsers_force_order.py
git commit -m "Add forceOrder liquidation message parser"
```

---

### Task 8: Rate-limited Binance REST client

**Files:**
- Create: `collector/src/collector/binance/rest.py`
- Test: `collector/tests/test_rest.py`

**Interfaces:**
- Produces: `TokenBucket(capacity: int, period_s: float, *, clock=time.monotonic, sleep=asyncio.sleep)` with `async acquire(n: int = 1) -> float` (seconds waited); `BinanceRest(base_url: str, *, client: httpx.AsyncClient | None = None, data_bucket: TokenBucket | None = None, fapi_bucket: TokenBucket | None = None, sleep=asyncio.sleep, max_retries: int = 3)` with `async get(path: str, params: dict | None = None, *, weight: int = 1) -> Any` and `async aclose()`; exceptions `BannedError`, `RateLimitedError`. Paths starting with `/futures/data/` consume 1 token from `data_bucket`; all others consume `weight` from `fapi_bucket`. Defaults: data bucket 800/300 s, fapi bucket 2000/60 s.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_rest.py`:
```python
import httpx
import pytest
import respx

from collector.binance.rest import BannedError, BinanceRest, RateLimitedError, TokenBucket


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self):
        return self.now

    async def sleep(self, s):
        self.sleeps.append(s)
        self.now += s


async def test_token_bucket_waits_when_empty():
    clk = FakeClock()
    b = TokenBucket(capacity=2, period_s=10, clock=clk, sleep=clk.sleep)
    assert await b.acquire() == 0.0
    assert await b.acquire() == 0.0
    waited = await b.acquire()
    assert waited == pytest.approx(5.0)
    assert clk.sleeps == [pytest.approx(5.0)]


async def test_token_bucket_refills_over_time():
    clk = FakeClock()
    b = TokenBucket(capacity=2, period_s=10, clock=clk, sleep=clk.sleep)
    await b.acquire(2)
    clk.now += 10
    assert await b.acquire(2) == 0.0


class Recorder:
    def __init__(self):
        self.calls: list[int] = []

    async def acquire(self, n=1):
        self.calls.append(n)
        return 0.0


@respx.mock
async def test_get_routes_buckets_and_parses_json():
    respx.get("https://api.test/futures/data/openInterestHist").mock(return_value=httpx.Response(200, json=[{"a": 1}]))
    respx.get("https://api.test/fapi/v1/premiumIndex").mock(return_value=httpx.Response(200, json={"b": 2}))
    data, fapi = Recorder(), Recorder()
    r = BinanceRest("https://api.test", data_bucket=data, fapi_bucket=fapi)
    assert await r.get("/futures/data/openInterestHist", {"symbol": "BTCUSDT"}) == [{"a": 1}]
    assert await r.get("/fapi/v1/premiumIndex", weight=10) == {"b": 2}
    assert data.calls == [1]
    assert fapi.calls == [10]
    await r.aclose()


@respx.mock
async def test_429_sleeps_retry_after_then_succeeds():
    route = respx.get("https://api.test/fapi/v1/x")
    route.side_effect = [httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200, json={"ok": True})]
    clk = FakeClock()
    r = BinanceRest("https://api.test", data_bucket=Recorder(), fapi_bucket=Recorder(), sleep=clk.sleep)
    assert await r.get("/fapi/v1/x") == {"ok": True}
    assert clk.sleeps == [7.0]


@respx.mock
async def test_418_sleeps_300_and_raises():
    respx.get("https://api.test/fapi/v1/x").mock(return_value=httpx.Response(418))
    clk = FakeClock()
    r = BinanceRest("https://api.test", data_bucket=Recorder(), fapi_bucket=Recorder(), sleep=clk.sleep)
    with pytest.raises(BannedError):
        await r.get("/fapi/v1/x")
    assert clk.sleeps == [300.0]


@respx.mock
async def test_persistent_429_raises_rate_limited():
    respx.get("https://api.test/fapi/v1/x").mock(return_value=httpx.Response(429, headers={"Retry-After": "1"}))
    clk = FakeClock()
    r = BinanceRest("https://api.test", data_bucket=Recorder(), fapi_bucket=Recorder(), sleep=clk.sleep, max_retries=2)
    with pytest.raises(RateLimitedError):
        await r.get("/fapi/v1/x")
    assert len(clk.sleeps) == 3


@respx.mock
async def test_5xx_retries_with_backoff():
    route = respx.get("https://api.test/fapi/v1/x")
    route.side_effect = [httpx.Response(503), httpx.Response(200, json=1)]
    clk = FakeClock()
    r = BinanceRest("https://api.test", data_bucket=Recorder(), fapi_bucket=Recorder(), sleep=clk.sleep)
    assert await r.get("/fapi/v1/x") == 1
    assert clk.sleeps == [1.0]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_rest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.binance.rest'`

- [ ] **Step 3: Implement rest.py**

```python
from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

import httpx

SleepFn = Callable[[float], Awaitable[None]]


class BannedError(Exception):
    """HTTP 418: IP banned by Binance."""


class RateLimitedError(Exception):
    """Still 429 after all retries."""


class TokenBucket:
    def __init__(self, capacity: int, period_s: float, *, clock=time.monotonic, sleep: SleepFn = asyncio.sleep):
        self.capacity = capacity
        self.period_s = period_s
        self._tokens = float(capacity)
        self._clock = clock
        self._sleep = sleep
        self._last = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._last = now
        self._tokens = min(float(self.capacity), self._tokens + elapsed * self.capacity / self.period_s)

    async def acquire(self, n: int = 1) -> float:
        waited = 0.0
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return waited
                deficit = n - self._tokens
                wait = deficit * self.period_s / self.capacity
                await self._sleep(wait)
                waited += wait


class BinanceRest:
    def __init__(self, base_url: str, *, client: httpx.AsyncClient | None = None,
                 data_bucket=None, fapi_bucket=None, sleep: SleepFn = asyncio.sleep, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self._own_client = client is None
        self.data_bucket = data_bucket or TokenBucket(800, 300)
        self.fapi_bucket = fapi_bucket or TokenBucket(2000, 60)
        self._sleep = sleep
        self.max_retries = max_retries

    async def get(self, path: str, params: dict | None = None, *, weight: int = 1) -> Any:
        if path.startswith("/futures/data/"):
            await self.data_bucket.acquire(1)
        else:
            await self.fapi_bucket.acquire(weight)
        url = self.base_url + path
        for attempt in range(self.max_retries + 1):
            try:
                resp = await self._client.get(url, params=params)
            except httpx.TransportError:
                if attempt == self.max_retries:
                    raise
                await self._sleep(float(2 ** attempt))
                continue
            if resp.status_code == 429:
                await self._sleep(float(resp.headers.get("Retry-After", "60")))
                continue
            if resp.status_code == 418:
                await self._sleep(300.0)
                raise BannedError(path)
            if resp.status_code >= 500:
                if attempt == self.max_retries:
                    resp.raise_for_status()
                await self._sleep(float(2 ** attempt))
                continue
            resp.raise_for_status()
            return resp.json()
        raise RateLimitedError(path)

    async def aclose(self) -> None:
        if self._own_client:
            await self._client.aclose()
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_rest.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/binance/rest.py collector/tests/test_rest.py
git commit -m "Add token-bucket rate-limited Binance REST client"
```

---

### Task 9: Reconnecting WebSocket runner

**Files:**
- Create: `collector/src/collector/binance/ws.py`
- Test: `collector/tests/test_ws.py`

**Interfaces:**
- Produces: `backoff_delays(base: float = 1.0, cap: float = 60.0) -> Iterator[float]`; `async run_stream(url: str, on_message: Callable[[dict], Awaitable[None]], *, connect=websockets.connect, sleep=asyncio.sleep, on_event: Callable[[str, str], None] | None = None, max_connections: int | None = None) -> None`. `on_event(kind, detail)` receives `"connected"`, `"error"`, `"reconnect"`, `"bad_json"`. Backoff resets only after a connection delivered at least one message.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_ws.py`:
```python
import json
from contextlib import asynccontextmanager
from itertools import islice

from collector.binance.ws import backoff_delays, run_stream


def test_backoff_sequence_caps_at_60():
    assert list(islice(backoff_delays(), 8)) == [1, 2, 4, 8, 16, 32, 60, 60]


class FakeWs:
    def __init__(self, messages):
        self._messages = list(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


def make_connect(script):
    """script: list of either Exception instances or lists of raw messages."""
    calls = []

    @asynccontextmanager
    async def connect(url, **kwargs):
        calls.append(url)
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        yield FakeWs(item)

    connect.calls = calls
    return connect


async def test_reconnects_with_backoff_and_reset():
    sleeps, events, received = [], [], []

    async def sleep(s):
        sleeps.append(s)

    async def on_message(m):
        received.append(m)

    script = [ConnectionError("down"), ConnectionError("down"),
              [json.dumps({"e": "forceOrder", "n": 1}), "not json"], []]
    connect = make_connect(script)
    await run_stream("wss://x/ws", on_message, connect=connect, sleep=sleep,
                     on_event=lambda k, d: events.append(k), max_connections=4)
    assert received == [{"e": "forceOrder", "n": 1}]
    assert sleeps == [1, 2, 1, 2]
    assert events.count("connected") == 2
    assert events.count("error") == 2
    assert "bad_json" in events
    assert len(connect.calls) == 4
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_ws.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.binance.ws'`

- [ ] **Step 3: Implement ws.py**

```python
from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable, Iterator

import websockets

EventFn = Callable[[str, str], None]


def backoff_delays(base: float = 1.0, cap: float = 60.0) -> Iterator[float]:
    d = base
    while True:
        yield d
        d = min(d * 2, cap)


async def run_stream(url: str, on_message: Callable[[dict], Awaitable[None]], *,
                     connect=websockets.connect, sleep=asyncio.sleep,
                     on_event: EventFn | None = None, max_connections: int | None = None) -> None:
    emit = on_event or (lambda kind, detail: None)
    delays = backoff_delays()
    connections = 0
    while max_connections is None or connections < max_connections:
        connections += 1
        got_message = False
        try:
            async with connect(url, ping_interval=20, ping_timeout=20, max_queue=4096) as ws:
                emit("connected", url)
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        emit("bad_json", str(raw)[:120])
                        continue
                    got_message = True
                    await on_message(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any transport failure triggers reconnect
            emit("error", repr(exc))
        if got_message:
            delays = backoff_delays()
        delay = next(delays)
        emit("reconnect", str(delay))
        await sleep(delay)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_ws.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/binance/ws.py collector/tests/test_ws.py
git commit -m "Add reconnecting WebSocket stream runner"
```

---

### Task 10: Deribit DVOL fetcher

**Files:**
- Create: `collector/src/collector/deribit.py`
- Test: `collector/tests/test_deribit.py`

**Interfaces:**
- Produces: `DVOL_PATH = "/api/v2/public/get_volatility_index_data"`; `parse_dvol(payload: dict, currency: str, ingest_ms: int) -> list[dict]` → `dvol` rows; `async fetch_dvol(client: httpx.AsyncClient, base_url: str, currency: str, start_ms: int, end_ms: int, ingest_ms: int, resolution: str = "3600") -> list[dict]` following `result.continuation` pagination.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_deribit.py`:
```python
import httpx
import pyarrow as pa
import respx

from collector import lake
from collector.deribit import DVOL_PATH, fetch_dvol, parse_dvol

PAGE1 = {"result": {"data": [[1000, 0.5, 0.6, 0.4, 0.55], [2000, 0.55, 0.7, 0.5, 0.6]], "continuation": 999}}
PAGE2 = {"result": {"data": [[500, 0.4, 0.5, 0.3, 0.5]], "continuation": None}}


def test_parse_dvol_rows():
    rows = parse_dvol(PAGE1, "BTC", ingest_ms=1)
    assert rows[0] == {"currency": "BTC", "timestamp_ms": 1000, "open": 0.5, "high": 0.6,
                       "low": 0.4, "close": 0.55, "ingest_ms": 1}
    pa.Table.from_pylist(rows, schema=lake.DATASETS["dvol"].schema)


@respx.mock
async def test_fetch_follows_continuation():
    route = respx.get("https://d.test" + DVOL_PATH)
    route.side_effect = [httpx.Response(200, json=PAGE1), httpx.Response(200, json=PAGE2)]
    async with httpx.AsyncClient() as client:
        rows = await fetch_dvol(client, "https://d.test", "ETH", 0, 3000, ingest_ms=1)
    assert [r["timestamp_ms"] for r in rows] == [1000, 2000, 500]
    assert route.calls[0].request.url.params["end_timestamp"] == "3000"
    assert route.calls[1].request.url.params["end_timestamp"] == "999"
    assert route.calls[0].request.url.params["resolution"] == "3600"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_deribit.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.deribit'`

- [ ] **Step 3: Implement deribit.py**

```python
from __future__ import annotations

import httpx

DVOL_PATH = "/api/v2/public/get_volatility_index_data"


def parse_dvol(payload: dict, currency: str, ingest_ms: int) -> list[dict]:
    data = (payload.get("result") or {}).get("data") or []
    return [{
        "currency": currency,
        "timestamp_ms": int(c[0]),
        "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4]),
        "ingest_ms": ingest_ms,
    } for c in data]


async def fetch_dvol(client: httpx.AsyncClient, base_url: str, currency: str, start_ms: int,
                     end_ms: int, ingest_ms: int, resolution: str = "3600") -> list[dict]:
    rows: list[dict] = []
    end = end_ms
    for _ in range(50):  # hard stop against runaway pagination
        resp = await client.get(base_url.rstrip("/") + DVOL_PATH, params={
            "currency": currency, "start_timestamp": start_ms, "end_timestamp": end,
            "resolution": resolution}, timeout=10.0)
        resp.raise_for_status()
        payload = resp.json()
        rows.extend(parse_dvol(payload, currency, ingest_ms))
        cont = (payload.get("result") or {}).get("continuation")
        if not cont or cont == end:
            break
        end = cont
    return rows
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_deribit.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/deribit.py collector/tests/test_deribit.py
git commit -m "Add Deribit DVOL fetcher with continuation pagination"
```

---

### Task 11: Binance Vision download, checksum and CSV parsers

**Files:**
- Create: `collector/src/collector/binance/vision.py`
- Test: `collector/tests/test_vision.py`

**Interfaces:**
- Produces: `VISION_BASE = "https://data.binance.vision/data/futures/um"`; `vision_url(kind: str, symbol: str, date: str, timeframe: str | None = None) -> str` for `kind in {"klines", "metrics", "fundingRate"}` (fundingRate is monthly, uses `date[:7]`); `ChecksumError`; `verify_checksum(zip_bytes: bytes, checksum_text: str) -> None`; `unzip_csv(zip_bytes: bytes) -> str`; `parse_kline_csv(text: str, symbol: str, ingest_ms: int) -> list[dict]` (header optional); `parse_metrics_csv(text, symbol, ingest_ms) -> list[dict]`; `parse_funding_csv(text, symbol, ingest_ms) -> list[dict]`; `async fetch_csv(client: httpx.AsyncClient, kind: str, symbol: str, date: str, timeframe: str | None = None) -> str | None` (returns `None` on HTTP 404, raises `ChecksumError` on mismatch).

- [ ] **Step 1: Write failing tests**

`collector/tests/test_vision.py`:
```python
import hashlib
import io
import zipfile

import httpx
import pyarrow as pa
import pytest
import respx

from collector import lake
from collector.binance import vision as v

KLINE_CSV = (
    "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n"
    "1757721600000,111000.1,111500.0,110800.0,111200.5,1234.567,1757725199999,137000000.5,98765,600.1,66700000.2,0\n"
)
KLINE_CSV_NO_HEADER = "1757721600000,1,2,0.5,1.5,10,1757725199999,15,3,4,6,0\n"
METRICS_CSV = (
    "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,"
    "sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
    "2026-09-13 00:05:00,BTCUSDT,80000.5,8900000000.1,1.8,1.2,2.1,0.95\n"
)
FUNDING_CSV = "calc_time,funding_interval_hours,last_funding_rate\n1757721600000,8,0.0001\n"


def make_zip(csv_text: str, name: str = "x.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, csv_text)
    return buf.getvalue()


def test_urls():
    assert v.vision_url("klines", "BTCUSDT", "2026-09-13", "1h") == \
        "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2026-09-13.zip"
    assert v.vision_url("metrics", "BTCUSDT", "2026-09-13") == \
        "https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2026-09-13.zip"
    assert v.vision_url("fundingRate", "BTCUSDT", "2026-09-13") == \
        "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-09.zip"
    with pytest.raises(ValueError):
        v.vision_url("bogus", "BTCUSDT", "2026-09-13")


def test_checksum_ok_and_mismatch():
    z = make_zip(KLINE_CSV)
    good = hashlib.sha256(z).hexdigest() + "  BTCUSDT-1h-2026-09-13.zip\n"
    v.verify_checksum(z, good)
    with pytest.raises(v.ChecksumError):
        v.verify_checksum(z, "deadbeef  x.zip")


def test_unzip_csv():
    assert v.unzip_csv(make_zip("a,b\n1,2\n")) == "a,b\n1,2\n"


def test_parse_kline_with_and_without_header():
    rows = v.parse_kline_csv(KLINE_CSV, "BTCUSDT", ingest_ms=1)
    assert rows[0]["open_time_ms"] == 1757721600000
    assert rows[0]["trade_count"] == 98765
    assert rows[0]["taker_buy_quote"] == 66700000.2
    pa.Table.from_pylist(rows, schema=lake.DATASETS["klines_1h"].schema)
    rows2 = v.parse_kline_csv(KLINE_CSV_NO_HEADER, "BTCUSDT", ingest_ms=1)
    assert rows2[0]["close"] == 1.5


def test_parse_metrics_converts_time_string():
    rows = v.parse_metrics_csv(METRICS_CSV, "BTCUSDT", ingest_ms=1)
    assert rows[0]["create_time_ms"] == 1757721900000
    assert rows[0]["sum_taker_long_short_vol_ratio"] == 0.95
    pa.Table.from_pylist(rows, schema=lake.DATASETS["vision_metrics"].schema)


def test_parse_funding():
    rows = v.parse_funding_csv(FUNDING_CSV, "BTCUSDT", ingest_ms=1)
    assert rows[0] == {"symbol": "BTCUSDT", "calc_time_ms": 1757721600000,
                       "funding_interval_hours": 8.0, "last_funding_rate": 0.0001, "ingest_ms": 1}
    pa.Table.from_pylist(rows, schema=lake.DATASETS["vision_funding"].schema)


@respx.mock
async def test_fetch_csv_verifies_and_returns_text_or_none():
    z = make_zip(FUNDING_CSV)
    url = v.vision_url("fundingRate", "BTCUSDT", "2026-09-13")
    respx.get(url).mock(return_value=httpx.Response(200, content=z))
    respx.get(url + ".CHECKSUM").mock(return_value=httpx.Response(200, text=hashlib.sha256(z).hexdigest() + "  f.zip"))
    missing = v.vision_url("metrics", "BTCUSDT", "2026-09-13")
    respx.get(missing).mock(return_value=httpx.Response(404))
    async with httpx.AsyncClient() as client:
        assert await v.fetch_csv(client, "fundingRate", "BTCUSDT", "2026-09-13") == FUNDING_CSV
        assert await v.fetch_csv(client, "metrics", "BTCUSDT", "2026-09-13") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_vision.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.binance.vision'`

- [ ] **Step 3: Implement vision.py**

```python
from __future__ import annotations

import csv
import hashlib
import io
import zipfile
from datetime import datetime, timezone

import httpx

VISION_BASE = "https://data.binance.vision/data/futures/um"


class ChecksumError(Exception):
    pass


def vision_url(kind: str, symbol: str, date: str, timeframe: str | None = None) -> str:
    if kind == "klines":
        if not timeframe:
            raise ValueError("timeframe required for klines")
        return f"{VISION_BASE}/daily/klines/{symbol}/{timeframe}/{symbol}-{timeframe}-{date}.zip"
    if kind == "metrics":
        return f"{VISION_BASE}/daily/metrics/{symbol}/{symbol}-metrics-{date}.zip"
    if kind == "fundingRate":
        return f"{VISION_BASE}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{date[:7]}.zip"
    raise ValueError(f"unknown kind {kind!r}")


def verify_checksum(zip_bytes: bytes, checksum_text: str) -> None:
    expected = checksum_text.split()[0].strip().lower()
    actual = hashlib.sha256(zip_bytes).hexdigest()
    if expected != actual:
        raise ChecksumError(f"expected {expected} got {actual}")


def unzip_csv(zip_bytes: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        return z.read(name).decode("utf-8")


def _rows(text: str) -> list[list[str]]:
    reader = csv.reader(io.StringIO(text))
    out = []
    for r in reader:
        if not r or not any(cell.strip() for cell in r):
            continue
        out.append(r)
    return out


def _is_header(row: list[str]) -> bool:
    return not row[0].strip().replace(".", "", 1).isdigit()


def parse_kline_csv(text: str, symbol: str, ingest_ms: int) -> list[dict]:
    rows = _rows(text)
    if rows and _is_header(rows[0]):
        rows = rows[1:]
    return [{
        "symbol": symbol,
        "open_time_ms": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
        "close": float(r[4]), "volume": float(r[5]), "close_time_ms": int(r[6]),
        "quote_volume": float(r[7]), "trade_count": int(float(r[8])),
        "taker_buy_base": float(r[9]), "taker_buy_quote": float(r[10]),
        "ingest_ms": ingest_ms,
    } for r in rows]


def _time_str_to_ms(s: str) -> int:
    s = s.strip()
    if s.isdigit():
        return int(s)
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_metrics_csv(text: str, symbol: str, ingest_ms: int) -> list[dict]:
    rows = _rows(text)
    if rows and rows[0][0].strip() == "create_time":
        rows = rows[1:]
    return [{
        "symbol": symbol,
        "create_time_ms": _time_str_to_ms(r[0]),
        "sum_open_interest": float(r[2]), "sum_open_interest_value": float(r[3]),
        "count_toptrader_long_short_ratio": float(r[4]), "sum_toptrader_long_short_ratio": float(r[5]),
        "count_long_short_ratio": float(r[6]), "sum_taker_long_short_vol_ratio": float(r[7]),
        "ingest_ms": ingest_ms,
    } for r in rows]


def parse_funding_csv(text: str, symbol: str, ingest_ms: int) -> list[dict]:
    rows = _rows(text)
    if rows and rows[0][0].strip() == "calc_time":
        rows = rows[1:]
    return [{
        "symbol": symbol,
        "calc_time_ms": int(r[0]),
        "funding_interval_hours": float(r[1]),
        "last_funding_rate": float(r[2]),
        "ingest_ms": ingest_ms,
    } for r in rows]


async def fetch_csv(client: httpx.AsyncClient, kind: str, symbol: str, date: str,
                    timeframe: str | None = None) -> str | None:
    url = vision_url(kind, symbol, date, timeframe)
    resp = await client.get(url, timeout=60.0)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    sums = await client.get(url + ".CHECKSUM", timeout=30.0)
    if sums.status_code == 200:
        verify_checksum(resp.content, sums.text)
    return unzip_csv(resp.content)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_vision.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/binance/vision.py collector/tests/test_vision.py
git commit -m "Add Binance Vision downloader with checksum and CSV parsers"
```

---

### Task 12: Periodic scheduler and supervisor

**Files:**
- Create: `collector/src/collector/scheduler.py`
- Test: `collector/tests/test_scheduler.py`

**Interfaces:**
- Consumes: `StateStore` (Task 5), `backoff_delays` (Task 9).
- Produces: `seconds_until_next(interval_s: float, offset_s: float, now_s: float) -> float`; `async run_periodic(name: str, interval_s: float, fn: Callable[[], Awaitable[dict | None]], state: StateStore, *, offset_s: float = 0.0, align: bool = False, run_immediately: bool = True, sleep=asyncio.sleep, wall=time.time, max_runs: int | None = None) -> None`; `async supervise(name: str, factory: Callable[[], Awaitable[None]], state: StateStore, *, sleep=asyncio.sleep, max_restarts: int | None = None) -> None`. `fn` may return a dict stored as `detail`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_scheduler.py`:
```python
import pytest

from collector.scheduler import run_periodic, seconds_until_next, supervise
from collector.state import StateStore


def test_seconds_until_next_alignment():
    assert seconds_until_next(3600, 120, 36000 + 60) == pytest.approx(60)
    assert seconds_until_next(3600, 120, 36000 + 200) == pytest.approx(3520)
    assert seconds_until_next(60, 0, 125) == pytest.approx(55)


async def test_run_periodic_records_error_then_success(tmp_path):
    state = StateStore(tmp_path / "s.json")
    sleeps, calls = [], []

    async def sleep(s):
        sleeps.append(s)

    async def fn():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("first fails")
        return {"rows": 3}

    await run_periodic("t", 30, fn, state, sleep=sleep, max_runs=2)
    assert len(calls) == 2
    rec = state.get("t")
    assert rec["last_error"] == "RuntimeError('first fails')"
    assert rec["consecutive_errors"] == 0
    assert rec["detail"] == {"rows": 3}
    assert sleeps == [30]


async def test_run_periodic_aligned_uses_wall_clock(tmp_path):
    state = StateStore(tmp_path / "s.json")
    sleeps = []

    async def sleep(s):
        sleeps.append(s)

    async def fn():
        return None

    await run_periodic("t", 3600, fn, state, offset_s=120, align=True, run_immediately=False,
                       sleep=sleep, wall=lambda: 36000 + 60, max_runs=1)
    assert sleeps == [pytest.approx(60)]


async def test_supervise_restarts_with_backoff(tmp_path):
    state = StateStore(tmp_path / "s.json")
    sleeps, starts = [], []

    async def sleep(s):
        sleeps.append(s)

    async def factory():
        starts.append(1)
        if len(starts) == 1:
            raise ConnectionError("drop")
        return None  # clean exit also counts as a restart trigger

    await supervise("liq", factory, state, sleep=sleep, max_restarts=2)
    assert len(starts) == 2
    assert sleeps == [1, 2]
    assert state.get("liq")["consecutive_errors"] == 2
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_scheduler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.scheduler'`

- [ ] **Step 3: Implement scheduler.py**

```python
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from collector.binance.ws import backoff_delays
from collector.state import StateStore

log = logging.getLogger(__name__)


def seconds_until_next(interval_s: float, offset_s: float, now_s: float) -> float:
    base = (now_s // interval_s) * interval_s
    candidate = base + offset_s
    if candidate <= now_s:
        candidate += interval_s
    return candidate - now_s


async def run_periodic(name: str, interval_s: float, fn: Callable[[], Awaitable[dict | None]],
                       state: StateStore, *, offset_s: float = 0.0, align: bool = False,
                       run_immediately: bool = True, sleep=asyncio.sleep, wall=time.time,
                       max_runs: int | None = None) -> None:
    def next_delay() -> float:
        return seconds_until_next(interval_s, offset_s, wall()) if align else float(interval_s)

    if not run_immediately:
        await sleep(next_delay())
    runs = 0
    while max_runs is None or runs < max_runs:
        runs += 1
        try:
            detail = await fn()
            state.mark_success(name, detail=detail if isinstance(detail, dict) else None)
            log.info("task=%s ok detail=%s", name, detail)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            state.mark_error(name, repr(exc))
            log.exception("task=%s failed", name)
        if max_runs is not None and runs >= max_runs:
            return
        await sleep(next_delay())


async def supervise(name: str, factory: Callable[[], Awaitable[None]], state: StateStore, *,
                    sleep=asyncio.sleep, max_restarts: int | None = None) -> None:
    delays = backoff_delays()
    restarts = 0
    while max_restarts is None or restarts < max_restarts:
        restarts += 1
        try:
            await factory()
            state.mark_error(name, "exited unexpectedly")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            state.mark_error(name, repr(exc))
            log.exception("task=%s crashed", name)
        await sleep(next(delays))
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_scheduler.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/scheduler.py collector/tests/test_scheduler.py
git commit -m "Add periodic scheduler and crash supervisor"
```

---

### Task 13: REST collection tasks (symbols, metrics, premium, funding)

**Files:**
- Create: `collector/src/collector/tasks/__init__.py` (empty)
- Create: `collector/src/collector/tasks/context.py`
- Create: `collector/src/collector/tasks/symbols.py`
- Create: `collector/src/collector/tasks/metrics.py`
- Create: `collector/src/collector/tasks/premium.py`
- Create: `collector/src/collector/tasks/funding.py`
- Test: `collector/tests/test_tasks_rest.py`

**Interfaces:**
- Consumes: `Settings`, `BinanceRest`, `StateStore`, `lake.write_rows / partitions / view / date_from_ms / now_ms`, parsers from Task 6.
- Produces:
  - `Context(settings, rest, http: httpx.AsyncClient, state: StateStore, data_dir: Path, now_ms: Callable[[], int] = lake.now_ms)` dataclass.
  - `symbols.run_once(ctx) -> dict`, `symbols.load_symbols(data_dir) -> list[str]`, `symbols.ensure_symbols(ctx) -> list[str]`.
  - `metrics.ENDPOINTS: list[tuple[dataset, path, parser]]`, `metrics.run_once(ctx, *, concurrency: int = 8) -> dict` returning `{"symbols": n, "errors": e, "rows": {dataset: count}}`.
  - `premium.PremiumTask(ctx, flush_every: int = 15)` with `async poll() -> dict | None` (writes every `flush_every` polls) and `flush() -> int`.
  - `funding.run_once(ctx, *, concurrency: int = 8) -> dict`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_tasks_rest.py`:
```python
from pathlib import Path

import httpx

from collector import lake
from collector.config import Settings
from collector.state import StateStore
from collector.tasks import funding, metrics, premium, symbols
from collector.tasks.context import Context
from collector.tests.fixtures import binance as fx


class FakeRest:
    def __init__(self, routes: dict, fail_paths: set[str] | None = None):
        self.routes = routes
        self.fail_paths = fail_paths or set()
        self.calls: list[tuple[str, dict | None]] = []

    async def get(self, path, params=None, *, weight=1):
        self.calls.append((path, params))
        if path in self.fail_paths:
            raise RuntimeError("boom")
        return self.routes[path]


def make_ctx(tmp_path: Path, rest: FakeRest, now: int = 1_757_721_600_000) -> Context:
    return Context(settings=Settings(_env_file=None), rest=rest, http=httpx.AsyncClient(),
                   state=StateStore(tmp_path / "state.json"), data_dir=tmp_path, now_ms=lambda: now)


async def test_symbols_snapshot_and_load(tmp_path):
    ctx = make_ctx(tmp_path, FakeRest({"/fapi/v1/exchangeInfo": fx.EXCHANGE_INFO}))
    assert symbols.load_symbols(tmp_path) == []
    detail = await symbols.run_once(ctx)
    assert detail == {"symbols": 1}
    assert symbols.load_symbols(tmp_path) == ["BTCUSDT"]
    assert lake.partitions(tmp_path, "symbols") == ["2026-09-13"]


async def test_ensure_symbols_fetches_when_missing(tmp_path):
    rest = FakeRest({"/fapi/v1/exchangeInfo": fx.EXCHANGE_INFO})
    ctx = make_ctx(tmp_path, rest)
    assert await symbols.ensure_symbols(ctx) == ["BTCUSDT"]
    assert await symbols.ensure_symbols(ctx) == ["BTCUSDT"]
    assert len(rest.calls) == 1


async def test_metrics_writes_five_datasets_once_each(tmp_path):
    routes = {
        "/fapi/v1/exchangeInfo": fx.EXCHANGE_INFO,
        "/futures/data/openInterestHist": fx.OI_HIST,
        "/futures/data/globalLongShortAccountRatio": fx.LS_RATIO,
        "/futures/data/topLongShortPositionRatio": fx.LS_RATIO,
        "/futures/data/topLongShortAccountRatio": fx.LS_RATIO,
        "/futures/data/takerlongshortRatio": fx.TAKER_RATIO,
    }
    rest = FakeRest(routes)
    ctx = make_ctx(tmp_path, rest)
    detail = await metrics.run_once(ctx)
    assert detail["symbols"] == 1 and detail["errors"] == 0
    assert detail["rows"] == {"oi_hist": 2, "ls_global_account": 1, "ls_top_position": 1,
                              "ls_top_account": 1, "taker_ratio": 1}
    params = [p for path, p in rest.calls if path.startswith("/futures/data/")]
    assert all(p == {"symbol": "BTCUSDT", "period": "5m", "limit": 500} for p in params)
    for ds in detail["rows"]:
        assert lake.file_count(tmp_path, ds) >= 1
    row = lake.view(tmp_path, "taker_ratio").fetchall()[0]
    assert "BTCUSDT" in row


async def test_metrics_counts_errors_but_writes_others(tmp_path):
    routes = {
        "/fapi/v1/exchangeInfo": fx.EXCHANGE_INFO,
        "/futures/data/openInterestHist": fx.OI_HIST,
        "/futures/data/globalLongShortAccountRatio": fx.LS_RATIO,
        "/futures/data/topLongShortPositionRatio": fx.LS_RATIO,
        "/futures/data/topLongShortAccountRatio": fx.LS_RATIO,
        "/futures/data/takerlongshortRatio": fx.TAKER_RATIO,
    }
    ctx = make_ctx(tmp_path, FakeRest(routes, fail_paths={"/futures/data/takerlongshortRatio"}))
    detail = await metrics.run_once(ctx)
    assert detail["errors"] == 1
    assert detail["rows"]["taker_ratio"] == 0
    assert detail["rows"]["oi_hist"] == 2


async def test_premium_buffers_and_flushes(tmp_path):
    ctx = make_ctx(tmp_path, FakeRest({"/fapi/v1/premiumIndex": fx.PREMIUM_INDEX}))
    task = premium.PremiumTask(ctx, flush_every=2)
    assert await task.poll() is None
    assert lake.file_count(tmp_path, "premium_index") == 0
    assert await task.poll() == {"rows": 4}
    assert lake.file_count(tmp_path, "premium_index") == 1
    assert task.flush() == 0


async def test_funding_history_all_symbols(tmp_path):
    rest = FakeRest({"/fapi/v1/exchangeInfo": fx.EXCHANGE_INFO, "/fapi/v1/fundingRate": fx.FUNDING_RATE})
    ctx = make_ctx(tmp_path, rest)
    detail = await funding.run_once(ctx)
    assert detail == {"symbols": 1, "errors": 0, "rows": 2}
    assert ("/fapi/v1/fundingRate", {"symbol": "BTCUSDT", "limit": 1000}) in rest.calls
    assert lake.view(tmp_path, "funding_rate").count("*").fetchone()[0] == 2
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_tasks_rest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.tasks'`

- [ ] **Step 3: Implement context.py**

```python
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import httpx

from collector import lake
from collector.config import Settings
from collector.state import StateStore


@dataclass
class Context:
    settings: Settings
    rest: object  # BinanceRest or a test double exposing async get(path, params, *, weight)
    http: httpx.AsyncClient
    state: StateStore
    data_dir: Path
    now_ms: Callable[[], int] = field(default=lake.now_ms)
```

- [ ] **Step 4: Implement symbols.py**

```python
from __future__ import annotations

from pathlib import Path

from collector import lake
from collector.binance.parsers import parse_exchange_info
from collector.tasks.context import Context


async def run_once(ctx: Context) -> dict:
    payload = await ctx.rest.get("/fapi/v1/exchangeInfo", weight=1)
    ingest = ctx.now_ms()
    rows = parse_exchange_info(payload, lake.date_from_ms(ingest), ingest)
    lake.write_rows(ctx.data_dir, "symbols", rows, ingest_ms=ingest)
    return {"symbols": len(rows)}


def load_symbols(data_dir: Path) -> list[str]:
    parts = lake.partitions(data_dir, "symbols")
    if not parts:
        return []
    latest = parts[-1]
    rel = lake.view(data_dir, "symbols").filter(f"snapshot_date = '{latest}'").project("symbol")
    return sorted(r[0] for r in rel.fetchall())


async def ensure_symbols(ctx: Context) -> list[str]:
    syms = load_symbols(ctx.data_dir)
    if not syms:
        await run_once(ctx)
        syms = load_symbols(ctx.data_dir)
    return syms
```

- [ ] **Step 5: Implement metrics.py**

```python
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict

from collector import lake
from collector.binance.parsers import parse_ls_ratio, parse_oi_hist, parse_taker_ratio
from collector.tasks.context import Context
from collector.tasks.symbols import ensure_symbols

log = logging.getLogger(__name__)

ENDPOINTS = [
    ("oi_hist", "/futures/data/openInterestHist", parse_oi_hist),
    ("ls_global_account", "/futures/data/globalLongShortAccountRatio", parse_ls_ratio),
    ("ls_top_position", "/futures/data/topLongShortPositionRatio", parse_ls_ratio),
    ("ls_top_account", "/futures/data/topLongShortAccountRatio", parse_ls_ratio),
    ("taker_ratio", "/futures/data/takerlongshortRatio", parse_taker_ratio),
]


async def run_once(ctx: Context, *, concurrency: int = 8) -> dict:
    symbols = await ensure_symbols(ctx)
    ingest = ctx.now_ms()
    buckets: dict[str, list[dict]] = defaultdict(list)
    errors = 0
    sem = asyncio.Semaphore(concurrency)

    async def fetch(symbol: str, dataset: str, path: str, parser):
        nonlocal errors
        async with sem:
            try:
                payload = await ctx.rest.get(path, {"symbol": symbol, "period": "5m", "limit": 500})
            except Exception as exc:  # noqa: BLE001 - one failure must not sink the batch
                errors += 1
                log.warning("metrics %s %s failed: %r", dataset, symbol, exc)
                return
        buckets[dataset].extend(parser(payload, symbol, ingest))

    await asyncio.gather(*(fetch(s, ds, path, parser) for s in symbols for ds, path, parser in ENDPOINTS))
    rows = {}
    for dataset, _, _ in ENDPOINTS:
        lake.write_rows(ctx.data_dir, dataset, buckets[dataset], ingest_ms=ingest)
        rows[dataset] = len(buckets[dataset])
    return {"symbols": len(symbols), "errors": errors, "rows": rows}
```

- [ ] **Step 6: Implement premium.py**

```python
from __future__ import annotations

from collector import lake
from collector.binance.parsers import parse_premium_index
from collector.tasks.context import Context


class PremiumTask:
    def __init__(self, ctx: Context, flush_every: int = 15):
        self.ctx = ctx
        self.flush_every = flush_every
        self._buffer: list[dict] = []
        self._polls = 0

    async def poll(self) -> dict | None:
        payload = await self.ctx.rest.get("/fapi/v1/premiumIndex", weight=10)
        self._buffer.extend(parse_premium_index(payload, self.ctx.now_ms()))
        self._polls += 1
        if self._polls >= self.flush_every:
            return {"rows": self.flush()}
        return None

    def flush(self) -> int:
        n = len(self._buffer)
        if n:
            lake.write_rows(self.ctx.data_dir, "premium_index", self._buffer, ingest_ms=self.ctx.now_ms())
        self._buffer.clear()
        self._polls = 0
        return n
```

- [ ] **Step 7: Implement funding.py**

```python
from __future__ import annotations

import asyncio
import logging

from collector import lake
from collector.binance.parsers import parse_funding_rate
from collector.tasks.context import Context
from collector.tasks.symbols import ensure_symbols

log = logging.getLogger(__name__)


async def run_once(ctx: Context, *, concurrency: int = 8) -> dict:
    symbols = await ensure_symbols(ctx)
    ingest = ctx.now_ms()
    rows: list[dict] = []
    errors = 0
    sem = asyncio.Semaphore(concurrency)

    async def fetch(symbol: str):
        nonlocal errors
        async with sem:
            try:
                payload = await ctx.rest.get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1000})
            except Exception as exc:  # noqa: BLE001
                errors += 1
                log.warning("funding %s failed: %r", symbol, exc)
                return
        rows.extend(parse_funding_rate(payload, ingest))

    await asyncio.gather(*(fetch(s) for s in symbols))
    lake.write_rows(ctx.data_dir, "funding_rate", rows, ingest_ms=ingest)
    return {"symbols": len(symbols), "errors": errors, "rows": len(rows)}
```

- [ ] **Step 8: Run tests**

Run: `uv run pytest collector/tests/test_tasks_rest.py -v`
Expected: 6 passed

- [ ] **Step 9: Commit**

```bash
git add collector/src/collector/tasks/ collector/tests/test_tasks_rest.py
git commit -m "Add symbols, metrics, premium and funding collection tasks"
```

---

### Task 14: Liquidation stream sink and DVOL task

**Files:**
- Create: `collector/src/collector/tasks/liquidations.py`
- Create: `collector/src/collector/tasks/dvol.py`
- Test: `collector/tests/test_tasks_stream.py`

**Interfaces:**
- Consumes: `parse_force_order`, `ws.run_stream`, `fetch_dvol`, `Context`, `StateStore`.
- Produces:
  - `liquidations.LiquidationSink(ctx, *, flush_rows: int = 500, flush_interval_s: float = 60, clock=time.monotonic)` with `async on_message(msg: dict) -> None`, `flush() -> int`, `on_event(kind: str, detail: str) -> None`.
  - `liquidations.stream_url(ws_base: str) -> str` → `<ws_base>/ws/!forceOrder@arr`.
  - `async liquidations.run(ctx, *, run_stream=ws.run_stream) -> None` (loops forever via `run_stream`).
  - `dvol.run_once(ctx, *, lookback_h: int = 48) -> dict`.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_tasks_stream.py`:
```python
import httpx
import respx

from collector import lake
from collector.config import Settings
from collector.deribit import DVOL_PATH
from collector.state import StateStore
from collector.tasks import dvol, liquidations
from collector.tasks.context import Context
from collector.tests.fixtures import binance as fx


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def make_ctx(tmp_path, http=None):
    return Context(settings=Settings(_env_file=None), rest=None, http=http or httpx.AsyncClient(),
                   state=StateStore(tmp_path / "s.json"), data_dir=tmp_path, now_ms=lambda: 1_757_721_600_000)


def test_stream_url():
    assert liquidations.stream_url("wss://fstream.binance.com/market") == "wss://fstream.binance.com/market/ws/!forceOrder@arr"


async def test_sink_flushes_on_row_count(tmp_path):
    ctx = make_ctx(tmp_path)
    sink = liquidations.LiquidationSink(ctx, flush_rows=2, flush_interval_s=999, clock=Clock())
    await sink.on_message({"e": "aggTrade"})
    await sink.on_message(fx.FORCE_ORDER)
    assert lake.file_count(tmp_path, "liquidations") == 0
    await sink.on_message(fx.FORCE_ORDER)
    assert lake.file_count(tmp_path, "liquidations") == 1
    assert ctx.state.get("liquidations")["detail"] == {"rows": 2}


async def test_sink_flushes_on_interval(tmp_path):
    ctx = make_ctx(tmp_path)
    clk = Clock()
    sink = liquidations.LiquidationSink(ctx, flush_rows=999, flush_interval_s=60, clock=clk)
    await sink.on_message(fx.FORCE_ORDER)
    clk.t = 61
    await sink.on_message(fx.FORCE_ORDER)
    assert lake.file_count(tmp_path, "liquidations") == 1


def test_connected_event_marks_success(tmp_path):
    ctx = make_ctx(tmp_path)
    sink = liquidations.LiquidationSink(ctx)
    sink.on_event("connected", "wss://x")
    assert ctx.state.get("liquidations")["last_success_ms"] is not None
    sink.on_event("error", "ConnectionError()")
    assert ctx.state.get("liquidations")["last_error"] == "ConnectionError()"


async def test_run_wires_url_and_sink(tmp_path):
    ctx = make_ctx(tmp_path)
    seen = {}

    async def fake_run_stream(url, on_message, *, on_event=None, **kw):
        seen["url"] = url
        await on_message(fx.FORCE_ORDER)
        await on_message(fx.FORCE_ORDER)

    await liquidations.run(ctx, run_stream=fake_run_stream, flush_rows=2)
    assert seen["url"].endswith("/ws/!forceOrder@arr")
    assert lake.file_count(tmp_path, "liquidations") == 1


@respx.mock
async def test_dvol_task_writes_both_currencies(tmp_path):
    page = {"result": {"data": [[1757721600000, 0.5, 0.6, 0.4, 0.55]], "continuation": None}}
    respx.get("https://www.deribit.com" + DVOL_PATH).mock(return_value=httpx.Response(200, json=page))
    async with httpx.AsyncClient() as http:
        ctx = make_ctx(tmp_path, http)
        detail = await dvol.run_once(ctx)
    assert detail == {"rows": 2}
    rel = lake.view(tmp_path, "dvol")
    assert sorted(r[0] for r in rel.project("currency").fetchall()) == ["BTC", "ETH"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_tasks_stream.py -v`
Expected: FAIL with `ImportError: cannot import name 'dvol' from 'collector.tasks'`

- [ ] **Step 3: Implement liquidations.py**

```python
from __future__ import annotations

import logging
import time

from collector import lake
from collector.binance import ws
from collector.binance.parsers import parse_force_order
from collector.tasks.context import Context

log = logging.getLogger(__name__)
TASK = "liquidations"


def stream_url(ws_base: str) -> str:
    return ws_base.rstrip("/") + "/ws/!forceOrder@arr"


class LiquidationSink:
    def __init__(self, ctx: Context, *, flush_rows: int = 500, flush_interval_s: float = 60,
                 clock=time.monotonic):
        self.ctx = ctx
        self.flush_rows = flush_rows
        self.flush_interval_s = flush_interval_s
        self._clock = clock
        self._buffer: list[dict] = []
        self._last_flush = clock()

    async def on_message(self, msg: dict) -> None:
        row = parse_force_order(msg, self.ctx.now_ms())
        if row is None:
            return
        self._buffer.append(row)
        if len(self._buffer) >= self.flush_rows or (self._clock() - self._last_flush) >= self.flush_interval_s:
            self.flush()

    def flush(self) -> int:
        n = len(self._buffer)
        if n:
            lake.write_rows(self.ctx.data_dir, "liquidations", self._buffer, ingest_ms=self.ctx.now_ms())
            self.ctx.state.mark_success(TASK, detail={"rows": n})
        self._buffer.clear()
        self._last_flush = self._clock()
        return n

    def on_event(self, kind: str, detail: str) -> None:
        if kind == "connected":
            self.ctx.state.mark_success(TASK, detail={"event": "connected"})
            log.info("liquidations connected %s", detail)
        elif kind == "error":
            self.ctx.state.mark_error(TASK, detail)
            log.warning("liquidations error %s", detail)
        else:
            log.info("liquidations %s %s", kind, detail)


async def run(ctx: Context, *, run_stream=ws.run_stream, flush_rows: int = 500,
              flush_interval_s: float = 60) -> None:
    sink = LiquidationSink(ctx, flush_rows=flush_rows, flush_interval_s=flush_interval_s)
    try:
        await run_stream(stream_url(ctx.settings.binance_ws_base), sink.on_message, on_event=sink.on_event)
    finally:
        sink.flush()
```

- [ ] **Step 4: Implement dvol.py**

```python
from __future__ import annotations

from collector import lake
from collector.deribit import fetch_dvol
from collector.tasks.context import Context

CURRENCIES = ("BTC", "ETH")


async def run_once(ctx: Context, *, lookback_h: int = 48) -> dict:
    ingest = ctx.now_ms()
    start = ingest - lookback_h * 3600 * 1000
    rows: list[dict] = []
    for cur in CURRENCIES:
        rows.extend(await fetch_dvol(ctx.http, ctx.settings.deribit_base, cur, start, ingest, ingest))
    lake.write_rows(ctx.data_dir, "dvol", rows, ingest_ms=ingest)
    return {"rows": len(rows)}
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest collector/tests/test_tasks_stream.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add collector/src/collector/tasks/liquidations.py collector/src/collector/tasks/dvol.py collector/tests/test_tasks_stream.py
git commit -m "Add liquidation stream sink and DVOL task"
```

---

### Task 15: Binance Vision backfill orchestration

**Files:**
- Create: `collector/src/collector/backfill.py`
- Test: `collector/tests/test_backfill.py`

**Interfaces:**
- Consumes: `vision.fetch_csv / parse_*_csv / ChecksumError`, `lake.write_rows`, `symbols.ensure_symbols`, `Context`.
- Produces: `DATASET_KIND: dict[str, tuple[str, str | None]]` mapping `klines_1h → ("klines","1h")`, `klines_5m → ("klines","5m")`, `vision_metrics → ("metrics", None)`, `vision_funding → ("fundingRate", None)`; `date_range(start: str, end: str) -> list[str]` (inclusive, `YYYY-MM-DD`); `month_range(start: str, end: str) -> list[str]` (first-of-month dates covering the range); `Manifest(path: Path)` with `has(key: str) -> bool`, `add(key: str) -> None`, `save() -> None`; `async backfill(ctx, *, start: str, end: str, datasets: list[str], symbols: list[str] | None = None, concurrency: int = 8, fetch_csv=vision.fetch_csv) -> dict` returning `{"jobs", "downloaded", "skipped", "missing", "failed", "rows"}`. Manifest key format `"<dataset>|<symbol>|<date>"`; 404s are not recorded so they are retried next run.

- [ ] **Step 1: Write failing tests**

`collector/tests/test_backfill.py`:
```python
import httpx

from collector import backfill as bf
from collector import lake
from collector.binance.vision import ChecksumError
from collector.config import Settings
from collector.state import StateStore
from collector.tasks.context import Context

KLINE = "1757721600000,1,2,0.5,1.5,10,1757725199999,15,3,4,6,0\n"
FUNDING = "calc_time,funding_interval_hours,last_funding_rate\n1757721600000,8,0.0001\n"


def test_ranges():
    assert bf.date_range("2026-09-11", "2026-09-13") == ["2026-09-11", "2026-09-12", "2026-09-13"]
    assert bf.month_range("2026-07-20", "2026-09-02") == ["2026-07-01", "2026-08-01", "2026-09-01"]


def test_manifest_roundtrip(tmp_path):
    m = bf.Manifest(tmp_path / "m.json")
    assert not m.has("a")
    m.add("a")
    m.save()
    assert bf.Manifest(tmp_path / "m.json").has("a")


def make_ctx(tmp_path):
    return Context(settings=Settings(_env_file=None), rest=None, http=httpx.AsyncClient(),
                   state=StateStore(tmp_path / "s.json"), data_dir=tmp_path, now_ms=lambda: 1_757_800_000_000)


async def test_backfill_downloads_skips_and_counts(tmp_path):
    calls = []

    async def fake_fetch(client, kind, symbol, date, timeframe=None):
        calls.append((kind, symbol, date, timeframe))
        if kind == "klines":
            return KLINE
        if kind == "fundingRate":
            return FUNDING
        if kind == "metrics" and date == "2026-09-12":
            raise ChecksumError("bad")
        return None

    ctx = make_ctx(tmp_path)
    stats = await bf.backfill(ctx, start="2026-09-12", end="2026-09-13",
                              datasets=["klines_1h", "vision_metrics", "vision_funding"],
                              symbols=["BTCUSDT"], fetch_csv=fake_fetch)
    assert stats == {"jobs": 5, "downloaded": 3, "skipped": 0, "missing": 1, "failed": 1, "rows": 3}
    assert ("fundingRate", "BTCUSDT", "2026-09-01", None) in calls
    assert lake.view(tmp_path, "klines_1h").count("*").fetchone()[0] == 1
    assert lake.view(tmp_path, "vision_funding").count("*").fetchone()[0] == 1

    calls.clear()
    stats2 = await bf.backfill(ctx, start="2026-09-12", end="2026-09-13",
                               datasets=["klines_1h", "vision_metrics", "vision_funding"],
                               symbols=["BTCUSDT"], fetch_csv=fake_fetch)
    assert stats2["skipped"] == 3
    assert stats2["downloaded"] == 0
    assert all(k == "metrics" for k, *_ in calls)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_backfill.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.backfill'`

- [ ] **Step 3: Implement backfill.py**

```python
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import httpx

from collector import lake
from collector.binance import vision
from collector.binance.vision import ChecksumError
from collector.tasks.context import Context
from collector.tasks.symbols import ensure_symbols

log = logging.getLogger(__name__)

DATASET_KIND: dict[str, tuple[str, str | None]] = {
    "klines_1h": ("klines", "1h"),
    "klines_5m": ("klines", "5m"),
    "vision_metrics": ("metrics", None),
    "vision_funding": ("fundingRate", None),
}
PARSERS = {
    "klines_1h": vision.parse_kline_csv,
    "klines_5m": vision.parse_kline_csv,
    "vision_metrics": vision.parse_metrics_csv,
    "vision_funding": vision.parse_funding_csv,
}


def date_range(start: str, end: str) -> list[str]:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def month_range(start: str, end: str) -> list[str]:
    d0, d1 = date.fromisoformat(start).replace(day=1), date.fromisoformat(end).replace(day=1)
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.isoformat())
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return out


class Manifest:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._done: set[str] = set()
        if self.path.exists():
            self._done = set(json.loads(self.path.read_text(encoding="utf-8")))

    def has(self, key: str) -> bool:
        return key in self._done

    def add(self, key: str) -> None:
        self._done.add(key)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(sorted(self._done)), encoding="utf-8")


async def backfill(ctx: Context, *, start: str, end: str, datasets: list[str],
                   symbols: list[str] | None = None, concurrency: int = 8,
                   fetch_csv=vision.fetch_csv) -> dict:
    syms = symbols or await ensure_symbols(ctx)
    ingest = ctx.now_ms()
    manifest = Manifest(Path(ctx.data_dir) / "backfill_manifest.json")
    stats = {"jobs": 0, "downloaded": 0, "skipped": 0, "missing": 0, "failed": 0, "rows": 0}
    sem = asyncio.Semaphore(concurrency)
    jobs: list[tuple[str, str, str | None, str, str]] = []
    for ds in datasets:
        kind, tf = DATASET_KIND[ds]
        dates = month_range(start, end) if kind == "fundingRate" else date_range(start, end)
        jobs.extend((ds, kind, tf, sym, d) for sym in syms for d in dates)
    stats["jobs"] = len(jobs)

    async def one(ds: str, kind: str, tf: str | None, sym: str, d: str) -> None:
        key = f"{ds}|{sym}|{d}"
        if manifest.has(key):
            stats["skipped"] += 1
            return
        async with sem:
            try:
                text = await fetch_csv(ctx.http, kind, sym, d, tf)
            except (ChecksumError, httpx.HTTPError) as exc:
                stats["failed"] += 1
                log.warning("backfill %s failed: %r", key, exc)
                return
        if text is None:
            stats["missing"] += 1
            return
        rows = PARSERS[ds](text, sym, ingest)
        lake.write_rows(ctx.data_dir, ds, rows, ingest_ms=ingest)
        stats["downloaded"] += 1
        stats["rows"] += len(rows)
        manifest.add(key)

    await asyncio.gather(*(one(*j) for j in jobs))
    manifest.save()
    return stats
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_backfill.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add collector/src/collector/backfill.py collector/tests/test_backfill.py
git commit -m "Add Binance Vision backfill with manifest-based skip"
```

---

### Task 16: CLI (run / backfill / compact / status)

**Files:**
- Create: `collector/src/collector/cli.py`
- Test: `collector/tests/test_cli.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `app: typer.Typer`; `ALL_TASKS = ("symbols", "metrics", "premium", "funding", "liquidations", "dvol")`; `health_intervals(settings) -> dict[str, float]`; `build_context(settings) -> Context`; `select_tasks(only: str) -> list[str]`; `async run_tasks(ctx, names: list[str], *, premium_flush_every: int = 15) -> None`; commands `run --only`, `backfill --from --to --datasets --symbols`, `compact --date --dataset`, `status` (exit code 1 when any selected task is unhealthy).

- [ ] **Step 1: Write failing tests**

`collector/tests/test_cli.py`:
```python
from datetime import datetime, timezone

import pytest
from typer.testing import CliRunner

from collector import cli, lake
from collector.state import StateStore

runner = CliRunner()


def ms(y, m, d):
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)


def test_select_tasks():
    assert cli.select_tasks("") == list(cli.ALL_TASKS)
    assert cli.select_tasks("metrics, dvol") == ["metrics", "dvol"]
    with pytest.raises(ValueError):
        cli.select_tasks("nope")


def test_status_exit_1_when_unhealthy(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_DATA_DIR", str(tmp_path))
    cli.get_settings.cache_clear()
    result = runner.invoke(cli.app, ["status"])
    assert result.exit_code == 1
    assert "metrics" in result.stdout
    assert "UNHEALTHY" in result.stdout


def test_status_healthy_and_dataset_summary(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_DATA_DIR", str(tmp_path))
    cli.get_settings.cache_clear()
    state = StateStore(tmp_path / "state.json")
    for t in cli.ALL_TASKS:
        state.mark_success(t)
    lake.write_rows(tmp_path, "dvol", [{"currency": "BTC", "timestamp_ms": ms(2026, 9, 13),
                                        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "ingest_ms": 1}], ingest_ms=1)
    result = runner.invoke(cli.app, ["status"])
    assert result.exit_code == 0
    assert "dvol" in result.stdout and "2026-09-13" in result.stdout


def test_compact_command(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_DATA_DIR", str(tmp_path))
    cli.get_settings.cache_clear()
    row = {"currency": "BTC", "timestamp_ms": ms(2026, 9, 13), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "ingest_ms": 1}
    lake.write_rows(tmp_path, "dvol", [row], ingest_ms=1)
    lake.write_rows(tmp_path, "dvol", [row], ingest_ms=2)
    result = runner.invoke(cli.app, ["compact", "--date", "2026-09-13", "--dataset", "dvol"])
    assert result.exit_code == 0, result.stdout
    assert lake.file_count(tmp_path, "dvol") == 1


def test_backfill_command_delegates(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_DATA_DIR", str(tmp_path))
    cli.get_settings.cache_clear()
    captured = {}

    async def fake_backfill(ctx, **kw):
        captured.update(kw)
        return {"jobs": 0, "downloaded": 0, "skipped": 0, "missing": 0, "failed": 0, "rows": 0}

    monkeypatch.setattr(cli.backfill_mod, "backfill", fake_backfill)
    result = runner.invoke(cli.app, ["backfill", "--from", "2026-09-01", "--to", "2026-09-02",
                                     "--datasets", "klines_1h", "--symbols", "BTCUSDT,ETHUSDT"])
    assert result.exit_code == 0, result.stdout
    assert captured["start"] == "2026-09-01" and captured["end"] == "2026-09-02"
    assert captured["datasets"] == ["klines_1h"]
    assert captured["symbols"] == ["BTCUSDT", "ETHUSDT"]


def test_health_intervals_cover_all_tasks():
    from collector.config import Settings
    hi = cli.health_intervals(Settings(_env_file=None))
    assert set(hi) == set(cli.ALL_TASKS)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest collector/tests/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.cli'`

- [ ] **Step 3: Implement cli.py**

```python
from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import typer

from collector import backfill as backfill_mod
from collector import lake
from collector.binance.rest import BinanceRest
from collector.config import Settings, get_settings
from collector.scheduler import run_periodic, supervise
from collector.state import StateStore
from collector.tasks import dvol, funding, liquidations, metrics, symbols
from collector.tasks.context import Context
from collector.tasks.premium import PremiumTask

app = typer.Typer(no_args_is_help=True, add_completion=False)
ALL_TASKS = ("symbols", "metrics", "premium", "funding", "liquidations", "dvol")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)


def health_intervals(settings: Settings) -> dict[str, float]:
    return {
        "symbols": 86400, "metrics": settings.metrics_interval_s,
        "premium": settings.premium_interval_s, "funding": 86400,
        "liquidations": 900, "dvol": settings.dvol_interval_s,
    }


def select_tasks(only: str) -> list[str]:
    if not only.strip():
        return list(ALL_TASKS)
    names = [n.strip() for n in only.split(",") if n.strip()]
    bad = [n for n in names if n not in ALL_TASKS]
    if bad:
        raise ValueError(f"unknown tasks: {bad}; valid: {ALL_TASKS}")
    return names


def build_context(settings: Settings) -> Context:
    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    return Context(settings=settings, rest=BinanceRest(settings.binance_fapi_base),
                   http=httpx.AsyncClient(timeout=30.0), state=StateStore(data_dir / "state.json"),
                   data_dir=data_dir)


async def run_tasks(ctx: Context, names: list[str], *, premium_flush_every: int = 15) -> None:
    s, st = ctx.settings, ctx.state
    coros = []
    if "symbols" in names:
        coros.append(run_periodic("symbols", 86400, lambda: symbols.run_once(ctx), st, offset_s=300, align=True))
    if "metrics" in names:
        coros.append(run_periodic("metrics", s.metrics_interval_s, lambda: metrics.run_once(ctx), st, offset_s=120, align=True))
    if "premium" in names:
        task = PremiumTask(ctx, flush_every=premium_flush_every)
        coros.append(run_periodic("premium", s.premium_interval_s, task.poll, st, align=True))
    if "funding" in names:
        coros.append(run_periodic("funding", 86400, lambda: funding.run_once(ctx), st, offset_s=1800, align=True))
    if "liquidations" in names:
        coros.append(supervise("liquidations", lambda: liquidations.run(ctx), st))
    if "dvol" in names:
        coros.append(run_periodic("dvol", s.dvol_interval_s, lambda: dvol.run_once(ctx), st, align=True))
    try:
        await asyncio.gather(*coros)
    finally:
        await ctx.http.aclose()
        await ctx.rest.aclose()


@app.command()
def run(only: str = typer.Option("", help="Comma-separated subset of tasks")) -> None:
    """Start the always-on collector."""
    settings = get_settings()
    setup_logging(settings.log_level)
    names = select_tasks(only)
    logging.getLogger(__name__).info("starting tasks=%s data_dir=%s", names, settings.data_dir)
    asyncio.run(run_tasks(build_context(settings), names))


def _yesterday() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


@app.command()
def backfill(start: str = typer.Option(None, "--from", help="YYYY-MM-DD, default 90 days ago"),
             end: str = typer.Option(None, "--to", help="YYYY-MM-DD, default yesterday"),
             datasets: str = typer.Option("klines_1h,vision_metrics,vision_funding"),
             symbols: str = typer.Option("all", help="'all' or comma list")) -> None:
    """Download historical files from data.binance.vision into the lake."""
    settings = get_settings()
    setup_logging(settings.log_level)
    end = end or _yesterday()
    start = start or (date.fromisoformat(end) - timedelta(days=90)).isoformat()
    ds = [d.strip() for d in datasets.split(",") if d.strip()]
    syms = None if symbols.strip().lower() == "all" else [x.strip() for x in symbols.split(",") if x.strip()]
    ctx = build_context(settings)

    async def go():
        try:
            return await backfill_mod.backfill(ctx, start=start, end=end, datasets=ds, symbols=syms)
        finally:
            await ctx.http.aclose()
            await ctx.rest.aclose()

    stats = asyncio.run(go())
    typer.echo(json.dumps(stats))


@app.command()
def compact(date_: str = typer.Option(None, "--date", help="YYYY-MM-DD, default yesterday"),
            dataset: str = typer.Option(None, help="default: all datasets")) -> None:
    """Rewrite one day's partition per dataset with duplicates removed."""
    settings = get_settings()
    day = date_ or _yesterday()
    targets = [dataset] if dataset else list(lake.DATASETS)
    for ds in targets:
        removed, rows = lake.compact(settings.data_dir, ds, day)
        typer.echo(f"{ds:20s} {day} files_removed={removed} rows={rows}")


@app.command()
def status() -> None:
    """Show task health and dataset summary. Exit 1 if any task is unhealthy."""
    settings = get_settings()
    state = StateStore(Path(settings.data_dir) / "state.json")
    unhealthy = 0
    typer.echo("TASKS")
    for name, interval in health_intervals(settings).items():
        rec = state.get(name)
        ok = state.is_healthy(name, interval)
        unhealthy += 0 if ok else 1
        last = rec["last_success_ms"]
        last_s = datetime.fromtimestamp(last / 1000, tz=timezone.utc).isoformat(timespec="seconds") if last else "-"
        typer.echo(f"  {name:13s} {'ok' if ok else 'UNHEALTHY':9s} last_success={last_s} "
                   f"errors={rec['consecutive_errors']} last_error={rec['last_error'] or '-'}")
    typer.echo("DATASETS")
    for ds in lake.DATASETS:
        parts = lake.partitions(settings.data_dir, ds)
        typer.echo(f"  {ds:18s} files={lake.file_count(settings.data_dir, ds):5d} "
                   f"partitions={len(parts):4d} latest={parts[-1] if parts else '-'}")
    raise typer.Exit(code=1 if unhealthy else 0)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest collector/tests/test_cli.py -v`
Expected: 6 passed. Then run the whole suite: `uv run pytest -q` → all green.

- [ ] **Step 5: Smoke-run locally against real endpoints (manual, requires network)**

```bash
TRADER_DATA_DIR=./data uv run collector run --only symbols,premium,dvol
```
Watch JSON logs for `task=symbols ok`, `task=premium ok`, `task=dvol ok`, then Ctrl+C. Then:
```bash
TRADER_DATA_DIR=./data uv run collector status
```
Expected: `symbols`, `premium`, `dvol` show `ok`; datasets `symbols` and `dvol` list one partition (premium flushes after 15 polls, so it may still show 0 files). On PowerShell set the variable with `$env:TRADER_DATA_DIR = "./data"` first.

- [ ] **Step 6: Commit**

```bash
git add collector/src/collector/cli.py collector/tests/test_cli.py
git commit -m "Add collector CLI with run, backfill, compact and status"
```

---

### Task 17: Dockerfile, docker-compose and end-to-end acceptance

**Files:**
- Create: `collector/Dockerfile`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `data/.gitkeep` (so the bind mount directory exists on first clone)

**Interfaces:**
- Consumes: the `collector` console script from Task 16.
- Produces: `docker compose up -d collector` runs `collector run`; `docker compose run --rm collector <subcommand>` runs one-off commands; build context is the repo root (pyproject lives there), so the Dockerfile path is `collector/Dockerfile` with `context: .`. This differs from the spec's shorthand `build: ./collector` and is the intended reading.

- [ ] **Step 1: Write collector/Dockerfile**

```dockerfile
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    TRADER_DATA_DIR=/data

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY collector/src ./collector/src
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
VOLUME ["/data"]
ENTRYPOINT ["collector"]
CMD ["run"]
```

- [ ] **Step 2: Write docker-compose.yml**

```yaml
services:
  collector:
    build:
      context: .
      dockerfile: collector/Dockerfile
    command: ["run"]
    env_file:
      - path: .env
        required: false
    environment:
      TRADER_DATA_DIR: /data
    volumes:
      - ./data:/data
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "20m"
        max-file: "5"

  freqtrade:
    image: freqtradeorg/freqtrade:stable
    command: trade --config /freqtrade/user_data/config.json --strategy EmaCrossSample
    volumes:
      - ./freqtrade/user_data:/freqtrade/user_data
    ports:
      - "127.0.0.1:8080:8080"
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "20m"
        max-file: "5"
```

- [ ] **Step 3: Write .env.example**

```
# Copy to .env to override. All values optional; defaults shown.
TRADER_DATA_DIR=/data
TRADER_BINANCE_FAPI_BASE=https://fapi.binance.com
TRADER_BINANCE_WS_BASE=wss://fstream.binance.com/market
TRADER_DERIBIT_BASE=https://www.deribit.com
TRADER_METRICS_INTERVAL_S=3600
TRADER_PREMIUM_INTERVAL_S=60
TRADER_DVOL_INTERVAL_S=3600
TRADER_LOG_LEVEL=INFO
```

Add `.env` to `.gitignore` if not already present (it is, from the initial scaffold). Create `data/.gitkeep` and add `!data/.gitkeep` below the `data/` rule in `.gitignore`.

- [ ] **Step 4: Verify network reachability before building (manual)**

```bash
curl -sS -o /dev/null -w "%{http_code}\n" https://fapi.binance.com/fapi/v1/ping
curl -sS -o /dev/null -w "%{http_code}\n" "https://www.deribit.com/api/v2/public/get_time"
curl -sS -o /dev/null -w "%{http_code}\n" https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2025-08.zip.CHECKSUM
```
Expected: `200` three times. If any is not 200, stop and report; the design does not include proxy configuration.

- [ ] **Step 5: Build and start the collector**

Start Docker Desktop first. Then:
```bash
docker compose build collector
docker compose up -d collector
docker compose logs -f collector
```
Expected within 2 minutes: JSON log lines `task=symbols ok`, `task=dvol ok`, `task=premium ok`, `liquidations connected`. `task=metrics ok` appears after the first hourly run (it starts immediately, so within ~5 minutes for ~450 symbols at 800 req / 5 min).

- [ ] **Step 6: Acceptance checks (spec §7.2)**

```bash
docker compose exec collector collector status
docker compose run --rm collector backfill --from 2026-09-06 --to 2026-09-12 --symbols BTCUSDT,ETHUSDT
docker compose run --rm collector compact --date 2026-09-12
docker compose run --rm collector run --only funding   # Ctrl+C after "task=funding ok"
```
Expected: `status` lists all six tasks `ok` after 10 minutes and exit code 0; `backfill` prints JSON with `downloaded > 0` and `failed == 0`; `compact` prints one line per dataset; `data/lake/` contains the nine live dataset directories plus `funding_rate`, `klines_1h`, `vision_metrics`, `vision_funding`.

Query check from the host:
```bash
uv run python -c "from collector import lake; print(lake.view('data','oi_hist').count('*').fetchone())"
```

- [ ] **Step 7: Commit**

```bash
git add collector/Dockerfile docker-compose.yml .env.example data/.gitkeep .gitignore
git commit -m "Add collector Dockerfile and docker-compose services"
```

---

### Task 18: freqtrade dry-run skeleton

**Files:**
- Create: `freqtrade/user_data/config.json`
- Create: `freqtrade/user_data/strategies/EmaCrossSample.py`
- Create: `freqtrade/user_data/.gitkeep` directories as needed (`data/`, `logs/` are created by freqtrade at runtime and are gitignored)

**Interfaces:**
- Consumes: `docker-compose.yml` service `freqtrade` from Task 17.
- Produces: a runnable Binance USDⓈ-M futures dry-run with FreqUI on `http://127.0.0.1:8080`.

- [ ] **Step 1: Write config.json**

```json
{
  "$schema": "https://schema.freqtrade.io/schema.json",
  "bot_name": "trader-dryrun",
  "dry_run": true,
  "dry_run_wallet": 10000,
  "stake_currency": "USDT",
  "stake_amount": 1000,
  "tradable_balance_ratio": 0.99,
  "max_open_trades": 3,
  "timeframe": "1h",
  "trading_mode": "futures",
  "margin_mode": "isolated",
  "cancel_open_orders_on_exit": false,
  "process_only_new_candles": true,
  "unfilledtimeout": { "entry": 10, "exit": 10, "unit": "minutes" },
  "entry_pricing": {
    "price_side": "same",
    "use_order_book": true,
    "order_book_top": 1,
    "price_last_balance": 0.0,
    "check_depth_of_market": { "enabled": false, "bids_to_ask_delta": 1 }
  },
  "exit_pricing": { "price_side": "same", "use_order_book": true, "order_book_top": 1 },
  "exchange": {
    "name": "binance",
    "key": "",
    "secret": "",
    "ccxt_config": {},
    "ccxt_async_config": {},
    "pair_whitelist": ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT"],
    "pair_blacklist": []
  },
  "pairlists": [{ "method": "StaticPairList" }],
  "telegram": { "enabled": false, "token": "", "chat_id": "" },
  "api_server": {
    "enabled": true,
    "listen_ip_address": "0.0.0.0",
    "listen_port": 8080,
    "verbosity": "error",
    "enable_openapi": false,
    "jwt_secret_key": "change-me-local-only-3f9c2b1e",
    "ws_token": "change-me-local-only-ws-7a1d",
    "CORS_origins": [],
    "username": "freqtrader",
    "password": "local-dryrun-only"
  },
  "initial_state": "running",
  "force_entry_enable": false,
  "internals": { "process_throttle_secs": 5 },
  "dataformat_ohlcv": "feather",
  "dataformat_trades": "feather"
}
```

The API credentials are intentionally trivial: the port is bound to `127.0.0.1` only. Do not expose it beyond localhost without changing them.

- [ ] **Step 2: Write EmaCrossSample.py**

```python
"""Flow-validation strategy only. Proves download -> backtest -> dry-run works.
Not a trading opinion; do not run with real funds."""
from datetime import datetime

import talib.abstract as ta
from pandas import DataFrame

from freqtrade.strategy import IStrategy


class EmaCrossSample(IStrategy):
    INTERFACE_VERSION = 3
    timeframe = "1h"
    can_short = True
    minimal_roi = {"0": 0.05}
    stoploss = -0.03
    startup_candle_count = 60
    process_only_new_candles = True

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["ema_fast"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema_slow"] = ta.EMA(dataframe, timeperiod=50)
        return dataframe

    @staticmethod
    def _cross_up(df: DataFrame):
        return (df["ema_fast"] > df["ema_slow"]) & (df["ema_fast"].shift(1) <= df["ema_slow"].shift(1))

    @staticmethod
    def _cross_down(df: DataFrame):
        return (df["ema_fast"] < df["ema_slow"]) & (df["ema_fast"].shift(1) >= df["ema_slow"].shift(1))

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        has_volume = dataframe["volume"] > 0
        dataframe.loc[self._cross_up(dataframe) & has_volume, "enter_long"] = 1
        dataframe.loc[self._cross_down(dataframe) & has_volume, "enter_short"] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[self._cross_down(dataframe), "exit_long"] = 1
        dataframe.loc[self._cross_up(dataframe), "exit_short"] = 1
        return dataframe

    def leverage(self, pair: str, current_time: datetime, current_rate: float,
                 proposed_leverage: float, max_leverage: float, entry_tag: str | None,
                 side: str, **kwargs) -> float:
        return 2.0
```

- [ ] **Step 3: Download data and backtest (manual, network)**

```bash
docker compose pull freqtrade
docker compose run --rm freqtrade download-data --config /freqtrade/user_data/config.json --timerange 20250101- -t 1h
docker compose run --rm freqtrade backtesting --config /freqtrade/user_data/config.json --strategy EmaCrossSample --timerange 20250101-
```
Expected: download reports OHLCV plus funding-rate and mark-price data for the four pairs (futures mode downloads them automatically); backtesting prints a results table with trades for both long and short. Performance numbers are irrelevant; a non-empty table is the pass criterion. If TA-Lib import fails inside the image, you are on a non-official image; use `freqtradeorg/freqtrade:stable` exactly.

- [ ] **Step 4: Start dry-run and check FreqUI**

```bash
docker compose up -d freqtrade
docker compose logs --tail 50 freqtrade
```
Expected log lines include `Dry run is enabled` and `Bot started`. Open `http://127.0.0.1:8080`, log in with `freqtrader` / `local-dryrun-only`, confirm the four pairs appear under the whitelist.

- [ ] **Step 5: Pin the image tag**

Run `docker compose images freqtrade` to read the pulled version, then replace `freqtradeorg/freqtrade:stable` in `docker-compose.yml` with that exact tag (for example `freqtradeorg/freqtrade:2026.8`).

- [ ] **Step 6: Commit**

```bash
git add freqtrade/user_data/config.json freqtrade/user_data/strategies/EmaCrossSample.py docker-compose.yml
git commit -m "Add freqtrade Binance futures dry-run skeleton with sample strategy"
```

---

## Plan Self-Review

**Spec coverage**

| Spec section | Task(s) |
|---|---|
| §3 repo structure | 1, 17, 18 |
| §4.1 symbol universe | 6 (filter), 13 (`symbols`) |
| §4.2 six tasks + budgets | 8 (buckets 800/300 s, 2000/60 s), 13, 14, 16 (`run_tasks` intervals and offsets) |
| §4.3 datasets and columns | 3 (`DATASETS`), 6, 7, 10, 11 (parsers produce exact columns) |
| §4.4 write / partition / compact / view | 3, 4 |
| §4.5 rate limit, WS backoff, state, JSON logs | 8, 9, 5, 16 (`JsonFormatter`) |
| §4.6 backfill | 11, 15 |
| §4.7 CLI | 16 |
| §4.8 config | 2, 17 (`.env.example`) |
| §4.9 dependencies | 1 |
| §5 freqtrade | 18 |
| §6 compose | 17 |
| §7.1 tests | every task step 1 |
| §7.2 acceptance | 17 step 6, 18 steps 3–4 |
| §8 risks | 17 step 4 (network probe), 18 step 5 (pin tag), 6 (`rate_type` nullable) |

**Deviations from spec, decided here**
- Docker build context is the repo root (`context: .`, `dockerfile: collector/Dockerfile`) because `pyproject.toml` lives at the root.
- `premium_index` and `liquidations` buffer in memory and flush every 15 polls / 500 rows or 60 s to keep file counts sane; up to 15 minutes of premium data can be lost on an unclean stop. `compact` still runs daily.
- Backfill skip-tracking uses `data/backfill_manifest.json` instead of inspecting partitions, so a day that already has live-collected rows is still backfilled once.
- Liquidations health window is 900 s and `connected` events count as success, since quiet markets produce no rows.

**Type consistency check**
- `write_rows(data_dir, dataset, rows, ingest_ms=...)` used identically in Tasks 3, 13, 14, 15.
- `parse_ls_ratio(payload, symbol, ingest_ms)` and `parse_taker_ratio(payload, symbol, ingest_ms)` signatures match their use in `metrics.ENDPOINTS` (all three args positional).
- `run_periodic(name, interval_s, fn, state, *, offset_s, align, run_immediately, sleep, wall, max_runs)` matches Task 16 calls.
- `fetch_dvol(client, base_url, currency, start_ms, end_ms, ingest_ms, resolution)` matches `dvol.run_once`.
- `fetch_csv(client, kind, symbol, date, timeframe)` matches the `fake_fetch` double in Task 15 tests.
- `StateStore.get(task)["detail"]` is a dict; `LiquidationSink.flush` stores `{"rows": n}` and Task 14 tests assert exactly that.

**Placeholder scan**: no TBD/TODO; every code step contains the code.

