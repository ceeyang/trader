# Phase 0 设计：Binance 永续数据采集器 + freqtrade 骨架

*日期：2026-09-13 ｜ 状态：已由用户口头确认方案 A，待审阅本文档 ｜ 上游：[调研报告](../../research/ai-perp-trading-research-2026-09-12.md) 第 0 章与第 8.3 节*

## 1. 目标

搭起项目的第一层地基，两件事并行：

1. **数据采集器**：一个常驻进程，持续把 Binance USDⓈ-M 永续合约的衍生品指标、清算流、资金费率和 Deribit DVOL 落到本地 Parquet 数据湖，并能从 Binance Vision 回填历史。动机是 Binance 的 OI / 多空比 / taker 买卖量接口只保留最近 30 天，不采就丢。
2. **freqtrade 骨架**：用官方 Docker 镜像跑起 Binance 合约 dry-run，带一个示例策略，证明「下载数据 → 回测 → 模拟盘 → FreqUI」流程通畅，作为后续 Phase 2～4 的执行底座。

两者跑在本机 Windows 的 Docker Compose 里，Python 依赖用 uv 管理。全程只用公共接口，不需要交易所 API key，不产生任何真实交易。

## 2. 非目标（本阶段明确不做）

- 不接 OKX / Bybit / Hyperliquid
- 不采 tick、逐笔成交、订单簿
- 不做 Postgres / TimescaleDB
- 不做 Telegram / 邮件告警
- 不写任何真实交易策略，示例策略只用于验证流程
- 不接 LLM
- 不做多机双活

## 3. 仓库结构

```
trader/
├── docker-compose.yml              # 服务：collector、freqtrade
├── .env.example                    # 采集器可配环境变量样例
├── pyproject.toml                  # uv 工程，包名 collector
├── uv.lock
├── collector/
│   ├── Dockerfile                  # python:3.12-slim + uv sync
│   ├── src/collector/
│   │   ├── __init__.py
│   │   ├── cli.py                  # typer 入口：run / backfill / compact / status
│   │   ├── config.py               # pydantic-settings，读环境变量
│   │   ├── binance/
│   │   │   ├── rest.py             # httpx 客户端 + 令牌桶限速 + 429/418 退避
│   │   │   ├── ws.py               # websockets 客户端 + 指数退避重连
│   │   │   ├── parsers.py          # 各接口响应 → 归一化行（纯函数）
│   │   │   └── vision.py           # data.binance.vision 下载与解压
│   │   ├── deribit.py              # DVOL 拉取与解析
│   │   ├── lake.py                 # Parquet 写入、分区、compact 去重、DuckDB 视图
│   │   ├── state.py                # data/state.json 读写
│   │   └── tasks/
│   │       ├── symbols.py          # exchangeInfo 快照
│   │       ├── metrics.py          # 5 个 /futures/data 指标
│   │       ├── premium.py          # premiumIndex 全量
│   │       ├── funding.py          # 已结算资金费率
│   │       ├── liquidations.py     # !forceOrder@arr
│   │       └── dvol.py
│   └── tests/
├── freqtrade/
│   └── user_data/
│       ├── config.json             # Binance 合约 dry-run
│       └── strategies/EmaCrossSample.py
├── data/                           # 数据湖，gitignore
│   ├── lake/<dataset>/date=YYYY-MM-DD/part-<ingest_ts>.parquet
│   └── state.json
└── docs/superpowers/specs/         # 本文档
```

## 4. 采集器设计

### 4.1 币种范围

每天 00:05 UTC 从 `GET /fapi/v1/exchangeInfo` 拉一次，筛选 `contractType == PERPETUAL` 且 `status == TRADING` 且 `quoteAsset == USDT`（约 450 个）。整份快照落 `symbols` 数据集，供追踪上架下架；其他任务从最新快照读币种列表。启动时若无快照则立即拉一次。

### 4.2 采集任务

每个任务是一个独立 asyncio 协程，由 `run` 子命令统一启动；任一任务异常退出时记录错误并按退避重启，不影响其他任务。

| 任务 | 触发 | 接口 | 请求预算 |
|---|---|---|---|
| symbols | 每日 | `/fapi/v1/exchangeInfo` | 1 次/天 |
| metrics | 每小时整点后 2 分钟 | `/futures/data/openInterestHist`、`globalLongShortAccountRatio`、`topLongShortPositionRatio`、`topLongShortAccountRatio`、`takerlongshortRatio`，均 `period=5m&limit=500` | 约 450 × 5 = 2250 次/小时；接口独立限额 1000 次/5 分钟，令牌桶设 800 次/5 分钟 |
| premium | 每分钟 | `/fapi/v1/premiumIndex` 不带 symbol | 1 次/分钟，权重 10 |
| funding | 每日 00:30 UTC | `/fapi/v1/fundingRate?symbol=X&limit=1000` | 约 450 次/天 |
| liquidations | 常驻 WebSocket | `wss://fstream.binance.com/market/ws/!forceOrder@arr` | 无 REST 消耗 |
| dvol | 每小时 | `GET https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC\|ETH&resolution=3600` | 2 次/小时 |

metrics 用 `limit=500` 拉最近约 41 小时，与上次有大量重叠，靠 compact 去重；好处是任一小时失败都会在下一小时自动补齐，不需要断点续传逻辑。

### 4.3 数据集与列定义

所有时间戳列为 int64 毫秒 UTC，命名以 `_ms` 结尾；所有数值列为 float64；`symbol` 为 string；每张表附 `ingest_ms`（写入时间）。

| 数据集 | 列 | 去重键 |
|---|---|---|
| symbols | snapshot_date (string, YYYY-MM-DD), symbol, pair, contract_type, status, onboard_date_ms, price_precision, quantity_precision, ingest_ms | (snapshot_date, symbol) |
| oi_hist | symbol, timestamp_ms, sum_open_interest, sum_open_interest_value, ingest_ms | (symbol, timestamp_ms) |
| ls_global_account | symbol, timestamp_ms, long_short_ratio, long_account, short_account, ingest_ms | (symbol, timestamp_ms) |
| ls_top_position | 同上 | 同上 |
| ls_top_account | 同上 | 同上 |
| taker_ratio | symbol, timestamp_ms, buy_sell_ratio, buy_vol, sell_vol, ingest_ms | (symbol, timestamp_ms) |
| premium_index | symbol, time_ms, mark_price, index_price, estimated_settle_price, last_funding_rate, interest_rate, next_funding_time_ms, ingest_ms | (symbol, time_ms) |
| funding_rate | symbol, funding_time_ms, funding_rate, mark_price, rate_type (string, 可空), ingest_ms | (symbol, funding_time_ms) |
| liquidations | event_time_ms, symbol, side, order_type, time_in_force, orig_qty, price, avg_price, status, last_filled_qty, filled_accum_qty, trade_time_ms, ingest_ms | (symbol, trade_time_ms, side, orig_qty, price) |
| dvol | currency, timestamp_ms, open, high, low, close, ingest_ms | (currency, timestamp_ms) |

回填数据集（来自 Binance Vision，见 4.6）：

| 数据集 | 列 | 去重键 |
|---|---|---|
| klines_1h / klines_5m | symbol, open_time_ms, open, high, low, close, volume, close_time_ms, quote_volume, trade_count, taker_buy_base, taker_buy_quote, ingest_ms | (symbol, open_time_ms) |
| vision_metrics | symbol, create_time_ms, sum_open_interest, sum_open_interest_value, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio, count_long_short_ratio, sum_taker_long_short_vol_ratio, ingest_ms | (symbol, create_time_ms) |
| vision_funding | symbol, calc_time_ms, funding_interval_hours, last_funding_rate, ingest_ms | (symbol, calc_time_ms) |

### 4.4 写入、分区与 compact

- 每个任务把一批归一化行按「数据时间戳所属 UTC 日期」拆分，写到 `data/lake/<dataset>/date=YYYY-MM-DD/part-<ingest_ms>.parquet`。追加写，永不改已有文件。
- `compact --date YYYY-MM-DD [--dataset X]`：读该日期分区全部文件，按去重键去重（保留 `ingest_ms` 最大的一行），写成单个 `compacted-<ingest_ms>.parquet`，成功后删除旧文件。默认对所有数据集执行昨天的分区。compact 是幂等的，可重复跑。
- 读取：`lake.py` 提供 `view(dataset)` 返回 DuckDB 查询 `read_parquet('data/lake/<dataset>/**/*.parquet', hive_partitioning=true)` 并在 SQL 里做同样的去重，保证未 compact 时读到的也是干净数据。
- 每次写入用 pyarrow 显式 schema，防止不同批次列类型漂移。

### 4.5 限速、容错与状态

- REST：`rest.py` 内置两个令牌桶。`/futures/data/*` 一桶 800 次 / 5 分钟；其余 `/fapi/*` 按权重一桶 2000 / 分钟（官方 2400）。收到 429 读 `Retry-After` 休眠；收到 418 休眠 5 分钟并记错误。所有请求超时 10 秒，失败重试 3 次指数退避。
- WebSocket：断线后 1s 起指数退避到最大 60s 重连；Binance 24 小时强制断开视为正常重连；每条消息解析失败只记日志不中断。
- 状态：`data/state.json` 记录每个任务的 `last_success_ms`、`last_error`、`consecutive_errors`。`status` 子命令读它并按「距上次成功是否超过 2 倍周期」标红。
- 日志：标准库 logging 输出到 stdout，JSON 一行一条，交给 Docker 收集。
- 时钟：容器用宿主时间；本阶段不签名请求，无 recvWindow 问题。

### 4.6 历史回填

`backfill --from 2025-06-01 --to 2026-09-12 --datasets klines_1h,vision_metrics,vision_funding --symbols all|BTCUSDT,ETHUSDT`：

- 从 `https://data.binance.vision/data/futures/um/daily/{klines/<symbol>/1h,metrics/<symbol>}/...zip` 与 `monthly/fundingRate/<symbol>/...zip` 下载，校验 `.CHECKSUM`，解压 CSV 转 Parquet 写入对应数据集。
- 已存在的日期分区跳过，可重复执行。
- 并发下载上限 8，失败文件记录到日志并继续。
- 默认范围最近 90 天、`klines_1h` + `vision_metrics` + `vision_funding`、全部币种。1m/5m K 线体积大，只在显式指定时下载。

### 4.7 CLI

| 命令 | 作用 |
|---|---|
| `collector run [--only metrics,liquidations]` | 启动全部（或指定）常驻任务 |
| `collector backfill ...` | 见 4.6 |
| `collector compact [--date D] [--dataset X]` | 见 4.4 |
| `collector status` | 打印各任务健康状态与各数据集最新时间戳、文件数 |

### 4.8 配置项（环境变量，前缀 `TRADER_`）

`DATA_DIR`（默认 `/data`）、`BINANCE_FAPI_BASE`、`BINANCE_WS_BASE`、`DERIBIT_BASE`、`METRICS_INTERVAL_S`（3600）、`PREMIUM_INTERVAL_S`（60）、`DVOL_INTERVAL_S`（3600）、`LOG_LEVEL`。全部有默认值，`.env.example` 列出。

### 4.9 依赖

httpx、websockets、pyarrow、duckdb、pydantic-settings、typer、tenacity。测试：pytest、pytest-asyncio、respx。不用 ccxt：这些接口 ccxt 大多未封装，直接调更透明。

## 5. freqtrade 骨架

- 镜像 `freqtradeorg/freqtrade:stable`，compose 服务名 `freqtrade`，挂载 `./freqtrade/user_data:/freqtrade/user_data`。
- `config.json` 关键项：`exchange.name=binance`、`trading_mode=futures`、`margin_mode=isolated`、`dry_run=true`、`dry_run_wallet=10000`、`stake_currency=USDT`、`timeframe=1h`、`max_open_trades=3`、静态 pairlist `BTC/USDT:USDT`、`ETH/USDT:USDT`、`SOL/USDT:USDT`、`BNB/USDT:USDT`；`api_server.enabled=true` 监听 `0.0.0.0:8080`（compose 只映射到宿主 `127.0.0.1:8080`），用户名密码在 config 里明文但仅本机可达。不填交易所 key（dry-run 不需要）。
- 策略 `EmaCrossSample`：EMA(20) 上穿 EMA(50) 做多、下穿做空，`can_short=True`，`leverage()` 回调固定返回 2，`stoploss=-0.03`，`minimal_roi={"0": 0.05}`。文件头注明「仅验证流程，不代表策略观点」。
- 数据下载与回测用一次性容器：`docker compose run --rm freqtrade download-data --timerange 20250101- -t 1h` 与 `docker compose run --rm freqtrade backtesting --strategy EmaCrossSample --timerange 20250101-`。
- `freqtrade/user_data/{data,logs,backtest_results,hyperopt_results}/` 与 `tradesv3*.sqlite` 加入 .gitignore。

## 6. Docker Compose

```yaml
services:
  collector:
    build: ./collector
    command: collector run
    env_file:
      - path: .env
        required: false     # 缺失时用代码默认值
    volumes:
      - ./data:/data
    restart: unless-stopped
  freqtrade:
    image: freqtradeorg/freqtrade:stable
    command: trade --config /freqtrade/user_data/config.json --strategy EmaCrossSample
    volumes:
      - ./freqtrade/user_data:/freqtrade/user_data
    ports:
      - "127.0.0.1:8080:8080"
    restart: unless-stopped
```

collector 的 Dockerfile：`python:3.12-slim`，复制 `pyproject.toml`、`uv.lock`、`collector/`，`uv sync --frozen --no-dev`，入口 `collector`。宿主上 `uv run collector ...` 也能直接跑，便于开发调试。

## 7. 测试与验收

### 7.1 采集器测试（pytest）

- `parsers.py`：每个接口给一份真实响应样例（存 `tests/fixtures/`），断言归一化后的列名、类型、行数。
- `lake.py`：临时目录下写两批有重叠的行，compact 后行数等于去重后数量且保留 `ingest_ms` 最大者；`view()` 未 compact 时也返回去重结果。
- `rest.py`：respx 模拟 429 带 `Retry-After` 与 418，断言退避行为；令牌桶超额时阻塞。
- `ws.py`：只测消息处理函数（输入一条 forceOrder JSON，输出一行），不起真实 socket。
- `state.py`：读写与「超过 2 倍周期判不健康」逻辑。
- `vision.py`：给一个本地 zip + CHECKSUM，断言校验通过与转 Parquet 正确；校验失败时抛错。
- 一个 `@pytest.mark.network` 集成测试真实请求 Binance 与 Deribit 各一次，默认 `-m "not network"` 跳过。

### 7.2 验收标准

1. `docker compose up -d` 后 10 分钟内 `docker compose exec collector collector status` 显示 6 个任务全部健康，`data/lake/` 下出现 `symbols`、`oi_hist`、`ls_global_account`、`ls_top_position`、`ls_top_account`、`taker_ratio`、`premium_index`、`liquidations`、`dvol` 目录（`funding_rate` 在次日 00:30 后出现，验收时可用 `--only funding` 手动触发一次）。
2. `collector backfill --from <7 天前> --symbols BTCUSDT,ETHUSDT` 成功，DuckDB 能查出对应行。
3. `collector compact` 对昨天分区执行后文件数减少且行数与去重预期一致。
4. `uv run pytest` 全绿。
5. freqtrade 容器日志出现 dry-run 启动信息，浏览器打开 `http://127.0.0.1:8080` 能登录 FreqUI 看到 4 个币对；`backtesting` 命令能输出一份结果表。

## 8. 风险与待验证项

| 项 | 处理 |
|---|---|
| Binance Vision `metrics` 日度文件的行粒度官方未明示（社区称 5 分钟） | 回填时以实际文件为准，不在代码里假设间隔 |
| `fundingRate` 响应中 `rate_type` 字段 2026-07 新增，旧数据可能没有 | 列设为可空 string |
| 本机网络能否直连 `fapi.binance.com`、`data.binance.vision`、`www.deribit.com` | 实施第一步先 curl 探测；不通则需用户自行解决网络，本设计不含代理配置 |
| Docker Desktop 当前未运行 | 动手前启动 |
| 418 封 IP 最长 3 天 | 令牌桶留 20% 余量；`status` 会显示连续错误 |
| 清算流每币每秒仅 1 条快照，系统性低估 | 记录在数据集文档中，下游使用时知悉 |
| freqtrade `stable` 标签会随版本漂移 | 第一次拉取后在 compose 里固定到具体版本号 |
