# AI 自动化永续合约交易：调研报告

*生成日期：2026-09-12 ｜ 调研方式：4 个并行调研 agent，约 140 次联网搜索、100+ 页面深读 ｜ 来源约 250 个 ｜ 整体置信度：中高（核心结论多源交叉验证；单一来源信息已在文末单独列出）*

> 本报告按「合约 = 加密货币永续合约（Binance / OKX / Bybit / Hyperliquid）」理解。若你指的是股指期货或商品期货，基础设施与合规部分需要另行调研，但第 3～5 章（信号、AI 定位、回测与风控）的方法论通用。

---

## 执行摘要

1. **LLM 直接做交易决策，目前是亏钱的。** 2025 年 10 月 nof1 Alpha Arena 用真金让 6 个前沿模型在 Hyperliquid 交易永续，4 个亏 30%～63%，只有 Qwen3 Max（约 +22%）和 DeepSeek V3.1（约 +5%）盈利；多个竞技场的共同结论是「中位数模型总在亏钱」「agent 架构比底层模型更决定结果」。创始人自述：LLM 不擅长处理数值时间序列。
2. **AI 的正确位置在研究链上游和风控叙事层，执行必须是确定性代码。** 有实证的正面案例是「LLM 提假设 → 受约束 DSL → 确定性引擎验证」，样本外 Sharpe 1.55；有实证的负面案例是 TradeTrap：把新闻/社交数据喂给能下单的 LLM，可被系统性误导。
3. **最有实证的信号是资金费率/基差（carry）和 OI-杠杆堆积，但要当「风险预警」用，不是「择时」用。** carry 策略 2020～2024 年化 Sharpe 4～6，2025 年转负；「OI 创新高 + 资金费率持续 >15% APR」在 2025 年每次大回调前都出现，但严格研究证明没有任何单一变量能跨事件稳定预警。
4. **趋势/动量是唯一未显著衰减的收益因子**，但交易成本吃掉大半，alpha 集中在大市值币、牛市和空头腿。订单簿失衡（OBI）对个人无经济价值。多空比反向没有任何学术实证。
5. **数据必须从今天开始自己采。** Binance 的 OI / 多空比 / 主动买卖量接口只保留最近 30 天，Binance Vision 免费历史数据不含清算；不持续落库，历史就永远丢了。
6. **10bp 的手续费足以把小时级 ML 策略从 +73% 打到 -64%。** Binance 永续 taker 0.05%，一个来回正好 10bp。所有回测必须包含资金费率（按实际结算时刻）、标记价触发清算、maker/taker 分开计费、滑点、已下架合约。
7. **合规风险真实且在收紧。** 2026 年 2 月央行等八部门「42 号文」取代 2021 年「9·24 通知」，境外交易所向境内主体提供服务属非法、损失自担；香港永续合约仅限专业投资者；Binance / OKX / Bybit 均将中国大陆列为受限地区。

---

## 0. 先给答案：应该怎么做

按下面的顺序做，每一步都有前一步的产出作为输入。不要跳过 Phase 0 和 Phase 3。

| 阶段 | 目标 | 产出 | 时长参考 |
|---|---|---|---|
| **Phase 0 · 数据采集器** | 从今天起持续落库 30 天窗口的衍生品指标 | 一个常驻进程 + Parquet/DuckDB 数据湖 | 1 周内上线 |
| **Phase 1 · 研究框架** | 因子库、标签（三重障碍）、purged walk-forward、IC 评估 | Jupyter + 因子评估报告 | 3～6 周 |
| **Phase 2 · 全成本回测** | 资金费/标记价/费率/滑点/下架合约全部计入，报告 Deflated Sharpe | 可复现的回测引擎 | 2～4 周 |
| **Phase 3 · 模拟盘** | 交易所 testnet/demo + 本地 paper trading，对比回测成交价与滑点 | 4～12 周运行日志 | 1～3 个月 |
| **Phase 4 · 小资金实盘** | 确定性护栏 + 交易所侧止损 + dead man's switch + 外部看门狗 | 实盘系统 | 持续 |
| **Phase 5 · AI 层** | LLM 做事件解读、假设生成、因子代码、异常复盘；不直接下单 | 研究助理 agent | 与 Phase 1 并行 |

核心架构原则（来自 TradeTrap、Agent Market Arena、GuardLabs 三方一致）：**模型输出 → 确定性 Guardian 校验（白名单币种、名义上限、频率上限、日回撤上限、数据陈旧检查）→ 才能到交易所**。下单能力放在代码里，不放在 LLM 的工具里。

---

## 1. 基础设施：交易所 API 与开源框架

### 1.1 三大交易所永续 API 现状（2026-09）

| | Binance USDⓈ-M | OKX v5 | Bybit v5 |
|---|---|---|---|
| REST | `fapi.binance.com` | `www.okx.com`（2026-05 新增 `openapi.okx.com`） | `api.bybit.com` |
| 行情 WS | **2026-04-23 起拆为 `/public` `/market` `/private` 三端点**，旧 URL 已停用 | `ws.okx.com:8443/ws/v5/{public,private,business}` | `stream.bybit.com/v5/public/linear` |
| 模拟盘 | `demo-fapi.binance.com`（官方两处文档命名不一致，两个域名都要试） | 同域名 + 请求头 `x-simulated-trading: 1` | Testnet 与 Demo Trading 两套并存，官方建议用 Demo（生产环境内隔离） |
| 限速 | 2400 weight/min/IP；300 单/10s、1200 单/min/账户 | 多数交易端点 60 次/2s **按 instrument 计** | 600 请求/5s/IP（超限封 10 分钟） |
| 2025～26 重大变更 | 条件单迁到 Algo Service（2025-12）；RPI 订单；单连接流数上限 1024 | 撮合机房 2026-07 从香港迁到东京；orderbook 校验改 seqId | Classic 账户 2025-10-31 强制升级 UTA 2.0；2026-08 全合约支持对冲模式 |
| 机房 | AWS 东京（第三方一致，官方未公布） | AWS 东京（官方公告） | AWS 新加坡 apse1-az2/az3（官方 FAQ） |
| 官方历史数据 | **Binance Vision**（`data.binance.vision`）9 类日度 + 月度 fundingRate | `okx.com/historical-data`（tick 2021-09 起、L2 2023-03 起） | `public.bybit.com`（逐笔成交、premium_index） |

要点：
- 2026 年起「东京 VPS」同时覆盖 Binance、OKX、Hyperliquid；新加坡覆盖 Bybit。东京到 Binance 约 5～25ms。
- 延迟只在 1 分钟以下、跨所套利或做市时才重要。小时级以上策略先在任意云跑 dry-run，上实盘再迁东京。
- Co-location 不对个人开放。
- Binance 提供交易所侧 dead man's switch：`POST /fapi/v1/countdownCancelAll`（每 30s 心跳，120s 超时自动撤单）。

### 1.2 CCXT

- 当前 v4.5.78（2026-09-07），周更。**CCXT Pro（WebSocket）已并入免费包**，`import ccxt.pro`。
- 永续符号统一为 `BTC/USDT:USDT`；统一方法 `setLeverage` / `setMarginMode` / `setPositionMode` / `fetchFundingRate` / `watchPositions`。
- 另有 `ccxt-mcp` 可直接把 ccxt 暴露给 LLM agent，但按本报告结论，**不要给 agent 下单权限**。
- 统一层偶尔跟不上交易所变更（`NotSupported`），主力交易所建议保留直连官方 WS 的 adapter。

### 1.3 开源框架对比

| 项目 | Stars | 活跃度 | 永续实盘 | 回测+实盘一体 | 定位 |
|---|---|---|---|---|---|
| **freqtrade** | 54k | 2026.8 月更 | Binance/Bybit/OKX/Gate/Hyperliquid 等（Binance 仅单向模式） | 是，含 hyperopt、dry-run、**FreqAI**（自适应 ML/RL） | 最省事的整体方案 |
| **nautilus_trader** | 29k | v1.231（稳定）；2.0 rc 不建议实盘 | Binance/Bybit/OKX/Hyperliquid/dYdX 等 | 是，「同一策略代码跑回测与实盘」，纳秒级事件驱动 | 执行级验证最强，学习曲线陡 |
| hummingbot | 20k | 2.16.0 月更 | 18 个 perpetual connector | 部分（V2 Controllers） | 做市/执行 |
| jesse | 8.5k | 3.1.3 | Bybit/Binance/Hyperliquid | 回测免费，**实盘需付费插件** | 轻量 |
| OctoBot | 6.6k | 2.1.1 | 仅 isolated | 是 | 新手向 |
| TradingAgents | 105k | v0.4.0 | **否**（Yahoo Finance，输出到模拟交易所） | 回放式回测，声明研究用途 | 多智能体范式参考 |
| ai-hedge-fund | 63k | 活跃 | **否**（美股，明言不下单） | `--backtest` | 教育用途 |

结论：三个 LLM agent 项目都不执行交易，star 数反映的是范式热度不是绩效。真正连 CEX 永续实盘的是 freqtrade、nautilus、hummingbot、jesse、OctoBot。

---

## 2. 需要收集哪些资料

### 2.1 永续合约特有字段及用法

| 字段 | 含义 | 业内用法 | 陷阱 |
|---|---|---|---|
| **资金费率** | 多空互付以锚定现货；Binance 默认 8h，触顶转 1h，连续 16 周期 ≤0.025% 回落 4h（2026-01 起） | >0.10%/8h 视为拥挤；**与 30/90 日均值比较而非固定阈值**；跨所套利 | 强趋势可维持高费率数周；回测按固定 8h 计费会低估成本 |
| **持仓量 OI** | 未平仓名义总量 | OI↑+费率↑ = 方向拥挤；OI↓+极端费率 = 趋势衰竭；OI 峰值与价格峰值背离 = 脆弱性 | 实时接口只留 1 个月历史 |
| **多空比 / 大户多空比** | Binance 定义大户 = 保证金余额前 20% 用户；分持仓比与账户比 | 逆向情绪（>70% 多头偏拥挤） | **无学术实证**，仅社区经验 |
| **主动买卖量** | taker 买/卖成交量 | 「流量倾斜」，与多空比「仓位倾斜」互补 | 同上只留 30 天 |
| **清算数据** | 强平订单流 | 清算簇 = 价格被吸引的流动性 | Binance 每 symbol 每秒只推 1 笔快照，**所有聚合平台的清算量都是系统性低估**；热力图是倒推估计 |
| **标记价/指数价/溢价指数** | 标记价触发强平；溢价指数是资金费率输入 | 回测清算必须用标记价 | Binance Vision 有三套独立 K 线 |
| **DVOL** | Deribit 30 日隐含波动率指数 | 波动率择时，日预期波动 ≈ DVOL/20 | 2021-04 起有数据 |

### 2.2 免费数据源（优先级从高到低）

1. **Binance Vision** `data.binance.vision`，USDⓈ-M 目录：
   - 日度：aggTrades、bookDepth（±5% 档位累计深度，2023-01 起，非完整 L2）、bookTicker、klines、markPriceKlines、indexPriceKlines、premiumIndexKlines、**metrics**（OI、大户多空比、账户多空比、taker 多空比，BTCUSDT 2020-09 起）、trades
   - 月度：fundingRate（只有月度）
   - **没有清算数据集**。现货时间戳 2025-01 起改微秒，期货仍毫秒。
2. **Binance REST `/futures/data/*`**：openInterestHist、globalLongShortAccountRatio、topLongShortPositionRatio、takerlongshortRatio，权重 0，**只保留 30 天** → Phase 0 必须持续采集。`/fapi/v1/fundingRate` 可拉全历史。
3. **Bybit** `public.bybit.com`（逐笔、premium_index）；**OKX** 官网 historical-data（tick、L2、资金费率）。
4. **Deribit DVOL** 公共端点免费；**Hyperliquid** `/info` 免费，S3 归档 requester-pays 且「不保证及时」；**Polymarket** Gamma/CLOB API 免认证。
5. Coinglass 官方定价页**没有免费 API 档**（最低 $29/月，且低档历史只有 6～30 天）。

### 2.3 付费数据源

| 供应商 | 价格 | 适合谁 |
|---|---|---|
| Tardis.dev | $350～6,000/月（官网），tick 级 L2 回放 | 微观结构、滑点回放 |
| CryptoQuant | $29～109/月起有 API | 低预算链上因子 |
| Glassnode | Advanced 约 $49（仅 Light API）；Professional 约 $999 | 机构级链上 |
| Coinglass | $29～699/月 | 跨所聚合衍生品指标 |
| Velo Data | $199/月，含 Hyperliquid | 多所永续横向对比 |
| Laevitas | 按次 $0.001/请求 | 期权 IV、agent 按需调用 |
| Amberdata / Kaiko | 报价制，机构 | — |

对 1～10 万美元个人资金：**Phase 0～3 完全可以只用免费源**。Tardis 只在做执行级滑点研究时才需要。

### 2.4 另类数据

- **链上**：交易所净流入（流入↑偏空）、稳定币供应、巨鲸转账。视为「流量差额」而非全貌。
- **宏观**：BTC 对 CPI 意外的 1 小时 beta 约 -3.2%/百分点，效应集中在 1 小时窗口，4～24 小时不显著（Block Scholes 2026-07）；FOMC 决议本身反应接近基线（已提前定价）。
- **情绪**：LunarCrush $72～90/月，Santiment $49～249/月（低档有 30 天延迟）。LLM 做新闻情绪分类准确率约 87%（单一来源）。
- **2025～26 新类别**：Hyperliquid 链上永续已占全球（含 CEX）OI 约 9%；HIP-4 结果市场（2026-05）与 Polymarket 数据。有人利用 Polymarket 5 分钟 BTC 市场滞后 Binance 30～90 秒做延迟套利（单一案例）。

### 2.5 数据工程要点

- 策略持仓 ≥ 小时级：K 线 + 5 分钟级衍生品指标够用；执行/滑点研究必须逐笔 + L2。
- 存储：BTC/USDT 逐笔 0.5～2 GB/天，订单簿快照 50～200 MB/天，Parquet 压缩省 60～70%。
- 资金费率对齐：Binance 00/08/16 UTC，部分合约 4h，触顶可变 1h；Bybit 各 symbol 周期不同需查 instruments-info；**回测必须按结算时刻记账，禁止用未来费率**。
- 交易所停机：2025-08-29 Binance 全部 USDⓈ-M 合约停摆约 18 分钟（涉及约 900 亿美元 OI）。回测看不到闪崩、停机、无法成交的止损。
- 幸存者偏差：Binance 2025 年 9～12 月多批下架低流动性永续，仅用在线合约回测估计高估年化 5～15%。

---

## 3. 信号识别：什么有实证、什么没有

按证据强度排序。「学术实证」= 同行评审或严肃工作论文；「社区经验」= 行业研究/实盘竞赛。

### 3.1 资金费率 / carry（学术实证最扎实，但已衰减）

- Borri, Liu, Tsyvinski, Wu（2025-10）：cash-and-carry 2020-08～2025-05 年化 Sharpe 6.45，2024 起 4.06，**2025 转负**。资金费率分量平均约 8%/年。
- He, Manela 等（Binance 2020～2024）：永续对无套利基准的偏离**每年收敛约 11%**（套利资本增加）；收益主要来自价格收敛而非资金费本身。
- BIS WP 1087：crypto carry 平均 >10%/年，**高 carry 预测未来崩盘**。
- 用法：极值 → 降杠杆 / 风险预警；跨所费率差套利仍有操作空间但需算清 taker 费。

### 3.2 OI-杠杆堆积与清算级联（当预警用，不当择时用）

- Garcia Seuma（arXiv 2607.27070）：七次 BTC 永续崩盘（LUNA、FTX、2024-08、2024-12、2025-02、2025-04、2025-10）。结论：**没有单一变量跨事件稳定预警**；2025-10 信号在杠杆/订单流而非价格，2024-08 恰好相反；唯一一致的是 taker 订单流方差压缩，但「对逐事件预警太弱」。作者指出分钟级清算微结构数据无商业来源。
- Amberdata：2025-10-10 前 OI 峰值 547 亿美元（年初 +82%），资金费率数周 >15% APR；「2025 年每次大回调前都出现」，但承认择时困难。
- 清算热力图：未找到任何独立回测或学术验证。

### 3.3 时序动量 / 趋势跟踪（唯一未衰减的收益因子）

- Borri 等：两周动量多空周收益 2020 后约 0.021，显著且无明显衰减。
- Fieberg, Liedtke, Zaremba（IRFA 2024）：动量在大市值币存在，但**交易成本高昂、alpha 主要来自空头腿、集中在牛市**。
- AdaptiveTrend（arXiv 2602.11708，预印本）：150+ Binance 永续，6h K 线，样本外 2022～2024 Sharpe 2.41、最大回撤 -12.7%，建模 4bp taker + 滑点 + 8h 资金费；**容量约 500～1000 万美元**。多组件框架有多重比较风险。

### 3.4 跨所领先滞后（社区经验）

- Binance 领先 Hyperliquid 约 700ms、领先 Lighter 约 100ms（2026-02，Hayashi-Yoshida 估计）。Hyperliquid 滞后是结构性的（需两个 HyperBFT 共识周期）。
- 学术：中心化/期货市场领先，高波动期结果混乱。

### 3.5 订单簿失衡 OBI（对个人无经济价值）

- Bieganowski & Ślepaczuk（Binance 永续 1 秒数据 2022～2025-10）：taker 策略在小币显著，**扣费后年化 0.07%～7%**；maker 策略全部不显著，2025-10-10 闪崩中 maker 被「捡走陈旧买单」重创。

### 3.6 没有证据的信号

- **多空比反向**：零学术实证。
- **成交量异常**：学术结论偏负面，量类因子「不是 smart beta」，异常来自微盘币。

### 3.7 信号工程方法论（López de Prado 体系）

- **标签**：三重障碍法（止盈/止损/时间三道栏，取先触发者）代替固定期收益。
- **Meta-labeling**：一级模型给方向（高召回），二级分类器决定是否执行与仓位大小。在一级信号「中等」质量时最有效。
- **交叉验证**：purged K-fold + embargo，剔除与测试标签信息集重叠的训练样本；CPCV 生成多条回测路径。
- **评估**：IC（Spearman）、分位数收益单调性、换手率、**成本后 Sharpe**、**Deflated Sharpe Ratio**（输入试验次数 N，伪 Sharpe 随 √(2 ln N) 增长）。
- **成本阈值**：只有信号强度 > λ × 成本才交易，这一步把换手率削减 >99% 后才让 ML 策略转正。

---

## 4. AI / ML / LLM 的正确位置

### 4.1 传统 ML

| 方法 | 证据 | 结论 |
|---|---|---|
| 梯度提升树（XGBoost/LightGBM/CatBoost） | Bysik & Ślepaczuk（BTC 小时 2018～2026，27 折 walk-forward）：毛收益 +73%，**10bp 成本后 -64%**；加成本阈值后年化 65%、Sharpe 1.09，但 bootstrap 后不显著优于买入持有 | 最稳的基线，但「预测到交易的转换」才是瓶颈 |
| LSTM / Transformer | 多数论文只报 RMSE，很少报成本后 Sharpe；iTransformer 多空毛收益 +182%，成本后 -99% | 不要端到端做方向预测 |
| 强化学习 | 最新论文只在 10% 显著性击败启发式基线；实践共识是把 RL 限制在确定性风险边界内 | 仓位管理辅助，不做主决策 |
| 横截面 ML | XGBoost 优于 OLS，成本后仍盈利，但 alpha 集中在小市值难交易资产 | 个人资金要警惕流动性陷阱 |

### 4.2 LLM 真金实盘证据

| 竞技场 | 设定 | 结果 |
|---|---|---|
| Alpha Arena S1（2025-10-18～11-03） | 6 模型各 $10k，Hyperliquid 永续 | Qwen3 Max ≈+22%、DeepSeek ≈+5%，其余四个 -30%～-63%（Gemini/Grok 名次两来源不一致） |
| Alpha Arena S1.5（2025-11～12） | 美股，四模式含 20 倍杠杆 | Grok 4.20 全胜 +12%，GPT-5.1、Gemini 3 亏损 |
| 2026 年 | nof1 无新赛季（排行榜归档，单一来源） | — |
| TradeRank Arena（2026-01 起 6 季 43 模型） | 模拟资金 | **42.6% 的模型-赛季盈利**（单一来源） |
| Agent Market Arena（WWW 2026） | 4 架构 × 5 骨干模型实盘 | **架构差异远大于骨干模型差异** |
| LiveTradeBench | 21 模型 50 天 | LMArena 等基准**不能预测交易表现** |

TradingAgents 论文的 Sharpe 5.6～8.2 来自 3 个月、3 只美股回测，且模型预训练覆盖测试窗口。

### 4.3 LLM 适合做什么

| 适合 | 不适合 |
|---|---|
| 新闻/事件解读、情绪标注 | 高频 / 小时级方向预测 |
| 研报与公告总结 | 数值时序推理 |
| 策略/因子假设生成 + 受约束 DSL 代码生成（样本外 Sharpe 1.55，alpha 集中小币） | 无护栏自主执行 |
| 异常检测、事后复盘叙事 | 直接持有下单工具 |
| 决策支持 copilot（LATTICE 评测 6 个生产级加密 copilot） | 按信息优势给仓位（LLM 不会） |

### 4.4 安全前提

TradeTrap（arXiv 2512.02261）对 AI-Trader、NOFX、TradingAgents 等做提示注入、假新闻、数据投毒攻击：总收益 7.81% → 0.89%，Sharpe 5.72 → 0.29（数字来自摘要），部分 agent 回撤扩大 2～3 倍。**任何喂给 LLM 的外部文本都要视为攻击面**（OWASP LLM Top 10 2025 第一名）。

---

## 5. 回测陷阱与风控规则

### 5.1 永续回测检查清单

1. 资金费按**实际历史结算时间戳与频率**逐期扣付，检验费率上限切换逻辑（8h → 1h → 4h）
2. 清算/止损触发用**标记价**，成交用最新价或盘口
3. maker/taker 分别计费；Binance 基础 0.02%/0.05%，BNB 抵扣 -10%
4. 滑点模型 ≥ 点差 + 与下单量相关的冲击，薄盘口时段加大
5. 信号在 bar 收盘确认、**下一 bar 开盘执行**（加密收盘到下一开盘差 0.5～2%，往往就是「策略的全部边际」）
6. 数据集含**已下架合约**及下架日期
7. 记录参数搜索次数，报告 **DSR/PSR** 而非原始 Sharpe
8. 实盘前 4～12 周 paper trading，对比回测中的成交价、滑点、延迟

AutoQuant（arXiv 2512.22476）实证：零成本/仅手续费回测相对全成本回测「materially inflate apparent performance」，全成本筛选往往选出回撤更低而非收益更高的参数。

### 5.2 杠杆与清算数学

- 维持保证金按**名义价值分层**，与所选杠杆无关：`MM = 名义 × MMR − 维持金额`。BTCUSDT 第 1 档 MMR 0.40%（第三方）。新账户前 30 天杠杆上限 20x。
- 清算流程：标记价触发 → 撤单 → IOC 部分平仓 → 保险基金 → **ADL**。ADL 排序 = `PnL% × 有效杠杆`，盈利越高、杠杆越高越先被减仓。**对冲策略一侧被 ADL 后另一侧裸露**。
- 2025-10-10：24h 清算约 190 亿美元；永续 OI -43%；Binance 上 USDe 跌至 $0.65，Binance 赔付 2.83 亿美元；Hyperliquid 12 分钟内 ADL 21 亿美元仓位。
- 2026-02-01 周末 ETH -17%，清算 22～26 亿；2026-08-20 空头挤压清算 27.4 亿美元（24h）。
- 交易所调整仓位档位（OKX 2026-02-27）也会触发被动清算，公告要提前处理。

### 5.3 仓位与熔断参数（社区/机构常见区间）

| 规则 | 区间 |
|---|---|
| Kelly | 1/4～1/2 Kelly，无人用全 Kelly |
| 波动率目标 | 年化 10～20%（趋势策略），5～10%（机构级）；20 日回看 |
| 单笔风险 | 1～2% 账户 |
| 日损失熔断 | 3～5%；prop firm 标准日 3～5%、最大回撤 5～10% |
| 周 / 月熔断 | 8～12% / 15～20%（月度触发需全面复盘） |
| 连亏 | 2～3 次即停 |
| 单交易所敞口 | ≤30% |
| LLM agent 示例 | 单笔 ≤$1,000、组合杠杆 ≤2x、每小时 ≤10 笔、每日 ≤50 笔、白名单币种、数据陈旧 >5 分钟不交易、全局 `trading_enabled` 开关 |

**止损必须放在交易所侧**（硬止损 + 追踪止损），而不是只在机器人本地维护。

---

## 6. 系统安全

### 6.1 API key

- Binance 官方 5 条：不共享；按用途拆分（现货/合约/只读各一把）；密钥用环境变量或外部加密文件；**全部启用 IP 白名单**（无白名单且 30 天不活跃自动删除）；**改用 Ed25519**（HMAC 已标记 deprecated）。
- 机器人密钥**永不开提现**。但「仅交易权限」仍可造成损失：攻击者在薄流动性交易对上对敲，把受害账户资金转到自己的对手单（3Commas 2022：44 人损失 1,480 万美元）。

### 6.2 2025～26 年真实案例（与你直接相关）

- **2026-02 OpenClaw/ClawHub 供应链事件**：341～386 个恶意 skill，早期约 17% skill 含恶意载荷；伪装成 Bybit/Polymarket 交易自动化工具，**窃取交易所 API key、钱包私钥、SSH 凭证、浏览器密码**，并分发 AMOS macOS 窃密木马。→ 你在用 Claude Code + 插件生态，安装任何第三方 skill/插件前要审计。
- Binance 2025-10-20 封禁 600+ 使用「未授权第三方工具」的账户。
- Coinbase 用户 2024-12～2025-01 损失 6,500 万美元，主要是社工，部分涉及旧税务软件 API key。

### 6.3 LLM 提示注入防御（OWASP + TradeTrap + GuardLabs）

- 下单能力放在确定性代码，不放在模型工具里
- 不可信内容（新闻、社交、网页）与指令严格隔离并标注
- 模型输出经确定性 Guardian（白名单、名义上限、频率上限）后才到交易所
- 高风险动作人工审批；所有决策审计日志；定期对抗测试

### 6.4 看门狗与 kill switch

- 交易所侧：Binance `countdownCancelAll`（每 30s 调用，countdownTime=120000ms）；Kraken `cancelAllOrdersAfter`；HTX/BitMEX 类似
- 外部心跳看门狗**必须运行在机器人之外的机器上**，「沉默即事故」
- 全局 `trading_enabled` 开关不重新部署即可停新开仓
- 注意：交易所维护期间 countdown 功能本身可能暂停（Binance COIN-M 2026-06-29），需独立预案

### 6.5 运维

- Binance WS 单连接 24h 强制断开；listenKey 60 分钟续期
- 重连：指数退避 + 抖动，**重连后先用 REST 重建挂单/仓位/余额视图再信任流**
- 订单对账：每笔带 clientOrderId（幂等）；显式状态机并持久化；超时后按 clientOrderId 查询而非盲目重发；本地与交易所不一致 → 停止交易 → 对账 → 安全后恢复
- 时钟：-1021 错误根因是主机时钟漂移（VM 暂停/恢复后常见），用 chrony 持续同步，不要一味放大 recvWindow
- 双活：任一时刻只能一个实例下单（带 TTL 的租约锁），防脑裂；此部分个人级成熟方案公开证据薄

---

## 7. 合规现状（客观陈述）

- **中国大陆**：2025-11-28 央行牵头 13 部门重申「坚持对虚拟货币的禁止性政策」；**2026-02-06 央行等八部门「42 号文」**取代 2021 年「9·24 通知」：境内虚拟货币相关业务属非法金融活动；境外交易所向境内主体提供服务属非法；相关民事法律行为无效、损失自担；新增「属人 + 穿透」管辖。律所解读：单纯个人持有仍处灰色地带，但交易环节是监管重点，交易损失不受民事法律保护。
- **香港**：SFC 2026-02-11 框架允许持牌平台向**专业投资者**提供永续合约；零售不可参与；抵押品限法币/稳定币/受监管代币化存款。
- **交易所 KYC**：Binance 2021 年起停止大陆服务；Bybit 服务协议将中国大陆列为完全受限（2026-05 版）；OKX 2021 年起停止大陆手机号注册，2024-05 起不再服务香港居民。
- **自动化交易与 ToS**：交易所普遍允许 API 自动交易（Binance 自己提供机器人），但禁止对敲、spoofing、quote stuffing、front-running 及「不合理加重平台负担」的 API 使用；反复触发速率限制可导致封号。

---

## 8. 推荐技术栈与目录规划

### 8.1 技术栈（Python 为主，社区共识）

| 层 | 选择 | 理由 |
|---|---|---|
| 行情接入 | `ccxt.pro` 起步 + 主力交易所直连官方 WS adapter | WS 为主拿最新完整数据，REST 补断线缺口（Bybit 官方 FAQ 推荐模式） |
| 研究存储 | **Parquet + DuckDB**，按 `exchange/symbol/date` 分区 | 零拷贝 Arrow，单机性能常与 ClickHouse 持平，零运维 |
| 实盘状态 | PostgreSQL（或 TimescaleDB） | 事务 + 订单状态机持久化 |
| 因子研究 | pandas/polars + Alphalens 风格 IC 评估 + 自写三重障碍/purged CV | AFML 方法论 |
| 回测 | vectorbt（大规模扫参）→ nautilus_trader（执行级验证并直接上实盘）；或 freqtrade 一站式 | 三层分工是当前社区主流 |
| 执行 | nautilus 或 freqtrade；自建则 asyncio + ccxt.pro + 自实现幂等/状态机/限速器 | — |
| 风控 | 独立 Guardian 模块（纯代码，模型不可绕过） | TradeTrap / GuardLabs |
| 监控 | Telegram/Discord webhook + Prometheus/Grafana；外部心跳看门狗 | — |
| LLM 层 | Claude API 做事件解读/假设生成/因子代码；输入经注入检测；输出只进研究库不进执行 | — |
| 部署 | 先本地 dry-run；实盘迁 AWS/Vultr 东京（Binance/OKX/Hyperliquid）或新加坡（Bybit） | — |

### 8.2 建议目录结构

```
trader/
├── docs/research/          # 本报告及后续调研
├── collector/              # Phase 0：常驻采集器（funding, OI, L/S ratio, taker vol, liquidations WS, DVOL）
├── data/                   # Parquet 数据湖（gitignore）
├── research/               # notebooks、因子库、标签、purged CV
├── backtest/               # 全成本回测引擎或 nautilus 策略
├── execution/              # 订单状态机、对账、限速、dead man's switch
├── guardian/               # 确定性风控：限额、熔断、白名单、数据陈旧检查
├── agents/                 # LLM 研究助理（只读市场数据 + 产出假设/报告，无下单工具）
├── ops/                    # 看门狗、告警、部署脚本
└── tests/
```

### 8.3 Phase 0 采集器最小规格（本周可做）

每 5 分钟拉取并落 Parquet：`/futures/data/openInterestHist`、`globalLongShortAccountRatio`、`topLongShortPositionRatio`、`topLongShortAccountRatio`、`takerlongshortRatio`（period=5m，全部 USDⓈ-M 交易对）；WS 订阅 `!forceOrder@arr` 记录清算流；每 8h 拉 `/fapi/v1/fundingRate` 与 premiumIndex；每日拉 Deribit DVOL；每日增量下载 Binance Vision 前一日 klines/metrics/bookDepth。同时记录 `exchangeInfo` 快照以追踪上架/下架。

---

## 关键要点

- **先采数据，再谈策略。** 30 天窗口的衍生品指标不采就没了；这是唯一有时间压力的事。
- **把「AI 自动化交易」拆成两件事：AI 做研究，代码做交易。** 真金实盘证据一致表明 LLM 直接下单是负期望。
- **只认成本后、样本外、多重检验校正后的数字。** 10bp 手续费 + 资金费率 + 标记价清算 + 下架合约，缺一个回测就是假的。
- **信号优先级**：趋势/动量（大市值、成本后）> carry/资金费率（当预警与套利，不当择时）> OI-杠杆堆积（降杠杆信号）> 其他。OBI 和多空比不要花时间。
- **护栏是独立模块**：交易所侧止损、dead man's switch、外部看门狗、日损熔断、白名单，模型不可绕过。
- **合规风险是真实成本**，42 号文之后大陆用户损失不受民事法律保护；先评估自己的司法辖区与账户合规性。

---

## 单一来源 / 未能交叉验证的信息（引用时请谨慎）

1. Binance testnet 是 `demo-fapi.binance.com` 还是 `testnet.binancefuture.com`：官方两页面不一致
2. Binance 撮合位于 AWS 东京：Binance 未官方公布，AWS 博客 + 多家第三方一致
3. OKX 迁移后 4ms / 63ms 延迟：仅新闻，官方公告无数字
4. nof1 Alpha Arena 全部细节：官方站多次 429，数据来自二手来源；Gemini/Grok 名次两来源冲突；2026 年是否有新赛季仅 TradeRank 一处
5. TradeTrap 具体数字（7.81%→0.89%、5.72→0.29）：来自摘要
6. Binance Vision metrics 行粒度与 bookDepth 快照间隔：官方未明示
7. Coinglass 是否有免费 API：官方定价页无，第三方称有
8. BTCUSDT 第 1 档 MMR 0.40%、BNB 抵扣 10%：仅第三方
9. Hyperliquid 10·10 ADL 35,000 次 / 20,000 用户：仅 Wu Blockchain / BTCC
10. 资金费率套利论文（ScienceDirect）「六个月 115.9%」：页面 403 无法核实
11. AdaptiveTrend Sharpe 2.41：arXiv 预印本未同行评审
12. Polymarket 滞后 30～90 秒套利：单一个人博客
13. Jesse 实盘插件 $899+：仅一处
14. Bybit / OKX 对大陆受限条款原文：第三方转述
15. 个人级双活/故障转移：缺少针对交易机器人的成熟案例

---

## 精选来源（完整清单见各 agent 原始报告，约 250 条）

**交易所官方**
- Binance 衍生品 changelog https://developers.binance.com/docs/derivatives/change-log
- Binance WS 三端点拆分通知 https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- Binance 资金费率 FAQ https://www.binance.com/en/support/faq/detail/360033525031
- Binance 清算协议 https://www.binance.com/en/support/faq/binance-futures-liquidation-protocols-360033525271
- Binance ADL https://www.binance.com/en/support/faq/what-is-auto-deleveraging-adl-and-how-does-it-work-360033525471
- Binance countdownCancelAll https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders
- Binance API 安全 5 条 https://www.binance.com/en/blog/security/how-to-use-an-api-key-securely-5-tips-from-binance-8638066848800196896
- Binance Vision 期货目录 https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/ ；仓库 https://github.com/binance/binance-public-data
- Binance OI 统计（30 天）https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
- Binance 清算流限制 https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
- OKX API v5 https://www.okx.com/docs-v5/en/ ；东京迁移 https://www.okx.com/en-eu/help/okx-trading-server-migration-announcement-hong-kong-tokyo
- Bybit v5 https://bybit-exchange.github.io/docs/v5/guide ；FAQ（机房、接入模式）https://bybit-exchange.github.io/docs/faq
- Hyperliquid 费率 https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees ；历史数据 https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
- Deribit DVOL https://insights.deribit.com/exchange-updates/dvol-deribit-implied-volatility-index/

**学术论文**
- Borri, Liu, Tsyvinski, Wu, Cryptocurrency as an Investable Asset Class https://arxiv.org/abs/2510.14435
- He, Manela, Ross, von Wachter, Fundamentals of Perpetual Futures https://arxiv.org/html/2212.06888v5
- BIS WP 1087 Crypto carry https://www.bis.org/publ/work1087.pdf
- Garcia Seuma, Early-warning signals across seven crypto-perpetual liquidation cascades https://arxiv.org/html/2607.27070
- Fieberg, Liedtke, Zaremba, Cryptocurrency anomalies and economic constraints https://ideas.repec.org/a/eee/finana/v94y2024ics1057521924001509.html
- Bieganowski & Ślepaczuk, Explainable Patterns in Cryptocurrency Microstructure https://arxiv.org/html/2602.00776v1
- Bysik & Ślepaczuk, ML-Based Bitcoin Trading Under Transaction Costs https://arxiv.org/html/2606.00060
- AdaptiveTrend https://arxiv.org/html/2602.11708v1
- TradeTrap https://arxiv.org/abs/2512.02261 ；AutoRedTrader https://arxiv.org/pdf/2605.09185
- Agent Market Arena https://arxiv.org/abs/2510.11695 ；LiveTradeBench https://arxiv.org/abs/2511.03628 ；LATTICE https://arxiv.org/abs/2604.26235
- From Hypotheses to Factors: Constrained LLM Agents in Crypto https://arxiv.org/html/2604.26747v1
- TradingAgents https://arxiv.org/html/2412.20138v7
- AutoQuant（全成本回测）https://arxiv.org/abs/2512.22476
- ADL 不可能三角（10·10 分析）https://arxiv.org/abs/2512.01112
- Bailey & López de Prado, Deflated Sharpe Ratio https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
- Joubert, Meta-Labeling https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4032018 ；AFML 笔记 https://reasonabledeviations.com/notes/adv_fin_ml/
- Plazuelo Pascual 等，Price Discovery in Cryptocurrency Markets https://arxiv.org/abs/2506.08718

**行业研究 / 事件**
- CoinDesk Research 10·10 清算 https://www.coindesk.com/research/market-spotlight-the-19-billion-liquidation-that-shook-crypto
- Amberdata Leverage & Liquidations https://blog.amberdata.io/leverage-liquidations-the-31b-deleveraging
- Binance 赔付 2.83 亿 https://www.theblock.co/post/374295/binance-pays-283-million-in-compensation-following-fridays-depegs-covering-user-losses
- Hyperliquid ADL https://wublock.substack.com/p/hyperliquid-activates-cross-margin
- 2026-08-20 空头挤压 https://www.coindesk.com/markets/2026/08/20/bearish-crypto-bets-lose-record-usd2-7-billion-as-bitcoin-surges-toward-usd70-000
- Block Scholes CPI 敏感度 https://www.blockscholes.com/institutional-research/is-bitcoin-showing-greater-sensitivity-to-us-cpi-releases-again
- Alpha Arena S1 https://www.iweaver.ai/blog/alpha-arena-ai-trading-season-1-results/ ；https://forklog.com/en/four-out-of-six-ai-models-suffer-losses-in-trading-tournament/
- Flat Circle AI Trading Arenas https://blog.flatcircle.ai/p/ai-trading-arenas
- Binance–Hyperliquid 领先滞后 https://www.weex.com/news/detail/who-is-leading-the-price-discovery-in-the-cryptocurrency-market-measured-delays-on-platforms-like-binance-and-hyperliquid-dloz1e4yzce7uc4wj39b55fa
- Kaiko 流动性集中 https://www.kaiko.com/resources/the-crypto-liquidity-concentration-report
- 八所费率对比 https://crypto.news/maker-and-taker-fees-compared-across-8-crypto-exchanges/
- 交易所机房与延迟 https://arbitron.app/learn/crypto-exchange-server-locations

**安全**
- OWASP LLM01 https://genai.owasp.org/llmrisk/llm01-prompt-injection/
- OpenClaw/ClawHub 恶意 skill https://thehackernews.com/2026/02/researchers-find-341-malicious-clawhub.html ；https://unit42.paloaltonetworks.com/openclaw-ai-supply-chain-risk/
- 3Commas API key 事件 https://3commas.io/blog/december-10-update-on-investigation-api-key-exchange-attacks
- GuardLabs Guardian 架构 https://dev.to/guardlabs_team/your-ai-trading-agent-will-lose-all-your-money-heres-how-to-stop-it-1e15
- Binance 封禁 600+ 账户 https://www.theblock.co/post/375278/binance-bans-more-than-600-accounts-over-unauthorized-third-party-tools

**合规**
- 央行 2025-11-28 会议 https://www.pbc.gov.cn/goutongjiaoliu/113456/113469/5916794/index.html
- 42 号文解读（汉坤）https://www.hankunlaw.com/portal/article/index/cid/8/id/16251.html ；（金杜）https://www.kingandwood.com/cn/zh/insights/latest-thinking/practical-issues-of-virtual-currencies-key-regulatory-points-of-document-no-42-and-its-connection-with-criminal-judicial-disposal-needs.html
- 香港 SFC 永续框架 https://www.charltonslaw.com/sfc-virtual-asset-update-new-guidance-on-va-margin-financing-perpetual-contracts-and-affiliated-market-makers/
- OKX ToS https://www.okx.com/help/terms-of-service

**框架与工具**
- freqtrade https://github.com/freqtrade/freqtrade ；nautilus_trader https://github.com/nautechsystems/nautilus_trader ；hummingbot https://github.com/hummingbot/hummingbot
- CCXT Pro 手册 https://github.com/ccxt/ccxt/wiki/ccxt.pro.manual
- Tardis https://tardis.dev/ ；Coinglass 定价 https://www.coinglass.com/pricing ；Velo https://docs.velo.xyz/api

---

## 方法

4 个并行调研 agent 分别覆盖：① 交易所 API 与开源框架（36 次搜索、45 页深读）；② 数据采集（36 次搜索、28 页深读）；③ 信号与 AI/ML/LLM（31 次搜索、20+ 页深读，含 14 篇论文）；④ 回测、风控、安全、合规（37 次搜索、25 页深读）。优先最近 12 个月资料；所有网页内容仅作数据引用；单一来源信息单独标注。无法抓取（403/404/429）的页面在正文标注为「仅摘要 / 未验证」。
