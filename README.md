# trader

AI 辅助的加密货币永续合约自动化交易系统（建设中）。

## 当前状态

- 2026-09-12：完成深度调研，见 [docs/research/ai-perp-trading-research-2026-09-12.md](docs/research/ai-perp-trading-research-2026-09-12.md)

## 核心原则（来自调研结论）

1. AI/LLM 放在研究层（事件解读、假设生成、因子代码），执行必须是确定性代码。
2. 数据从第一天开始自己采：交易所衍生品指标接口只保留 30 天。
3. 回测必须计入资金费率（按实际结算时刻）、标记价触发清算、maker/taker 费率、滑点、已下架合约。
4. 风控是独立模块：交易所侧止损、dead-man switch、外部看门狗、日损熔断、白名单，模型不可绕过。

## 路线图

| 阶段 | 内容 |
|---|---|
| Phase 0 | 常驻数据采集器（funding / OI / 多空比 / taker 买卖量 / 清算流 / DVOL）落 Parquet + DuckDB |
| Phase 1 | 因子库、三重障碍标签、purged walk-forward、IC 评估 |
| Phase 2 | 全成本回测，报告 Deflated Sharpe |
| Phase 3 | 交易所模拟盘 + paper trading |
| Phase 4 | 小资金实盘 + 确定性护栏 |
| Phase 5 | LLM 研究助理（无下单工具） |

## 免责声明

本仓库仅为个人研究与工程实践，不构成任何投资建议。加密货币衍生品交易风险极高，请自行评估所在司法辖区的合规要求。
