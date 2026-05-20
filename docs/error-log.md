# Error Log

> When stuck, add an entry here before asking for help or changing approach.
> Format: one H2 per incident, dated, with the three required fields.

---

<!--
## YYYY-MM-DD — Short title

| Field | Detail |
|-------|--------|
| **Symptom** | What went wrong / what you observed |
| **Root cause** | Why it happened |
| **Fix** | What resolved it, or "open" if unresolved |
-->

## 2026-05-20 — clearinghouseState 数值字段类型误判 (positionValue / liquidationPx / returnOnEquity)

| Field | Detail |
|-------|--------|
| **Symptom** | `api_notes.md` 早期版本记录 `positionValue` 为 `float`（非 string），`returnOnEquity` 为 `float`，`liquidationPx` 非 null 时为 `float`。Phase 2 parser fixtures 据此把 `liquidationPx` 写成 JSON number（无引号），与真实 API 类型不符。 |
| **Root cause** | 早期观察来自小样本 smoke test（5–10 个仓位）。这些仓位的 `positionValue` 恰好都是整数边界值（如 `844640.0`），JSON 文件里的 `"844640.0"`（带引号）与 `844640.0`（无引号）视觉上相似，直接看 JSON 形态而非用 `type()` 检查导致误判。`liquidationPx` 同理——小样本里的 float 值被当成 native number 记录。 |
| **Fix** | 用 `scripts/verify_schema_types.py` 对 200 地址 / 862 仓位执行 `type()` 精确检验（2026-05-20）。结论：所有数值字段（`szi`, `entryPx`, `positionValue`, `unrealizedPnl`, `returnOnEquity`, `liquidationPx`, `marginUsed`, `cumFunding.allTime`）一致为 `str`；`leverage.value` 是唯一的 native `int`；`leverage.rawUsd` 是 `str`，仅 isolated 仓位有（~8%）。已修正 `api_notes.md` schema 表格、3 个 test fixtures（`liquidationPx` 改为带引号 string），parser 本身逻辑无误（`float()` 接受 str 输入）。 |
| **Lesson** | 小样本 schema 验证不可靠。数值字段的 JSON 表示（有/无引号）可能因值的形态（整数、小数位数）而混淆视觉判断。必须用 `type()` 而非看 JSON 形态来确认 Python 类型。 |

## 2026-05-20 — Phase 1→2: 429 根因确认 + Phase 2 配置最终决定

| Field | Detail |
|-------|--------|
| **Symptom** | smoke test 21×429 + 对照实验 Config E (1000/15) 17×429，均集中在 60s 窗口前 ~9s；5 个对照实验中 A/B/C/D 全部 0×429 |
| **Root cause** | 双重作用缺一不可：(1) **burst 速率**——concurrency=15 在 ~9s 内消耗完 1000 weight（≈333 weight/s），(2) **HLClient 429 后 sleep 8s 期间其他协程仍消耗 quota**，导致连锁 429（实验观测：17 次 429 中 15/16 间隔 < 1s，且全部发生在窗口第 7.8–8.9s）。单独任一条件均不触发：Config D（700/15）32s 才打满预算，sleep 期间剩余量不足以形成连锁，0×429；Config B（1000/5）burst 速率低，0×429 |
| **Fix** | Phase 2 采用：`PHASE2_RATE_BUDGET=900`（留 100 headroom）+ `PHASE2_SCANNER_CONCURRENCY=8`（单 scanner；4 scanner 叠加 ~32 并发，仍在 D 路径安全区间内）+ `PHASE2_INTER_BATCH_SLEEP_SEC=1.0`（batch 级平滑，补充 QuotaManager 的 per-request 级别控制）|
| **Confidence** | 实验数据直接支持 C（850/8）和 D（700/15）路径稳定；B（1000/5）在单 scanner 稳定但 4-scanner 叠加场景未测试；900/8 选择在已验证的 C 路径上方、有据可查的安全区间内 |

## 2026-05-20 — Phase 1→2 smoke test: 429 burst 原因待查

| Field | Detail |
|-------|--------|
| **Symptom** | smoke test 10 分钟内触发 21 次 429，集中在每个 60s 窗口前 ~8s；QuotaManager 软预算 1000 weight/min 未被突破，但服务端仍返回 429 |
| **Root cause** | 不确定，三个候选假设：(1) QuotaManager 的 rolling 窗口起点与 HL 服务端窗口起点存在 drift，导致客户端认为"刚好在限内"但服务端已超限；(2) HLClient 收到 429 后 sleep 8s 期间，其他 14 个并发协程仍在消耗 weight，最终 60s 窗口末尾 usage 超过服务端阈值；(3) 15 协程 burst 速率约 336 weight/s（15 × 2 / 0.09s RTT），远超配额平均速率 17 weight/s，HL 服务端对子时间窗内的 burst 有额外限流，与总量无关 |
| **Fix** | 暂不修复；Phase 2 collector 启动时先用并发=8、max_per_min=850 观察 429 频次。若假设 1 成立：降 max_per_min 至 850；若假设 3 成立：并发保持 8 不变；假设 2 属 YAGNI，暂不处理（难度高，收益低） |

## 2026-05-20 — Phase 1→2 smoke test: QuotaManager 排队延迟可能极长

| Field | Detail |
|-------|--------|
| **Symptom** | smoke test 中 max latency = 56,481ms，远超 HLClient 设定的 10s timeout；P95 = 532ms 也明显高于正常 RTT |
| **Root cause** | smoke test 脚本的 latency 计时包含了 `QuotaManager.acquire()` 的排队等待时间（队满时最长等待约一整个 60s 窗口）和 429 后的 8s sleep，而非纯网络 RTT（实测约 80–100ms）。这不是 Phase 1 代码的 bug，而是 smoke test 计时设计问题 |
| **Implication** | Phase 2 scanner 不能假设每次查询约 100ms；在高负载下单次 acquire+request 可能阻塞数十秒。需分别记录 "acquire wait time" 和 "network RTT" 两个指标，以便区分配额瓶颈与网络问题 |
| **Fix** | Phase 2 HLClient 在 `_post` 中分别计时 acquire 和 HTTP，各自记录到 log debug |

## 2026-05-20 — liquidationPx=null 根因 FINAL (v3)

| Field | Detail |
|-------|--------|
| **Symptom** | clearinghouseState 返回的 `liquidationPx` 约 42% 为 null（5943 仓位样本；100% 的 null 是 cross-margin，isolated null = 0%） |
| **v1 hypothesis** | REJECTED — "null = cross + small size"：v1 数据显示 cross ≥$10k 仓位仍有 30.7% null，size 不是充分解释 |
| **v2 hypothesis** | REJECTED（方法有误）— "HL 黑盒服务端策略"：v2 的 H_B 测试用了错误简化公式（entry × leverage），没有代入包含 `margin_available` 的真实公式，导致错误判定 |
| **v3 root cause (CONFIRMED)** | HL 官方文档（hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations）给出精确公式：`liq_price = price − side × margin_available / position_size / (1 − l × side)`，其中 cross: `margin_available = account_value − maintenance_margin_required`。当 cross 账户 `account_value` 远超 `position_size` 时，公式算出的 `liq_price` 为负数（long）或天文数字（short），HL 返回 null。这完美解释：isolated 100% 非 null（isolated_margin 有限，不会过剩）；cross 各 size bucket 均有 null（不是 size，是 account_value/position_size 比率）；对冲账户 null 率 2×（净敞口小但账户余额大）；margin_used/size_usd 两组一致（该比率与真正的 account_value/position_size 无关） |
| **Implication (POSITIVE)** | null 仓位本质上是"几乎不会被清算"的仓位，其对清算密度图的贡献接近零。`SKIP_NULL_LIQ_PX=True` 不仅是合理选择，**是最优选择**。真实"可被清算"仓位的覆盖率接近 100%（不是之前以为的 60%）。Phase 4 无需量化偏差，无需在 edge 报告中声明覆盖率限制 |
| **Fix** | `PHASE2_SKIP_NULL_LIQ_PX=True`，记录每 pass 的 null 率即可，不需要补算公式 |

## 2026-05-20 — Phase 1: dirty TTL 远短于 LONGTAIL 扫描间隔的语义陷阱

| Field | Detail |
|-------|--------|
| **Symptom** | `test_dirty_skipped_in_due` 失败——clock 推进 3600s（LONGTAIL interval）后，dirty 地址却出现在 `get_due_addresses` 结果里 |
| **Root cause** | 不是 bug，是设计的语义边界：`DIRTY_SET_TTL_SEC=300s` 远短于 `TIER_SCAN_INTERVAL_SEC[LONGTAIL]=3600s`。推进 3600s 时 dirty 标记（`dirty_until`）早已过期，地址在 due check 之前已经"脱污"，正确地出现在扫描队列里 |
| **Implication** | Phase 2 dirty scanner 必须保证 **5min 内能扫完全部 dirty 队列**。若 dirty 队列长度超出配额能消化的量，必须做截断（`get_dirty_addresses(limit=200)` 已有硬限），并在日志里告警；否则 dirty 信号静默丢失，没有任何错误提示 |

## 2026-05-20 — Phase 0: 500 地址样本密度图近乎空白

| Field | Detail |
|-------|--------|
| **Symptom** | 500 地址样本下 BTC ±5% 内总清算量仅 $0.92M，密度图所有 bin 接近零，无法做视觉判断 |
| **Root cause** | 地址来源是 leaderboard 大户（按账户价值排名），这类账户杠杆低、仓位安全边际大，清算价普遍在 ±5% 之外；500 个地址里只有 29 个仓位的 `liquidationPx` 落在范围内 |
| **Fix** | 改用 WebSocket trades 流采集"活跃交易者"地址（实际成交 = 高杠杆概率高）；2033 地址样本下 BTC ±5% 内清算量升至 $13.1M，148 个仓位落在范围内，密度图出现可见结构 |
