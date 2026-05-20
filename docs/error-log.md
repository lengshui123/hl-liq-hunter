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

## 2026-05-20 — Phase 1→2 smoke test: liquidationPx=null 比例 42% 语义未确认

| Field | Detail |
|-------|--------|
| **Symptom** | smoke test 中 42% 的仓位 `liquidationPx` 字段为 null；最初推断为"cross margin 没单仓位清算价" |
| **Root cause** | 推断不成立——实测中 cross margin 占 94%，如果 cross margin 全部 null，比例应远高于 42%；实际原因未知，候选：(a) 仓位规模过小、保证金极充足，清算价在可见价格范围外；(b) HL 服务端对某些仓位类型不计算清算价；(c) cross margin 部分仓位有清算价而其他没有，取决于账户整体保证金率。HL 官方文档未明确说明 |
| **Implication** | 清算密度图可能少计 42% 的市场仓位；若这些仓位实际有清算价而我们未使用，会低估密度。但由于语义不明，无法安全地用 entry × leverage 公式回填 |
| **Fix** | Phase 2 暂时跳过 `liquidationPx=null` 的仓位，在日志中记录 null 比例；Phase 4 决定是否实现 fallback 计算。禁止在语义未确认前使用公式回填 |

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
