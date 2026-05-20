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
