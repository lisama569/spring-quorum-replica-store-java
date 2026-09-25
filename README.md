# 单进程多副本法定人数键值存储（Spring Boot）

在**同一个 JVM 进程**内模拟一个 3 副本（默认 `n1,n2,n3`）的 Raft 键值集群，
支持带任期（term）与提交位置（commitIndex）的多数派复制、主节点变更、
故障恢复与受控故障注入。所有节点间消息都经过一个**确定性消息网络**，
通过“消息泵”手动推进，因此分区、乱序、重复、暂停等场景可以**重复验证**。

> 仅依赖 Spring Boot Web/Test 与 SLF4J，不依赖任何外部服务或真实凭据。

## 1. 一致性语义

| 语义 | 实现 |
| --- | --- |
| 多数派提交 | 写追加到领导日志后复制；**从 commitIndex+1 起连续**达到多数派 matchIndex、且包含本任期条目时才提交（标准 Raft 提交规则） |
| 读已提交 / 不脏读 | 读必须经**多数派屏障**：领导向全部跟随者发心跳，收到多数派成功应答后才读本地已应用状态机 |
| 未达多数派 | 在有限空闲预算内无法形成多数派的写返回 `QUORUM_UNAVAILABLE`；领导截断自己未提交的尾部条目并登记 `abortedWrites`，**恢复后也不会事后补提交** |
| 陈旧任期 | 请求带 `expectedTerm` 且不等于领导当前任期 → `STALE_TERM`；更高任期的 RPC 会让旧主退回跟随者 |
| 日志冲突 | AppendEntries 做 prevLogIndex/prevLogTerm 一致性检查，冲突按 conflictTerm/conflictIndex 快速回退并截断未提交尾部；**提交点 commitIndex 单调不减、绝不回滚** |
| 旧主脑裂 | 分区期少数派旧主可能仍是 LEADER，但其写无法多数派；客户端选择**最高任期**领导；分区恢复后旧主发现更高任期即退位并补齐日志 |
| 请求幂等 | 同一 `requestId` 已提交 → 幂等返回成功且不产生新日志；同 id 不同负载 → `DUPLICATE_MISMATCH` |
| CAS 条件写 | 携带 `expectedValue` 时，仅当该 key 当前已提交值相等才接受，否则 `CAS_CONFLICT` |

提交成功的写满足：随后经多数派屏障的读一定可见；所有节点重放已提交日志得到相同状态机。

## 2. 可区分的拒绝原因（RejectReason）

| 原因 | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `NOT_LEADER` | 503 | 目标节点不是领导（或已退位） |
| `NO_LEADER` | 503 | 当前没有可用领导且选不出多数派 |
| `STALE_TERM` | 409 | 请求任期落后于当前任期 |
| `QUORUM_UNAVAILABLE` | 503 | 预算内无法形成多数派（含被判定失败的同一 requestId 重试） |
| `LOG_CONFLICT` | 409 | 与已提交/现有历史冲突 |
| `DUPLICATE_MISMATCH` | 409 | requestId 复用但 key/value 不同 |
| `CAS_CONFLICT` | 409 | 条件写期望值不匹配 |
| `NODE_UNAVAILABLE` | 503 | 节点停机/暂停 |
| `KEY_NOT_FOUND` | 404 | 多数派读屏障通过，但键不存在 |

每个写/读判定都打印结构化日志：`requestId / term / commitIndex / basis(判定依据)`。

## 3. 可观测字段

`GET /api/status` 返回 `leaderId` 与每个节点的 `NodeSnapshot`：

- `nodeId, role(FOLLOWER/CANDIDATE/LEADER), term/currentTerm, votedFor`
- `commitIndex`（已提交位置）、`lastLogIndex, lastLogTerm, appliedIndex`
- `paused, alive`
- `committedData`：已应用的键值状态机
- `nextIndex / matchIndex`（仅领导有值）：对各跟随者的同步位置
- `logSummary`：`index:term:requestId` 日志摘要，用于核对重复/回滚

## 4. 故障注入方式（均为 HTTP API）

| 操作 | 端点 |
| --- | --- |
| 暂停节点（消息滞留，不投递） | `POST /api/admin/nodes/{id}/pause` |
| 恢复节点（可选丢弃滞留消息） | `POST /api/admin/nodes/{id}/resume?dropPending=false` |
| 节点重启（重放持久化日志后追平领导） | `POST /api/admin/nodes/{id}/restart` |
| 主节点故障（杀掉当前主并在存活侧重新选举） | `POST /api/admin/leader/fail` |
| 手动发起选举 | `POST /api/admin/election/{id}` |
| 制造双向分区 | `POST /api/admin/partition` body `{"sideA":["n1"],"sideB":["n2","n3"]}` |
| 恢复分区并收敛 | `POST /api/admin/partition/heal` |
| 按概率丢消息 | `POST /api/admin/network/drop?from=n1&to=n2&probability=0.5` |
| 链路乱序（LIFO） | `POST /api/admin/network/order?from=n1&to=n3&mode=LIFO` |
| 消息重复注入 | `POST /api/admin/network/duplicate?from=n1&to=n2&extraCopies=2` |

网络模型说明：分区期间跨链路消息**滞留而非删除**，恢复后由 Raft 任期/日志
一致性检查拒绝陈旧消息；停机（dead）节点的在途消息立即清空，重启从持久化
日志与提交位恢复；暂停（paused）节点消息保留，恢复后可选择丢弃。

## 5. 本地运行与验证

```bash
# 运行全部测试（24 个：核心集群单测 + MockMvc 端到端）
mvn test

# 启动服务（端口 8080）
mvn spring-boot:run
```

手动验证脚本：

```bash
# 先选出 leader
curl -X POST localhost:8080/api/admin/election/n1

# 写（多数派提交）
curl -X POST localhost:8080/api/kv/color -H 'Content-Type: application/json' \
  -d '{"requestId":"w-1","value":"blue"}'

# 读（多数派屏障）
curl localhost:8080/api/kv/color

# 观测
curl localhost:8080/api/status | jq .

# 主节点故障后，旧数据仍可读
curl -X POST localhost:8080/api/admin/leader/fail
curl localhost:8080/api/kv/color
```

测试覆盖（见 `src/test/.../cluster` 与 `.../web`）：

- 多数派成功 / 单跟随者宕机仍可写 / 不足多数派写失败且事后不可见
- 陈旧任期、CAS 冲突、重复请求幂等与 requestId 复用冲突
- 主节点故障切换、任期递增、已提交数据不丢
- 少数派旧主写失败、分区恢复后日志与状态机完全收敛、历史读不回滚
- 节点停机重启：重放已提交日志并追随领导补齐
- 消息重复投递（日志中 requestId 只出现一次）、LIFO 乱序
- 暂停/恢复/存活观测字段；HTTP 端到端错误码区分

## 6. 仅在“单进程模拟”范围内成立的保证

以下保证**只在本工程的确定性单进程模型下成立**，不可外推到真实分布式系统：

1. **无真实网络与时钟**：消息靠内存队列 + 单线程泵按固定顺序投递，
   没有真实延迟、带宽、并发与物理时钟；“超时/预算”是泵的轮次数。
2. **持久化是内存模拟**：`currentTerm/votedFor/log/commitIndex` 用对象字段表示，
   进程真正退出后数据不保留；“重启”只是在同一 JVM 内清空易失状态并重放。
3. **故障模型受限**：仅支持暂停、停机、分区、概率丢弃、LIFO 乱序与固定份数重复，
   不模拟拜占庭行为、磁盘损坏、网络长期半开、真实 RPC 超时重试风暴等。
4. **客户端门面是同步的**：`write/read` 内部直接驱动消息泵直到得到响应，
   不存在真实多客户端并发；HTTP 层用 `synchronized` 串行化所有请求。
5. **确定性随机种子**（`new Random(42)`）：概率丢弃在相同调用序列下结果稳定，
   但这不是真实随机分布的统计保证。

真实系统中还需要持久化 WAL、租选/会话、真实 RPC 超时与重试、成员变更、
快照与日志压缩、并发客户端等机制；本项目聚焦于让 Raft 的提交、任期与
恢复结论在本地**可重复、可观测、可解释**。
