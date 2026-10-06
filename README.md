# Spring Quorum Replica Store

单进程内模拟一个 **3 节点（可配置）Raft 风格多副本键值存储** 的 Spring Boot 服务。
所有副本运行在同一 JVM 内，通过内存消息总线通信；支持带任期（term）与提交位置
（commitIndex）的复制、法定人数（quorum）读写、主节点变更、故障注入与运行态观测。

> 这是一个用于**演示与可重复验证一致性结论**的模拟系统，不依赖外部服务或真实凭据。

## 1. 架构概览

```
HTTP 客户端
   │
   ▼
ClusterController ── 读写 / 观测 / 故障注入 REST 接口
   │
   ▼
Cluster ──────────── 集群门面、全局串行锁、时钟 tick 调度
   │           ▲
   ▼           │
ReplicaNode ×N │     日志、任期、角色、状态机；处理选举/复制消息
   │           │
   ▼           │
MessageBus ─────┘     同步投递 + 暂停/分区/重复/乱序(hold)注入
```

- **全局串行执行**：`Cluster` 用一把锁串行化所有 tick 与客户端请求；消息为
  **同步投递**（发送方调用栈内即完成接收方处理），因此不存在真实并发竞争。
- **持久状态 vs 易失状态**（崩溃语义）：
  - 持久（crash/restart 保留）：`currentTerm`、`votedFor`、复制日志、
    `commitIndex`、`lastApplied`、键值状态机、已应用请求结果表。
  - 易失（crash 后丢失）：角色、选举计时器、leader 的 `nextIndex/matchIndex`。
- 主节点上任时写入一条当前任期的 **no-op 日志**，用于按 Raft 规则安全推进
  对旧任期日志的提交。

## 2. 一致性语义

### 写路径（quorum 复制）
1. 只有 `LEADER` 接受写；非主节点返回 `NOT_LEADER`。
2. 追加日志后向所有从节点发送 `AppendEntries`，**同步**统计复制成功数。
3. 只有属于**当前任期**且被 **⌈N/2+1⌉** 节点复制的条目，才会推进 `commitIndex`
   并应用到状态机。这避免提交一个可能已被新主覆盖的旧任期条目。
4. 达不到多数派：前台返回 `NO_QUORUM`，该条目**不会**被应用、不可见；
   恢复后可能被新主的日志截断覆盖。
5. 幂等：客户端携带 `requestId`；已提交的同 `requestId` 重复请求直接返回首次
   结果（`reason=DUPLICATE_REQUEST`），**不追加新日志、不推进提交位置**。
   重复/乱序的复制消息由“位置+任期一致性检查”天然去重。

### 键版本与条件写入（CAS）
- 每个键维护一个**单调递增的版本号**（从 1 开始）：每次成功写入（含删除）
  版本 +1；删除后版本以“墓碑”形式保留，重新创建同一键时版本**继续递增、
  绝不回退**。
- 读取结果同时携带 `found`（键是否存在）、`value` 与 `version`；
  键不存在时读取本身仍成功（多数派已确认），以 `found=false, version=0` 区分，
  不与 `NO_QUORUM` 等失败混淆。
- 写/删请求可携带 `expectedVersion`：
  - 与当前版本一致 → 生效并返回新版本；
  - 不一致 → 拒绝并返回 `VERSION_MISMATCH`，同时带回**当前实际版本**供上层
    重试，键值与版本均不被改动；
  - 键不存在但指定了非 0 的 `expectedVersion` → 返回 `KEY_NOT_FOUND`
    （`expectedVersion=0` 表示“期望键不存在”，可用于 create-if-absent）；
  - 删除不存在的键 → 同样返回 `KEY_NOT_FOUND`。
- 条件校验在**提交应用时**对最新已提交状态求值，结果随日志复制到各副本，
  因此所有副本对同一请求得到相同的判定。
- 删除（`POST /api/cluster/delete`）与写入走**同一条受版本约束的路径**，
  同样支持 `expectedVersion` 与 `clientTerm`。

### 客户端任期与重复请求
- 写/删请求可携带 `clientTerm`（上层网关缓存的任期号）：
  - `clientTerm < 当前任期` → 直接拒绝，返回 `STALE_TERM`，不产生任何状态变更；
  - 未携带（老请求）→ 不校验，按原行为处理。
- 同一 `requestId` 重复提交时按**请求指纹**（操作类型、键、值、期望版本、
  客户端任期）严格判定：
  - 内容**完全一致** → 视为重试，重放首次结果（`DUPLICATE_REQUEST`），
    不产生新状态变更；
  - 内容**不一致** → 明确拒绝，返回 `REQUEST_CONFLICT`，不改动任何键。

### 读路径（read-index）
主节点读前先发起一轮心跳并统计当轮成功响应：连同自己达到多数派才服务读，
保证不会由一个被分区的旧主返回陈旧数据。否则返回 `NO_QUORUM`。

### 选举与冲突
- 候选人日志必须至少和投票人一样新（比较最后日志任期/位置）才能获得投票。
- `AppendEntries` 的 `prevLogIndex/prevLogTerm` 一致性检查失败 →
  返回 `LOG_CONFLICT:*`，主节点回退 `nextIndex` 后补传完整后缀，保证
  “与已提交历史冲突”的请求被拒绝且最终收敛到同一条日志。
- 收到更高任期消息自动降级为 follower；旧主在分区恢复后不能覆盖新历史。

### 可区分的拒绝原因 `RejectionReason`
| reason | 含义 |
| --- | --- |
| `NONE` | 成功 |
| `NOT_LEADER` | 写入/读取的节点不是主节点 |
| `STALE_TERM` | 请求任期落后于当前任期 |
| `NO_QUORUM` | 无法形成多数派（复制提交失败 / read-index 确认失败 / 无主） |
| `LOG_CONFLICT` | 与已提交（或已存在）日志历史冲突 |
| `NODE_UNAVAILABLE` | 节点已崩溃，不可服务 |
| `DUPLICATE_REQUEST` | 重复请求（内容一致），回放首次结果，无新状态变更 |
| `VERSION_MISMATCH` | 条件写入的期望版本与当前实际版本不一致，键值未改动（结果带实际版本） |
| `KEY_NOT_FOUND` | 键不存在（对不存在的键指定已存在版本 / 删除不存在的键） |
| `REQUEST_CONFLICT` | 同一 requestId 重复提交但内容不一致，已拒绝，无任何状态变更 |

## 3. 可观测字段

`GET /api/cluster/status` 返回每个副本的快照：

| 字段 | 含义 |
| --- | --- |
| `nodeId` | 节点标识（n1..nN） |
| `role` | `FOLLOWER` / `CANDIDATE` / `LEADER` |
| `currentTerm` | 当前任期 |
| `commitIndex` | 已提交日志位置 |
| `lastApplied` | 已应用到状态机的位置（正常时等于 commitIndex） |
| `logSize` | 本地日志长度 |
| `paused` | 是否被故障注入暂停（消息层隔离） |
| `alive` | 是否存活（false=已 crash 尚未 restart） |

`GET /api/cluster/replication` 返回**主节点视角的复制进度**（无主时
`leaderId="none"`、`followers=[]`）：

| 字段 | 含义 |
| --- | --- |
| `leaderId` / `term` | 主节点标识与当前任期 |
| `leaderLastLogIndex` | 主节点日志末端位置 |
| `commitIndex` | 主节点已提交位置 |
| `followers[].nodeId` | 从副本标识 |
| `followers[].sentIndex` | 主节点已尝试发送到的日志位置（nextIndex-1） |
| `followers[].matchIndex` | 从副本已确认复制的日志位置 |
| `followers[].lag` | 落后主节点日志末端的条目数 |
| `followers[].state` | `IN_SYNC` 已追平 / `CATCHING_UP` 存活可达但落后（如崩溃重启后正在追赶）/ `BLOCKED` 被暂停或分区导致落后 / `DOWN` 已崩溃未重启 |

节点恢复（resume/restart）后，心跳会把缺失的日志后缀补传给落后副本，
进度最终回到 `IN_SYNC` 且各副本 `commitIndex` 一致；已确认过的读取结果
（值与版本）不会被回滚。

节点处理每个关键判定时都会打印日志，包含 **node、event、requestId、term、
commitIndex、version 与判定依据（reason）**，例如：

```
node=n1 event=write-committed requestId=m-1 term=1 commitIndex=2 version=1 reason=NONE
node=n2 event=append-rejected term=2 leader=n1 reason=STALE_TERM
node=n1 event=read-rejected requestId=r term=1 commitIndex=2 reason=NO_QUORUM
node=n1 event=applied requestId=cw-4 term=1 commitIndex=3 key=c2 op=PUT version=1 reason=VERSION_MISMATCH
```

## 4. REST 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/cluster/write` | body `{"key","value","requestId","expectedVersion"?,"clientTerm"?}`；成功 200，失败 409 |
| `POST /api/cluster/delete` | body `{"key","requestId","expectedVersion"?,"clientTerm"?}`；与写入同一受版本约束路径 |
| `GET  /api/cluster/read?key=&requestId=` | 法定人数确认读，返回 `found/value/version` |
| `GET  /api/cluster/status` | 全部副本状态 |
| `GET  /api/cluster/replication` | 主节点视角的各从副本复制进度 |
| `GET  /api/cluster/leader` | 当前可达多数派的主 |
| `POST /api/cluster/fault/pause/{id}` | 暂停节点（丢弃其收发消息） |
| `POST /api/cluster/fault/resume/{id}` | 恢复暂停 |
| `POST /api/cluster/fault/crash/{id}` | 崩溃（保留持久状态） |
| `POST /api/cluster/fault/restart/{id}` | 重启并重新加入 |
| `POST /api/cluster/fault/partition` | body `{"groupA":[...],"groupB":[...]}` |
| `POST /api/cluster/fault/heal` | 解除分区 |

## 5. 本地运行与验证

```bash
# 运行全部测试（确定性：测试内手动驱动时钟，不依赖真实定时器）
mvn test

# 打包并启动（固定端口 8080，可用 -Dcluster.nodes=5 调整节点数）
mvn -DskipTests package
java -jar target/spring-quorum-replica-store-java-1.0-SNAPSHOT.jar

# 基本读写
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k1","value":"v1","requestId":"req-1"}'
curl -s 'localhost:8080/api/cluster/read?key=k1&requestId=read-1'
curl -s localhost:8080/api/cluster/status

# 条件写入（CAS）：版本对不上会返回 409 VERSION_MISMATCH 并带回实际版本
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k1","value":"v2","requestId":"req-2","expectedVersion":1}'
# 携带网关缓存任期：落后于当前任期时返回 409 STALE_TERM
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k1","value":"v3","requestId":"req-3","clientTerm":1}'
# 受版本约束的删除
curl -s -X POST localhost:8080/api/cluster/delete -H 'Content-Type: application/json' \
  -d '{"key":"k1","requestId":"req-4","expectedVersion":2}'
# 主节点视角的复制进度
curl -s localhost:8080/api/cluster/replication
```

### 兼容性

- 老的写请求（只带 `key/value/requestId`，不带 `expectedVersion/clientTerm`）
  语义不变：无条件写入、不校验任期，成功结果同样会带上键的新版本。
- 老的读请求不变；响应新增 `found/version` 字段，键不存在时仍为 200，
  以 `found=false` 区分。
- 同 `requestId` 重试只有内容**完全一致**才会被当作重放；内容不一致会收到
  `409 REQUEST_CONFLICT`（此前版本未定义该行为）。

### 手工故障演练
```bash
# 隔离主节点（少数派写应返回 409 NO_QUORUM），多数派侧会选出新主
curl -s -X POST localhost:8080/api/cluster/fault/partition \
  -H 'Content-Type: application/json' -d '{"groupA":["n1"],"groupB":["n2","n3"]}'
sleep 2
curl -s localhost:8080/api/cluster/leader
# 恢复后所有节点提交位置一致，旧主降级为 follower
curl -s -X POST localhost:8080/api/cluster/fault/heal
```

### 测试覆盖
| 测试类 | 覆盖场景 |
| --- | --- |
| `QuorumWriteTest` | 全票/多数派写成功、无多数派写失败、非主拒绝、过期任期拒绝、读多数派确认 |
| `LeaderFailoverTest` | 主崩溃重选后已提交数据不丢、旧主恢复后收敛且不能覆盖 |
| `MessageFaultsTest` | 消息 100% 重复无重复状态、重复客户端请求幂等、乱序空洞 → LOG_CONFLICT → 收敛 |
| `RestartRecoveryTest` | 从节点崩溃错过提交后追平；跨多任期主重启，全部已提交数据保留可读 |
| `PartitionHealTest` | 少数派不可提交/读、多数派继续服务、heal 后一致且读不回滚；短时不可用恢复追平 |
| `ConditionalWriteTest` | 条件写命中/不命中（带回实际版本）、键不存在可区分、版本单调不倒退、删除走同一受版本约束路径 |
| `ClientTermAndDedupTest` | 落后 clientTerm 被拒（STALE_TERM）、故障切换后缓存任期失效、同 requestId 内容不一致被拒（REQUEST_CONFLICT）、一致重试重放、老请求兼容 |
| `ReplicationProgressTest` | 复制进度字段可见、暂停=BLOCKED 与崩溃重启=CATCHING_UP 可区分、恢复后追平且与已提交内容一致、已确认读取不回滚 |
| `ClusterApiSmokeTest` | 真实 Spring 容器下端到端 HTTP 读写/观测/暂停/幂等 |

## 6. 仅在单进程模拟范围内成立的保证与限制

- **无真实网络**：投递是方法调用，默认同步且“最终可达”；延迟、丢包、乱序、
  重复只在通过显式故障注入（pause/partition/hold/duplicateRate）时发生。
- **崩溃模型是模拟的**：`crash()` 清空易失字段、对象仍在内存中；“持久状态”
  只是不会被该方法清除，并非真实磁盘 fsync，进程真正退出后数据不保留。
- **无拜占庭故障**：节点严格遵守协议，不伪造消息。
- **时钟由 tick 驱动**：单元测试手动 tick 保证确定性；Web 运行态使用固定 50ms
  定时器。不模拟时钟漂移，只模拟随机化选举超时。
- **单串行化点**：一致性依赖 `Cluster` 全局锁与同步投递；这不是一个可水平扩展
  的生产实现，没有真实 RPC、持久化存储、日志压缩与成员变更。
- 因此本文档所述“已提交不丢、未达多数派不成功、旧主不能覆盖、分区恢复一致、
  键版本单调、条件写判定一致、重复请求严格去重、复制进度最终追平”等结论，
  **仅在上述单进程、同步投递、非拜占庭模拟模型内可重复验证**。
