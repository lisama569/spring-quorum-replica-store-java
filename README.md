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
    `commitIndex`、`lastApplied`、键值状态机（含每键版本与删除墓碑）、
    已应用请求结果表（含内容指纹）。
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

### 键版本与条件写入
- 每个键维护一个**只增不减**的版本号：首次写入为 1，之后每次成功写入
  （包括删除）都 +1。删除后保留**墓碑版本**，键被重建时继续递增而不是回到 1。
- 读取成功时同时返回 `found`（键是否存在）与 `version`（当前版本）；
  键不存在时 `success=true, found=false, version=0`，与 `NO_QUORUM` 等失败可区分。
- 写/删请求可携带 `expectedVersion`（期望的当前版本）：
  - 与当前版本一致才生效，成功响应返回新版本 `version`；
  - 不一致时拒绝并返回 `currentVersion`（当前实际版本）供上层重试，
    **不对键值做任何改动**：键存在时 `reason=VERSION_MISMATCH`，
    键不存在时 `reason=KEY_NOT_FOUND`（当前版本为 0 或墓碑版本）；
  - `expectedVersion=0` 表示“期望键不存在”，可用于 create-if-absent；
  - 删除要求键当前存在，对不存在的键删除一律 `KEY_NOT_FOUND`。
- 删除（`POST /api/cluster/delete`）与写入共用同一条受版本约束的复制/提交路径。
- 不带 `expectedVersion` 的老请求保持原有**无条件写入**语义。

### 客户端任期与重复请求
- 写/删请求可携带 `clientTerm`（上层网关缓存的任期号）。若 `clientTerm` 小于
  当前任期，直接拒绝并返回 `reason=STALE_TERM`，区别于版本冲突与多数派失败，
  且不产生任何状态变更。不带 `clientTerm` 的老请求不做该检查。
- 同一 `requestId` 重复提交时逐字段比对内容（key、value、是否删除、期望版本）：
  - **完全一致**：视为重试，直接回放首次结果（`reason=DUPLICATE_REQUEST`，
    含首次写入的版本），不产生新状态；
  - **任何字段不一致**：拒绝并返回 `reason=REQUEST_CONFLICT`，不改动任何键。

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
| `VERSION_MISMATCH` | 条件写入的期望版本与当前版本不一致（响应带 `currentVersion`） |
| `KEY_NOT_FOUND` | 键不存在：条件写前置了已存在版本 / 删除不存在的键 |
| `REQUEST_CONFLICT` | 同一 `requestId` 再次提交但内容与首次不一致 |

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

`GET /api/cluster/replication` 返回**主节点视角**的复制进度
（无多数派主时 `leaderId=null`）：

| 字段 | 含义 |
| --- | --- |
| `leaderId` / `term` / `commitIndex` / `lastLogIndex` | 主节点标识、任期、已提交位置、日志末端位置 |
| `followers[].nodeId` | 从副本标识 |
| `followers[].nextIndex` | 主节点下一条要发往该副本的日志位置 |
| `followers[].matchIndex` | 该副本已确认复制的日志位置 |
| `followers[].lag` | 落后条目数（`lastLogIndex - matchIndex`） |
| `followers[].state` | `IN_SYNC` 已追平；`CATCHING_UP` 可达但落后（如崩溃重启后追赶中）；`BLOCKED` 因暂停/分区不可达而落后；`DOWN` 已崩溃未重启 |

节点处理每个关键判定时都会打印日志，包含 **node、event、requestId、term、
commitIndex、version 与判定依据（reason）**，例如：

```
node=n1 event=write-committed requestId=m-1 term=1 commitIndex=2 key=k1 version=3 delete=false
node=n1 event=write-rejected requestId=m-2 term=1 key=k1 expectedVersion=5 currentVersion=3 reason=VERSION_MISMATCH
node=n1 event=write-rejected requestId=m-3 clientTerm=1 term=2 reason=STALE_TERM
node=n2 event=append-rejected term=2 leader=n1 reason=STALE_TERM
node=n1 event=read-rejected requestId=r term=1 commitIndex=2 reason=NO_QUORUM
```

## 4. REST 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/cluster/write` | body `{"key","value","requestId",?"expectedVersion",?"clientTerm"}`；成功 200，失败 409 |
| `POST /api/cluster/delete` | body `{"key","requestId",?"expectedVersion",?"clientTerm"}`；与写入同一受版本约束路径 |
| `GET  /api/cluster/read?key=&requestId=` | 法定人数确认读；响应含 `found` 与 `version` |
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
```

### 版本与条件写入演练
```bash
# create-if-absent：expectedVersion=0 表示“期望键不存在”，成功返回 version=1
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k2","value":"v1","requestId":"cas-1","expectedVersion":0}'
# 版本匹配的条件写：成功返回 version=2；把 1 换成其他数字将返回 409 VERSION_MISMATCH
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k2","value":"v2","requestId":"cas-2","expectedVersion":1}'
# 读取同时返回 found 与 version
curl -s 'localhost:8080/api/cluster/read?key=k2&requestId=cas-read'
# 带版本约束的删除；对不存在的键删除返回 409 KEY_NOT_FOUND
curl -s -X POST localhost:8080/api/cluster/delete -H 'Content-Type: application/json' \
  -d '{"key":"k2","requestId":"cas-del","expectedVersion":2}'
# 网关式重试：clientTerm 落后于当前任期时返回 409 STALE_TERM
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k3","value":"v","requestId":"ct-1","clientTerm":0}'
# 同一 requestId 提交不同内容返回 409 REQUEST_CONFLICT
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"k2","value":"other","requestId":"cas-1","expectedVersion":0}'
```

### 复制进度观测演练
```bash
# 暂停一个从副本后写几条，观察该副本 state=BLOCKED 且 lag 增长
curl -s -X POST localhost:8080/api/cluster/fault/pause/n3
curl -s -X POST localhost:8080/api/cluster/write -H 'Content-Type: application/json' \
  -d '{"key":"r1","value":"v","requestId":"rp-1"}'
curl -s localhost:8080/api/cluster/replication
# 恢复后心跳驱动追赶，state 回到 IN_SYNC、lag 归 0
curl -s -X POST localhost:8080/api/cluster/fault/resume/n3
sleep 1 && curl -s localhost:8080/api/cluster/replication
# 崩溃中的副本显示 DOWN；restart 后、追平前显示 CATCHING_UP
curl -s -X POST localhost:8080/api/cluster/fault/crash/n3
curl -s localhost:8080/api/cluster/replication
curl -s -X POST localhost:8080/api/cluster/fault/restart/n3
```

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
| `ConditionalWriteTest` | 条件写命中/不命中（带当前版本返回）、键不存在（读与条件写）、删除走版本路径、版本单调不倒退（含主切换后） |
| `ClientTermAndDedupTest` | 落后 clientTerm 拒绝（STALE_TERM）、同 requestId 内容不一致拒绝（REQUEST_CONFLICT）、一致重放、老请求兼容 |
| `ReplicationProgressTest` | 暂停副本 BLOCKED、崩溃副本 DOWN、重启后 CATCHING_UP → IN_SYNC 追平、已确认读不回滚 |
| `ClusterApiSmokeTest` | 真实 Spring 容器下端到端 HTTP 读写/观测/暂停/幂等；条件写/删除/落后任期/请求冲突/复制进度接口 |

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
- **版本与条件写**：版本号只存在于各副本的内存状态机中（随模拟的“持久状态”
  在 crash/restart 间保留），条件检查在主节点串行执行路径上完成；版本单调性、
  墓碑版本保留、条件写原子性等结论同样只在上述模拟模型内成立。
- **复制进度观测**：`nextIndex/matchIndex/lag/state` 是主节点的内存视图，
  `BLOCKED`/`CATCHING_UP` 的区分依赖模拟环境能直接读取暂停/分区/存活标志，
  真实系统中主节点无法这样区分“对方崩溃”与“网络断开”。
- 因此本文档所述“已提交不丢、未达多数派不成功、旧主不能覆盖、分区恢复一致”等
  结论，**仅在上述单进程、同步投递、非拜占庭模拟模型内可重复验证**。
