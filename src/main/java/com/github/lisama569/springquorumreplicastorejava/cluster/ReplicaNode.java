package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.LogEntry;
import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.Role;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOp;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import com.github.lisama569.springquorumreplicastorejava.rpc.Envelope;
import com.github.lisama569.springquorumreplicastorejava.rpc.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * 单副本节点：维护任期、日志、提交位置与键值状态机，
 * 处理选举与复制消息。
 *
 * <p>线程模型：所有公开方法由 {@link Cluster} 在全局锁内串行调用，
 * 节点内部不做并发控制。消息投递是同步的（同一调用栈内完成）。</p>
 *
 * <p>持久状态（崩溃/重启后保留）：currentTerm、votedFor、log、
 * commitIndex、lastApplied、kv、appliedRequests。
 * 易失状态（崩溃后丢失）：role、选举计时、leader 的 nextIndex/matchIndex。</p>
 */
public class ReplicaNode {

    private static final Logger log = LoggerFactory.getLogger(ReplicaNode.class);

    /** 心跳间隔（tick 数）。 */
    static final int HEARTBEAT_INTERVAL_TICKS = 2;
    /** 选举超时范围（tick 数）。 */
    static final int ELECTION_TIMEOUT_MIN_TICKS = 10;
    static final int ELECTION_TIMEOUT_RANGE_TICKS = 10;
    /** 空操作条目的键标记（主节点上任时写入，用于推进当前任期提交位置）。 */
    static final String NOOP_KEY = "";

    private final String id;
    private final List<String> allNodes;
    private final int quorum;
    private final MessageBus bus;
    private final Random random;

    // ---- 持久状态 ----
    private long currentTerm;
    private String votedFor;
    private final List<LogEntry> entries = new ArrayList<>();
    private long commitIndex;
    private long lastApplied;
    private final Map<String, String> kv = new HashMap<>();
    /** 每个键的当前版本（单调递增，删除后保留墓碑版本，保证只增不减）。 */
    private final Map<String, Long> versions = new HashMap<>();
    /** 已应用请求及其结果，用于幂等去重与重复内容校验（随状态机持久）。 */
    private final Map<String, AppliedRequest> appliedRequests = new HashMap<>();

    /**
     * 已应用请求记录：保存请求指纹（内容）与首次执行结果。
     * 同 requestId 重复提交时，仅内容完全一致才允许重放。
     */
    record AppliedRequest(WriteOp op, String key, String value, Long expectedVersion,
                          Long clientTerm, WriteOutcome outcome) {
        static AppliedRequest of(LogEntry entry, WriteOutcome outcome) {
            return new AppliedRequest(entry.op(), entry.key(), entry.value(),
                    entry.expectedVersion(), entry.clientTerm(), outcome);
        }

        boolean matches(WriteOp op, String key, String value, Long expectedVersion,
                        Long clientTerm) {
            return this.op == op && this.key.equals(key)
                    && java.util.Objects.equals(this.value, value)
                    && java.util.Objects.equals(this.expectedVersion, expectedVersion)
                    && java.util.Objects.equals(this.clientTerm, clientTerm);
        }
    }

    // ---- 易失状态 ----
    private Role role = Role.FOLLOWER;
    private boolean alive = true;
    private String currentLeader;
    private int electionElapsed;
    private int electionTimeoutTicks;
    private int heartbeatElapsed;
    private final Set<String> votesReceived = new HashSet<>();
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    /** 当轮读确认集合：同步投递下，clientRead 发起的心跳往返内响应的从节点。 */
    private final Set<String> readConfirmations = new HashSet<>();

    public ReplicaNode(String id, List<String> allNodes, MessageBus bus) {
        this.id = id;
        this.allNodes = List.copyOf(allNodes);
        this.quorum = allNodes.size() / 2 + 1;
        this.bus = bus;
        this.random = new Random(id.hashCode());
        resetElectionTimeout();
    }

    public String id() {
        return id;
    }

    // ------------------------------------------------------------------
    // 时钟
    // ------------------------------------------------------------------

    /** 时钟推进一格：驱动选举超时与心跳。 */
    public void tick() {
        if (!alive) {
            return;
        }
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= HEARTBEAT_INTERVAL_TICKS) {
                heartbeatElapsed = 0;
                for (String peer : allNodes) {
                    if (!peer.equals(id)) {
                        replicateTo(peer);
                    }
                }
            }
        } else {
            if (++electionElapsed >= electionTimeoutTicks) {
                startElection();
            }
        }
    }

    private void resetElectionTimeout() {
        electionElapsed = 0;
        electionTimeoutTicks = ELECTION_TIMEOUT_MIN_TICKS
                + random.nextInt(ELECTION_TIMEOUT_RANGE_TICKS);
    }

    // ------------------------------------------------------------------
    // 选举
    // ------------------------------------------------------------------

    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        votesReceived.clear();
        votesReceived.add(id);
        resetElectionTimeout();
        log.info("node={} event=election-start term={} lastLogIndex={}",
                id, currentTerm, lastLogIndex());
        for (String peer : allNodes) {
            if (!peer.equals(id)) {
                bus.send(id, peer, new Message.RequestVoteRequest(
                        id, currentTerm, lastLogIndex(), lastLogTerm()));
            }
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        currentLeader = id;
        heartbeatElapsed = 0;
        for (String peer : allNodes) {
            nextIndex.put(peer, lastLogIndex() + 1);
            matchIndex.put(peer, peer.equals(id) ? lastLogIndex() : 0L);
        }
        log.info("node={} event=became-leader term={} commitIndex={}", id, currentTerm, commitIndex);
        // 上任写入 no-op 条目：只有提交当前任期条目才能按规则推进提交位置，
        // 同时让旧任期的未提交条目随多数派复制尽快落定。
        appendLocal(new LogEntry(currentTerm, lastLogIndex() + 1, WriteOp.NOOP,
                NOOP_KEY, NOOP_KEY, null, null, "noop-" + id + "-" + currentTerm));
        matchIndex.put(id, lastLogIndex());
        for (String peer : allNodes) {
            if (!peer.equals(id)) {
                replicateTo(peer);
            }
        }
    }

    private void stepDown(long newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
        }
        role = Role.FOLLOWER;
        votesReceived.clear();
    }

    // ------------------------------------------------------------------
    // 消息处理
    // ------------------------------------------------------------------

    /** 处理收到的消息。 */
    public void onMessage(Envelope envelope) {
        if (!alive) {
            return;
        }
        Message message = envelope.message();
        if (message.term() > currentTerm) {
            stepDown(message.term());
        }
        switch (message) {
            case Message.RequestVoteRequest request -> onRequestVote(request);
            case Message.RequestVoteResponse response -> onRequestVoteResponse(response);
            case Message.AppendEntriesRequest request -> onAppendEntries(request);
            case Message.AppendEntriesResponse response -> onAppendEntriesResponse(response);
        }
    }

    private void onRequestVote(Message.RequestVoteRequest request) {
        boolean granted;
        String reason;
        if (request.term() < currentTerm) {
            granted = false;
            reason = RejectionReason.STALE_TERM.name();
        } else if (votedFor != null && !votedFor.equals(request.from())) {
            granted = false;
            reason = "ALREADY_VOTED:" + votedFor;
        } else if (!isLogUpToDate(request.lastLogIndex(), request.lastLogTerm())) {
            granted = false;
            reason = "CANDIDATE_LOG_BEHIND";
        } else {
            granted = true;
            reason = "GRANT";
            votedFor = request.from();
            resetElectionTimeout();
        }
        log.info("node={} event=vote term={} candidate={} granted={} reason={}",
                id, currentTerm, request.from(), granted, reason);
        bus.send(id, request.from(),
                new Message.RequestVoteResponse(id, currentTerm, granted, reason));
    }

    private void onRequestVoteResponse(Message.RequestVoteResponse response) {
        if (role != Role.CANDIDATE || response.term() != currentTerm || !response.voteGranted()) {
            return;
        }
        votesReceived.add(response.from());
        log.info("node={} event=vote-received term={} from={} votes={}/{}",
                id, currentTerm, response.from(), votesReceived.size(), quorum);
        if (votesReceived.size() >= quorum) {
            becomeLeader();
        }
    }

    private void onAppendEntries(Message.AppendEntriesRequest request) {
        if (request.term() < currentTerm) {
            log.info("node={} event=append-rejected term={} leader={} reason={}",
                    id, currentTerm, request.from(), RejectionReason.STALE_TERM);
            bus.send(id, request.from(), new Message.AppendEntriesResponse(
                    id, currentTerm, false, lastLogIndex(), RejectionReason.STALE_TERM.name()));
            return;
        }
        // 承认当前任期的主节点
        role = Role.FOLLOWER;
        currentLeader = request.from();
        resetElectionTimeout();

        // 一致性检查：前一条日志必须匹配
        if (request.prevLogIndex() > 0) {
            if (lastLogIndex() < request.prevLogIndex()) {
                rejectAppend(request, "LOG_CONFLICT:missing-prevLogIndex");
                return;
            }
            if (termAt(request.prevLogIndex()) != request.prevLogTerm()) {
                rejectAppend(request, "LOG_CONFLICT:prevLogTerm-mismatch");
                return;
            }
        }
        // 幂等追加：同位置同任期条目视为已存在（重复/乱序投递安全）
        for (LogEntry entry : request.entries()) {
            if (entry.index() <= lastLogIndex()) {
                if (termAt(entry.index()) != entry.term()) {
                    truncateFrom(entry.index());
                    appendLocal(entry);
                }
                // 同任期同位置：已存在，跳过
            } else {
                appendLocal(entry);
            }
        }
        // 推进提交位置
        if (request.leaderCommit() > commitIndex) {
            long newCommit = Math.min(request.leaderCommit(), lastLogIndex());
            advanceCommitTo(newCommit);
        }
        bus.send(id, request.from(), new Message.AppendEntriesResponse(
                id, currentTerm, true, lastLogIndex(), "OK"));
    }

    private void rejectAppend(Message.AppendEntriesRequest request, String reason) {
        log.info("node={} event=append-rejected term={} leader={} prevLogIndex={} reason={}",
                id, currentTerm, request.from(), request.prevLogIndex(), reason);
        bus.send(id, request.from(), new Message.AppendEntriesResponse(
                id, currentTerm, false, lastLogIndex(), reason));
    }

    private void onAppendEntriesResponse(Message.AppendEntriesResponse response) {
        if (role != Role.LEADER || response.term() != currentTerm) {
            return;
        }
        String follower = response.from();
        if (response.success()) {
            matchIndex.put(follower, response.matchIndex());
            nextIndex.put(follower, response.matchIndex() + 1);
            readConfirmations.add(follower);
            advanceCommitByQuorum();
        } else {
            // 冲突回退：利用响应中的日志末端提示快速收敛，随后立即重试
            long hint = Math.min(nextIndex.getOrDefault(follower, lastLogIndex() + 1) - 1,
                    response.matchIndex() + 1);
            nextIndex.put(follower, Math.max(1, hint));
            log.info("node={} event=replication-conflict term={} follower={} reason={} nextIndex={}",
                    id, currentTerm, follower, response.reason(), nextIndex.get(follower));
            replicateTo(follower);
        }
    }

    // ------------------------------------------------------------------
    // 复制与提交
    // ------------------------------------------------------------------

    private void replicateTo(String follower) {
        long next = nextIndex.getOrDefault(follower, lastLogIndex() + 1);
        long prevLogIndex = next - 1;
        long prevLogTerm = prevLogIndex > 0 ? termAt(prevLogIndex) : 0;
        List<LogEntry> toSend = new ArrayList<>();
        for (long i = next; i <= lastLogIndex(); i++) {
            toSend.add(entries.get((int) i - 1));
        }
        bus.send(id, follower, new Message.AppendEntriesRequest(
                id, currentTerm, prevLogIndex, prevLogTerm, toSend, commitIndex));
    }

    /** 仅当条目属于当前任期且被多数派复制时才推进提交位置。 */
    private void advanceCommitByQuorum() {
        for (long n = lastLogIndex(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) {
                continue;
            }
            int count = 0;
            for (String peer : allNodes) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count >= quorum) {
                advanceCommitTo(n);
            }
            break;
        }
    }

    private void advanceCommitTo(long newCommit) {
        if (newCommit <= commitIndex) {
            return;
        }
        commitIndex = newCommit;
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = entries.get((int) lastApplied - 1);
            apply(entry);
        }
        log.info("node={} event=commit-advanced term={} commitIndex={}",
                id, currentTerm, commitIndex);
    }

    private void apply(LogEntry entry) {
        if (entry.op() == WriteOp.NOOP) {
            return;
        }
        long currentVersion = versions.getOrDefault(entry.key(), 0L);
        WriteOutcome outcome;
        if (entry.expectedVersion() != null && entry.expectedVersion() != currentVersion) {
            // 条件写入版本不匹配：不改动任何键值，返回实际版本供上层重试
            RejectionReason reason = currentVersion == 0
                    ? RejectionReason.KEY_NOT_FOUND : RejectionReason.VERSION_MISMATCH;
            outcome = WriteOutcome.rejected(entry.requestId(), entry.term(), entry.index(),
                    currentVersion, reason,
                    "expectedVersion=" + entry.expectedVersion()
                            + " actualVersion=" + currentVersion);
        } else if (entry.op() == WriteOp.DELETE && !kv.containsKey(entry.key())) {
            outcome = WriteOutcome.rejected(entry.requestId(), entry.term(), entry.index(),
                    currentVersion, RejectionReason.KEY_NOT_FOUND,
                    "delete of absent key=" + entry.key());
        } else {
            long newVersion = currentVersion + 1;
            if (entry.op() == WriteOp.DELETE) {
                kv.remove(entry.key());
            } else {
                kv.put(entry.key(), entry.value());
            }
            // 版本只增不减：删除也推进版本（墓碑保留在 versions 中）
            versions.put(entry.key(), newVersion);
            outcome = WriteOutcome.ok(entry.requestId(), entry.term(), entry.index(),
                    newVersion, "applied-by-commit op=" + entry.op());
        }
        appliedRequests.put(entry.requestId(), AppliedRequest.of(entry, outcome));
        log.info("node={} event=applied requestId={} term={} commitIndex={} key={} op={}"
                        + " version={} reason={}",
                id, entry.requestId(), entry.term(), entry.index(), entry.key(), entry.op(),
                outcome.version(), outcome.reason());
    }

    // ------------------------------------------------------------------
    // 客户端接口
    // ------------------------------------------------------------------

    /** 客户端写请求（旧签名：无条件 PUT、不携带客户端任期）。 */
    public WriteOutcome clientWrite(String key, String value, String requestId) {
        return clientWrite(WriteOp.PUT, key, value, requestId, null, null);
    }

    /**
     * 客户端写请求（仅主节点可成功）。
     *
     * @param op              PUT 或 DELETE
     * @param expectedVersion 期望的当前版本；null 表示无条件写入
     * @param clientTerm      调用方缓存的任期；落后于当前任期时明确拒绝
     */
    public WriteOutcome clientWrite(WriteOp op, String key, String value, String requestId,
                                    Long expectedVersion, Long clientTerm) {
        if (!alive) {
            return WriteOutcome.rejected(requestId, currentTerm, commitIndex, 0,
                    RejectionReason.NODE_UNAVAILABLE, "node=" + id + " crashed");
        }
        if (role != Role.LEADER) {
            return WriteOutcome.rejected(requestId, currentTerm, commitIndex, 0,
                    RejectionReason.NOT_LEADER,
                    "node=" + id + " role=" + role + " leader=" + currentLeader);
        }
        AppliedRequest previous = appliedRequests.get(requestId);
        if (previous != null) {
            if (previous.matches(op, key, value, expectedVersion, clientTerm)) {
                WriteOutcome stored = previous.outcome();
                log.info("node={} event=write-duplicate requestId={} term={} commitIndex={}"
                                + " version={} originalReason={}",
                        id, requestId, currentTerm, stored.commitIndex(), stored.version(),
                        stored.reason());
                return new WriteOutcome(stored.success(), requestId, currentTerm,
                        stored.commitIndex(), stored.version(), RejectionReason.DUPLICATE_REQUEST,
                        "idempotent-replay of commitIndex=" + stored.commitIndex()
                                + " originalReason=" + stored.reason());
            }
            log.info("node={} event=write-rejected requestId={} term={} commitIndex={} reason={}",
                    id, requestId, currentTerm, commitIndex, RejectionReason.REQUEST_CONFLICT);
            return WriteOutcome.rejected(requestId, currentTerm, commitIndex,
                    versions.getOrDefault(key, 0L), RejectionReason.REQUEST_CONFLICT,
                    "requestId=" + requestId + " already applied with different content");
        }
        if (clientTerm != null && clientTerm < currentTerm) {
            log.info("node={} event=write-rejected requestId={} term={} clientTerm={}"
                            + " commitIndex={} reason={}",
                    id, requestId, currentTerm, clientTerm, commitIndex, RejectionReason.STALE_TERM);
            return WriteOutcome.rejected(requestId, currentTerm, commitIndex, 0,
                    RejectionReason.STALE_TERM,
                    "clientTerm=" + clientTerm + " < currentTerm=" + currentTerm);
        }
        LogEntry entry = new LogEntry(currentTerm, lastLogIndex() + 1, op, key, value,
                expectedVersion, clientTerm, requestId);
        appendLocal(entry);
        matchIndex.put(id, entry.index());
        for (String peer : allNodes) {
            if (!peer.equals(id)) {
                replicateTo(peer);
            }
        }
        advanceCommitByQuorum();
        if (commitIndex >= entry.index()) {
            // 提交后条件校验结果已在 apply 中确定（可能为 VERSION_MISMATCH/KEY_NOT_FOUND）
            WriteOutcome outcome = appliedRequests.get(requestId).outcome();
            log.info("node={} event=write-committed requestId={} term={} commitIndex={}"
                            + " version={} reason={}",
                    id, requestId, currentTerm, commitIndex, outcome.version(), outcome.reason());
            return outcome;
        }
        log.info("node={} event=write-rejected requestId={} term={} commitIndex={} reason={}",
                id, requestId, currentTerm, commitIndex, RejectionReason.NO_QUORUM);
        return WriteOutcome.rejected(requestId, currentTerm, commitIndex, 0,
                RejectionReason.NO_QUORUM,
                "entry index=" + entry.index() + " not replicated to quorum");
    }

    /** 客户端读请求（需多数派确认的线性一致读）。 */
    public ReadOutcome clientRead(String key, String requestId) {
        if (!alive) {
            return ReadOutcome.rejected(key, currentTerm, commitIndex,
                    RejectionReason.NODE_UNAVAILABLE, "node=" + id + " crashed");
        }
        if (role != Role.LEADER) {
            return ReadOutcome.rejected(key, currentTerm, commitIndex,
                    RejectionReason.NOT_LEADER,
                    "node=" + id + " role=" + role + " leader=" + currentLeader);
        }
        // 读前确认自己仍是当前任期的多数派主节点（read-index 机制）：
        // 清空当轮确认集后发起一轮心跳，同步投递下响应会在本调用栈内回填确认集。
        readConfirmations.clear();
        for (String peer : allNodes) {
            if (!peer.equals(id)) {
                replicateTo(peer);
            }
        }
        int confirmations = 1 + readConfirmations.size();
        if (confirmations < quorum) {
            log.info("node={} event=read-rejected requestId={} term={} commitIndex={} reason={}",
                    id, requestId, currentTerm, commitIndex, RejectionReason.NO_QUORUM);
            return ReadOutcome.rejected(key, currentTerm, commitIndex,
                    RejectionReason.NO_QUORUM,
                    "read-index confirmations=" + confirmations + " quorum=" + quorum);
        }
        boolean found = kv.containsKey(key);
        String value = found ? kv.get(key) : null;
        long version = versions.getOrDefault(key, 0L);
        log.info("node={} event=read-served requestId={} term={} commitIndex={} key={} hit={}"
                        + " version={}",
                id, requestId, currentTerm, commitIndex, key, found, version);
        return ReadOutcome.ok(key, found, value, version, currentTerm, commitIndex,
                "read-index quorum=" + confirmations + " commitIndex=" + commitIndex
                        + (found ? "" : " key-not-found"));
    }

    // ------------------------------------------------------------------
    // 崩溃与重启
    // ------------------------------------------------------------------

    /** 模拟崩溃：清空易失状态，保留持久状态（任期、投票、日志、状态机）。 */
    public void crash() {
        alive = false;
        role = Role.FOLLOWER;
        votesReceived.clear();
        nextIndex.clear();
        matchIndex.clear();
        currentLeader = null;
        log.info("node={} event=crash term={} commitIndex={}", id, currentTerm, commitIndex);
    }

    /** 模拟重启：从持久状态恢复并重新加入集群。 */
    public void restart() {
        alive = true;
        role = Role.FOLLOWER;
        resetElectionTimeout();
        log.info("node={} event=restart term={} commitIndex={} logSize={}",
                id, currentTerm, commitIndex, entries.size());
    }

    // ------------------------------------------------------------------
    // 观测
    // ------------------------------------------------------------------

    /** 运行态快照。 */
    public NodeStatus status() {
        return new NodeStatus(id, role, currentTerm, commitIndex, lastApplied,
                entries.size(), false, alive);
    }

    /**
     * 主节点视角：各从副本的复制进度，peer → {sentIndex, matchIndex}。
     * 仅作为主节点时有意义；sentIndex = nextIndex-1（已尝试发送到的位置）。
     */
    public Map<String, long[]> replicationView() {
        Map<String, long[]> view = new java.util.LinkedHashMap<>();
        for (String peer : allNodes) {
            if (!peer.equals(id)) {
                long matched = matchIndex.getOrDefault(peer, 0L);
                long sent = nextIndex.getOrDefault(peer, lastLogIndex() + 1) - 1;
                view.put(peer, new long[]{sent, matched});
            }
        }
        return view;
    }

    // ------------------------------------------------------------------
    // 日志辅助
    // ------------------------------------------------------------------

    private long lastLogIndex() {
        return entries.size();
    }

    private long lastLogTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    private long termAt(long index) {
        return entries.get((int) index - 1).term();
    }

    private void appendLocal(LogEntry entry) {
        entries.add(entry);
    }

    private void truncateFrom(long index) {
        while (lastLogIndex() >= index) {
            entries.remove(entries.size() - 1);
        }
    }

    private boolean isLogUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = lastLogTerm();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= lastLogIndex();
    }

    /** 日志条目快照（测试构造乱序消息用）。 */
    List<LogEntry> logSnapshot() {
        return List.copyOf(entries);
    }
}
