package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单个 Raft 副本。
 *
 * <p>维护持久化状态（模拟）：currentTerm / votedFor / log；
 * 易失状态：commitIndex / applied 数据 / leader 的 nextIndex、matchIndex。
 * 所有方法仅由 {@link RaftCluster} 的单线程消息泵调用，因此方法内部无需加锁。
 * 任期、日志匹配、多数派提交严格遵循 Raft；另外实现：
 * 客户端 requestId 去重、CAS 条件写、读需多数派屏障确认。</p>
 */
public class ReplicaNode {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaNode.class);

    /** 单个客户端写请求在领导节点上的跟踪状态。 */
    private static final class PendingWrite {
        final long entryIndex;
        final String key;
        final String value;
        final long deadlineTick;

        PendingWrite(long entryIndex, String key, String value, long deadlineTick) {
            this.entryIndex = entryIndex;
            this.key = key;
            this.value = value;
            this.deadlineTick = deadlineTick;
        }
    }

    /** 读请求的多数派屏障跟踪。 */
    private static final class PendingRead {
        final String requestId;
        final String key;
        final Set<String> barrierAcks = new HashSet<>();
        final long deadlineTick;

        PendingRead(String requestId, String key, long deadlineTick) {
            this.requestId = requestId;
            this.key = key;
            this.deadlineTick = deadlineTick;
        }
    }

    private final String nodeId;
    private final RaftCluster cluster;
    private final Network net;

    // ---- 持久化状态（模拟重启后保留） ----
    private long currentTerm;
    private String votedFor;
    private final List<LogEntry> log = new ArrayList<>();

    // ---- 易失状态 ----
    private Role role = Role.FOLLOWER;
    private long commitIndex;
    private long lastApplied;
    private final Map<String, String> stateMachine = new LinkedHashMap<>();

    // leader 易失状态
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    // candidate 易失状态
    private final Set<String> votesReceived = new HashSet<>();

    // 客户端请求跟踪
    private final Map<String, PendingWrite> pendingWrites = new HashMap<>();
    private PendingRead pendingRead;
    /** requestId -> 已提交日志索引，用于跨重启/重复请求直接返回成功。 */
    private final Map<String, Long> committedWrites = new HashMap<>();
    /** 已判定“未达多数派”而中止的 requestId，之后同一请求一律按失败处理，绝不允许事后提交。 */
    private final Set<String> abortedWrites = new HashSet<>();

    private long nowTick;

    public ReplicaNode(String nodeId, RaftCluster cluster, Network network) {
        this.nodeId = nodeId;
        this.cluster = cluster;
        this.net = network;
        // index 0 哨兵，term 0
        this.log.add(new LogEntry(0, 0, null, null, null, null, true));
    }

    public String id() {
        return nodeId;
    }

    public Role role() {
        return role;
    }

    public long term() {
        return currentTerm;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastLogIndex() {
        return log.size() - 1L;
    }

    public long lastLogTerm() {
        return termAt(lastLogIndex());
    }

    private long termAt(long index) {
        if (index <= 0 || index >= log.size()) {
            return 0;
        }
        return log.get((int) index).term();
    }

    private LogEntry entryAt(long index) {
        if (index <= 0 || index >= log.size()) {
            return null;
        }
        return log.get((int) index);
    }

    // ======================================================================
    // 生命周期
    // ======================================================================

    /** 模拟节点重启：持久化状态（term/votedFor/log）保留，清空全部易失状态，
     * 重放已提交日志重建状态机。 */
    public void restart() {
        role = Role.FOLLOWER;
        nextIndex.clear();
        matchIndex.clear();
        votesReceived.clear();
        pendingWrites.clear();
        pendingRead = null;
        // 持久化状态（currentTerm/votedFor/log/commitIndex/abortedWrites）均保留；
        // 据此重放已提交日志重建状态机
        stateMachine.clear();
        committedWrites.clear();
        lastApplied = 0;
        applyCommitted();
        logger.info("node {} restarted: term={} commitIndex={} lastLogIndex={} replayedKeys={}",
                nodeId, currentTerm, commitIndex, lastLogIndex(), stateMachine.size());
    }

    /**
     * 某副本（重新）加入集群时由集群调用：若本节点是领导，重置对该 peer 的复制进度，
     * 从其日志尾部开始探测（停机期间在途消息已清空，旧的 nextIndex/matchIndex 不再可信）。
     */
    public void resetProgressFor(String peerId) {
        if (role == Role.LEADER && !peerId.equals(nodeId)) {
            matchIndex.put(peerId, 0L);
            nextIndex.put(peerId, lastLogIndex() + 1);
            sendAppendEntries(peerId, true);
        }
    }

    // ======================================================================
    // 消息入口
    // ======================================================================

    public void accept(RpcMessage message) {
        switch (message) {
            case RpcMessage.Timeout t -> handleTimeout(t);
            case RpcMessage.RequestVote rv -> handleRequestVote(rv);
            case RpcMessage.RequestVoteResult rvr -> handleRequestVoteResult(rvr);
            case RpcMessage.AppendEntries ae -> handleAppendEntries(ae);
            case RpcMessage.AppendEntriesResult aer -> handleAppendEntriesResult(aer);
            case RpcMessage.ClientWrite cw -> handleClientWrite(cw);
            case RpcMessage.ClientRead cr -> handleClientRead(cr);
            default -> {
                // client -> node 的响应不会出现在这里
            }
        }
    }

    /**
     * 每轮推进回调：先检查在途请求的超时预算（任何情况下都要执行）。
     */
    public void onTick(long tick) {
        this.nowTick = tick;
        if (role == Role.LEADER) {
            checkPendingDeadlines();
        }
    }

    /** 空闲轮（没有任何可投递消息）回调：领导补发日志/推进提交。 */
    public void onIdle(long tick) {
        this.nowTick = tick;
        if (role == Role.LEADER) {
            for (String peer : cluster.nodeIds()) {
                if (!peer.equals(nodeId)) {
                    sendAppendEntries(peer);
                }
            }
            tryAdvanceCommit();
        }
    }

    // ======================================================================
    // 选举
    // ======================================================================

    private void handleTimeout(RpcMessage.Timeout t) {
        if (t.term() < currentTerm) {
            logger.info("node {} ignore stale timeout term={} < currentTerm={}", nodeId, t.term(), currentTerm);
            return;
        }
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = nodeId;
        votesReceived.clear();
        votesReceived.add(nodeId);
        failAllPending(RejectReason.NOT_LEADER, "new election started");
        pendingRead = null;
        logger.info("requestVote decision: node={} requestId=- term={} commitIndex={} basis=election-timeout -> CANDIDATE",
                nodeId, currentTerm, commitIndex);
        for (String peer : cluster.nodeIds()) {
            if (!peer.equals(nodeId)) {
                net.send(new RpcMessage.RequestVote(nodeId, peer, net.nextId(), currentTerm,
                        lastLogIndex(), lastLogTerm()));
            }
        }
        if (votesReceived.size() >= cluster.quorum()) {
            becomeLeader();
        }
    }

    private void handleRequestVote(RpcMessage.RequestVote rv) {
        if (rv.term() < currentTerm) {
            net.send(new RpcMessage.RequestVoteResult(nodeId, rv.from(), net.nextId(), currentTerm, false));
            logger.info("vote decision: node={} candidate={} term={} candidateTerm={} basis=stale-term -> deny",
                    nodeId, rv.from(), currentTerm, rv.term());
            return;
        }
        if (rv.term() > currentTerm) {
            stepDown(rv.term());
        }
        boolean upToDate = rv.lastLogTerm() > lastLogTerm()
                || (rv.lastLogTerm() == lastLogTerm() && rv.lastLogIndex() >= lastLogIndex());
        boolean canVote = upToDate && (votedFor == null || votedFor.equals(rv.from()));
        if (canVote) {
            votedFor = rv.from();
        }
        net.send(new RpcMessage.RequestVoteResult(nodeId, rv.from(), net.nextId(), currentTerm, canVote));
        logger.info("vote decision: node={} requestId=- candidate={} term={} commitIndex={} candidateLog=({},{}) basis={} -> {}",
                nodeId, rv.from(), currentTerm, commitIndex, rv.lastLogIndex(), rv.lastLogTerm(),
                upToDate ? "log-up-to-date" : "candidate-log-stale",
                canVote ? "grant" : "deny(votedFor=" + votedFor + ")");
    }

    private void handleRequestVoteResult(RpcMessage.RequestVoteResult rvr) {
        if (rvr.term() > currentTerm) {
            stepDown(rvr.term());
            return;
        }
        if (role != Role.CANDIDATE || rvr.term() != currentTerm) {
            return;
        }
        if (rvr.granted()) {
            votesReceived.add(rvr.from());
            if (votesReceived.size() >= cluster.quorum()) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        nextIndex.clear();
        matchIndex.clear();
        for (String peer : cluster.nodeIds()) {
            if (!peer.equals(nodeId)) {
                nextIndex.put(peer, lastLogIndex() + 1);
                matchIndex.put(peer, 0L);
            }
        }
        // 领导人上任追加 no-op：提交后即可安全服务本任期读，并提交前任期条目
        LogEntry noOp = new LogEntry(currentTerm, lastLogIndex() + 1, null, null, null, null, true);
        log.add(noOp);
        logger.info("leader elected: node={} requestId=- term={} commitIndex={} basis=majority-votes={} -> LEADER noOp@{}",
                nodeId, currentTerm, commitIndex, votesReceived.size(), noOp.index());
        for (String peer : cluster.nodeIds()) {
            if (!peer.equals(nodeId)) {
                sendAppendEntries(peer);
            }
        }
        tryAdvanceCommit();
    }

    private void stepDown(long newTerm) {
        boolean termChanged = newTerm > currentTerm;
        currentTerm = newTerm;
        role = Role.FOLLOWER;
        if (termChanged) {
            votedFor = null;
        }
        nextIndex.clear();
        matchIndex.clear();
        votesReceived.clear();
        failAllPending(RejectReason.NOT_LEADER, "discovered higher term " + newTerm);
        pendingRead = null;
        logger.info("node {} stepDown -> FOLLOWER term={} commitIndex={}", nodeId, currentTerm, commitIndex);
        // 可能刚从分区/旧主状态回归：通知现有领导重置对本节点的复制进度并立即补发
        cluster.notifyLeaderResetPeer(nodeId);
    }

    // ======================================================================
    // 日志复制
    // ======================================================================

    private void sendAppendEntries(String peer) {
        sendAppendEntries(peer, false);
    }

    private void sendAppendEntries(String peer, boolean force) {
        // 默认情况下该链路仍有在途消息时不补发，避免确定性泵下队列只进不出；
        // 冲突回退后由 force=true 立即重发（此刻旧应答已被消费）。
        if (!force && net.pendingCount(nodeId, peer) > 0) {
            return;
        }
        long prevIndex = nextIndex.getOrDefault(peer, lastLogIndex() + 1) - 1;
        long prevTerm = termAt(prevIndex);
        LogEntry entry = entryAt(prevIndex + 1);
        net.send(new RpcMessage.AppendEntries(nodeId, peer, net.nextId(), currentTerm, nodeId,
                prevIndex, prevTerm, commitIndex, entry));
    }

    private void handleAppendEntries(RpcMessage.AppendEntries ae) {
        if (ae.term() < currentTerm) {
            net.send(new RpcMessage.AppendEntriesResult(nodeId, ae.from(), net.nextId(), currentTerm, false,
                    0, lastLogIndex() + 1, -1L));
            logger.info("appendEntries decision: node={} leader={} requestId=- term={} leaderTerm={} commitIndex={} basis=stale-term -> reject",
                    nodeId, ae.from(), currentTerm, ae.term(), commitIndex);
            return;
        }
        if (ae.term() > currentTerm) {
            stepDown(ae.term());
        } else if (role != Role.FOLLOWER) {
            // 同任期收到合法领导人心跳：退回跟随者
            stepDown(currentTerm);
        }

        long prevIndex = ae.prevLogIndex();
        long prevTerm = ae.prevLogTerm();
        if (prevIndex > lastLogIndex()) {
            net.send(new RpcMessage.AppendEntriesResult(nodeId, ae.from(), net.nextId(), currentTerm, false,
                    0, lastLogIndex() + 1, -1L));
            logger.info("appendEntries decision: node={} leader={} requestId=- term={} commitIndex={} basis=missing-prev@{} -> reject",
                    nodeId, ae.from(), currentTerm, commitIndex, prevIndex);
            return;
        }
        if (termAt(prevIndex) != prevTerm) {
            long conflictTerm = termAt(prevIndex);
            long conflictIndex = prevIndex;
            for (long i = prevIndex; i >= 1; i--) {
                if (termAt(i) == conflictTerm) {
                    conflictIndex = i;
                } else {
                    break;
                }
            }
            net.send(new RpcMessage.AppendEntriesResult(nodeId, ae.from(), net.nextId(), currentTerm, false,
                    0, conflictIndex, conflictTerm));
            logger.info("appendEntries decision: node={} leader={} requestId=- term={} commitIndex={} basis=conflict-term@{} expected={} actual={} -> reject",
                    nodeId, ae.from(), currentTerm, commitIndex, prevIndex, prevTerm, conflictTerm);
            return;
        }

        LogEntry entry = ae.entry();
        if (entry != null) {
            LogEntry existing = entryAt(entry.index());
            if (existing == null) {
                if (entry.index() != lastLogIndex() + 1) {
                    // 出现空洞：拒绝，等领导人按 nextIndex 重传
                    net.send(new RpcMessage.AppendEntriesResult(nodeId, ae.from(), net.nextId(), currentTerm,
                            false, 0, lastLogIndex() + 1, -1L));
                    return;
                }
                log.add(entry);
            } else if (existing.term() != entry.term() || !sameRequest(existing, entry)) {
                // 冲突：截断该位置及之后的未提交条目（已提交的不可能被截断，见提交规则）
                while (log.size() > entry.index()) {
                    log.remove(log.size() - 1);
                }
                log.set((int) entry.index(), entry);
            }
        }

        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.min(ae.leaderCommit(), lastLogIndex());
            applyCommitted();
        }

        long matchIndexValue = entry != null ? entry.index() : prevIndex;
        net.send(new RpcMessage.AppendEntriesResult(nodeId, ae.from(), net.nextId(), currentTerm, true,
                matchIndexValue, 0, -1L));
        if (entry != null) {
            logger.info("appendEntries decision: node={} leader={} requestId={} term={} commitIndex={} basis=append@{} -> accept matchIndex={}",
                    nodeId, ae.from(), entry.requestId(), currentTerm, commitIndex, entry.index(), matchIndexValue);
        }
    }

    private static boolean sameRequest(LogEntry a, LogEntry b) {
        return java.util.Objects.equals(a.requestId(), b.requestId())
                && java.util.Objects.equals(a.key(), b.key())
                && java.util.Objects.equals(a.value(), b.value());
    }

    private void handleAppendEntriesResult(RpcMessage.AppendEntriesResult aer) {
        if (aer.term() > currentTerm) {
            stepDown(aer.term());
            return;
        }
        if (role != Role.LEADER || aer.term() != currentTerm) {
            return;
        }
        String peer = aer.from();
        if (aer.success()) {
            long newMatch = Math.max(matchIndex.getOrDefault(peer, 0L), aer.matchIndex());
            boolean advanced = newMatch > matchIndex.getOrDefault(peer, 0L);
            matchIndex.put(peer, newMatch);
            nextIndex.put(peer, newMatch + 1);
            tryAdvanceCommit();
            checkPendingWrites();
            checkPendingRead(peer);
            // 应答驱动流水线：对端推进后立即补发下一条（此时该应答消息已被消费出队）
            if (advanced || newMatch < lastLogIndex()) {
                sendAppendEntries(peer, true);
            }
        } else {
            long ni = nextIndex.getOrDefault(peer, lastLogIndex() + 1);
            if (aer.conflictTerm() >= 0) {
                long conflictTerm = aer.conflictTerm();
                long lastOfTerm = 0;
                for (long i = lastLogIndex(); i >= 1; i--) {
                    if (termAt(i) == conflictTerm) {
                        lastOfTerm = i;
                        break;
                    }
                }
                ni = lastOfTerm > 0 ? lastOfTerm + 1 : Math.max(1, aer.conflictIndex());
            } else {
                ni = Math.max(1, aer.conflictIndex());
            }
            nextIndex.put(peer, ni);
            // 链路空闲（失败应答已被消费），立即按回退后的位置重发，避免恢复节点要等多个空闲轮
            sendAppendEntries(peer, true);
        }
    }

    /** 仅提交当前任期的多数派条目；提交点一旦前进便不可回退。 */
    private void tryAdvanceCommit() {
        if (role != Role.LEADER) {
            return;
        }
        // 从 commitIndex+1 起向上寻找连续的多数派匹配；只允许提交当前任期条目，
        // 找到第一个多数派条目 N（当前任期）时，commitIndex 可直接推进到 N
        // （中间的前任期条目借助 Raft 的日志一致性被一并提交）。
        for (long n = commitIndex + 1; n <= lastLogIndex(); n++) {
            int count = 1;
            for (String peer : cluster.nodeIds()) {
                if (!peer.equals(nodeId) && matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }
            if (count < cluster.quorum()) {
                break; // 连续性：更靠后的条目不可能先提交
            }
            if (termAt(n) == currentTerm) {
                commitIndex = n;
                applyCommitted();
                checkPendingWrites();
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            LogEntry e = log.get((int) ++lastApplied);
            if (!e.noOp()) {
                stateMachine.put(e.key(), e.value());
                committedWrites.put(e.requestId(), e.index());
            }
        }
    }

    // ======================================================================
    // 客户端写
    // ======================================================================

    private void handleClientWrite(RpcMessage.ClientWrite cw) {
        if (role != Role.LEADER) {
            replyWrite(cw, false, RejectReason.NOT_LEADER,
                    "not leader in term " + currentTerm);
            logger.info("write decision: node={} requestId={} term={} commitIndex={} basis=not-leader -> reject",
                    nodeId, cw.requestId(), currentTerm, commitIndex);
            return;
        }
        if (cw.expectedTerm() != null && cw.expectedTerm() != currentTerm) {
            replyWrite(cw, false, RejectReason.STALE_TERM,
                    "request term " + cw.expectedTerm() + " != leader term " + currentTerm);
            logger.info("write decision: node={} requestId={} term={} commitIndex={} expectedTerm={} basis=stale-term -> reject",
                    nodeId, cw.requestId(), currentTerm, commitIndex, cw.expectedTerm());
            return;
        }
        if (abortedWrites.contains(cw.requestId())) {
            replyWrite(cw, false, RejectReason.QUORUM_UNAVAILABLE,
                    "request previously failed to reach quorum and was aborted");
            logger.info("write decision: node={} requestId={} term={} commitIndex={} basis=previously-aborted -> reject",
                    nodeId, cw.requestId(), currentTerm, commitIndex);
            return;
        }
        // 幂等：同一 requestId
        Long committedAt = committedWrites.get(cw.requestId());
        if (committedAt != null) {
            LogEntry committed = entryAt(committedAt);
            if (!sameKeyPayload(committed, cw)) {
                replyWrite(cw, false, RejectReason.DUPLICATE_MISMATCH,
                        "requestId reused with different key/value");
                return;
            }
            replyWrite(cw, true, null, "duplicate request, already committed");
            logger.info("write decision: node={} requestId={} term={} commitIndex={} basis=duplicate-committed@{} -> success(idempotent)",
                    nodeId, cw.requestId(), currentTerm, commitIndex, committedAt);
            return;
        }
        LogEntry sameId = findLogEntryByRequestId(cw.requestId());
        if (sameId != null) {
            if (!sameKeyPayload(sameId, cw)) {
                replyWrite(cw, false, RejectReason.DUPLICATE_MISMATCH,
                        "requestId reused with different key/value");
                logger.info("write decision: node={} requestId={} term={} commitIndex={} basis=duplicate-mismatch -> reject",
                        nodeId, cw.requestId(), currentTerm, commitIndex);
                return;
            }
            pendingWrites.computeIfAbsent(cw.requestId(),
                    k -> new PendingWrite(sameId.index(), cw.key(), cw.value(), nowTick + 60));
            replicateAll();
            return;
        }
        // CAS 检查（基于已提交状态）
        if (cw.expectedValue() != null) {
            String current = stateMachine.get(cw.key());
            if (!cw.expectedValue().equals(current)) {
                replyWrite(cw, false, RejectReason.CAS_CONFLICT,
                        "expected=" + cw.expectedValue() + " actual=" + current);
                logger.info("write decision: node={} requestId={} term={} commitIndex={} key={} basis=cas-conflict expected={} actual={} -> reject",
                        nodeId, cw.requestId(), currentTerm, commitIndex, cw.key(), cw.expectedValue(), current);
                return;
            }
        }
        LogEntry entry = new LogEntry(currentTerm, lastLogIndex() + 1, cw.requestId(),
                cw.key(), cw.value(), cw.expectedValue(), false);
        log.add(entry);
        pendingWrites.put(cw.requestId(), new PendingWrite(entry.index(), cw.key(), cw.value(), nowTick + 60));
        logger.info("write decision: node={} requestId={} term={} commitIndex={} key={} basis=appended@{} -> replicate",
                nodeId, cw.requestId(), currentTerm, commitIndex, cw.key(), entry.index());
        replicateAll();
    }

    private void replicateAll() {
        for (String peer : cluster.nodeIds()) {
            if (!peer.equals(nodeId)) {
                sendAppendEntries(peer);
            }
        }
    }

    private static boolean sameKeyPayload(LogEntry e, RpcMessage.ClientWrite cw) {
        return e != null && java.util.Objects.equals(e.key(), cw.key())
                && java.util.Objects.equals(e.value(), cw.value());
    }

    private LogEntry findLogEntryByRequestId(String requestId) {
        for (int i = log.size() - 1; i >= 1; i--) {
            LogEntry e = log.get(i);
            if (requestId.equals(e.requestId())) {
                return e;
            }
        }
        return null;
    }

    private void checkPendingWrites() {
        var it = pendingWrites.entrySet().iterator();
        while (it.hasNext()) {
            var en = it.next();
            PendingWrite pw = en.getValue();
            if (pw.entryIndex <= commitIndex) {
                it.remove();
                net.send(new RpcMessage.ClientResponse(nodeId, "client", net.nextId(), en.getKey(), true,
                        null, currentTerm, commitIndex, pw.key, pw.value));
                logger.info("write decision: node={} requestId={} term={} commitIndex={} basis=majority-commit@{} -> success",
                        nodeId, en.getKey(), currentTerm, commitIndex, pw.entryIndex);
            }
        }
    }

    private void checkPendingDeadlines() {
        var it = pendingWrites.entrySet().iterator();
        while (it.hasNext()) {
            var en = it.next();
            if (nowTick > en.getValue().deadlineTick) {
                it.remove();
                abortWrite(en.getKey(), en.getValue());
                net.send(new RpcMessage.ClientResponse(nodeId, "client", net.nextId(), en.getKey(), false,
                        RejectReason.QUORUM_UNAVAILABLE, currentTerm, commitIndex,
                        en.getValue().key, en.getValue().value));
                logger.info("write decision: node={} requestId={} term={} commitIndex={} basis=quorum-unavailable-after-budget -> reject",
                        nodeId, en.getKey(), currentTerm, commitIndex);
            }
        }
        if (pendingRead != null && nowTick > pendingRead.deadlineTick) {
            net.send(new RpcMessage.ReadResult(nodeId, "client", net.nextId(), pendingRead.requestId,
                    false, null, currentTerm, commitIndex, RejectReason.QUORUM_UNAVAILABLE));
            logger.info("read decision: node={} requestId={} term={} commitIndex={} basis=barrier-quorum-unavailable -> reject",
                    nodeId, pendingRead.requestId, currentTerm, commitIndex);
            pendingRead = null;
        }
    }

    /**
     * 中止一个未在预算内达到多数派的客户端写：截断领导日志中所有未提交的客户端条目
     * （含本条及之后的条目），迫使已经持有这些条目的跟随者在下一轮 AE 一致性检查中
     * 删除它们；同时登记 abortedWrites，使该 requestId 之后永远不能“失败后又提交”。
     */
    private void abortWrite(String requestId, PendingWrite pw) {
        abortedWrites.add(requestId);
        if (pw.entryIndex > commitIndex && role == Role.LEADER) {
            while (log.size() - 1L > Math.max(commitIndex, pw.entryIndex - 1)) {
                LogEntry removed = log.remove(log.size() - 1);
                if (!removed.noOp() && removed.requestId() != null) {
                    abortedWrites.add(removed.requestId());
                }
            }
            for (String peer : cluster.nodeIds()) {
                if (!peer.equals(nodeId)) {
                    matchIndex.put(peer, Math.min(matchIndex.getOrDefault(peer, 0L), commitIndex));
                    nextIndex.put(peer, Math.min(nextIndex.getOrDefault(peer, lastLogIndex() + 1),
                            lastLogIndex() + 1));
                }
            }
            logger.info("abort write: node={} requestId={} term={} commitIndex={} truncatedLogTo={}",
                    nodeId, requestId, currentTerm, commitIndex, lastLogIndex());
        }
    }

    private void failAllPending(RejectReason reason, String basis) {
        for (String requestId : new ArrayList<>(pendingWrites.keySet())) {
            PendingWrite pw = pendingWrites.remove(requestId);
            net.send(new RpcMessage.ClientResponse(nodeId, "client", net.nextId(), requestId, false,
                    reason, currentTerm, commitIndex, pw.key, pw.value));
            logger.info("write decision: node={} requestId={} term={} commitIndex={} basis={} -> reject({})",
                    nodeId, requestId, currentTerm, commitIndex, basis, reason);
        }
        if (pendingRead != null) {
            net.send(new RpcMessage.ReadResult(nodeId, "client", net.nextId(), pendingRead.requestId,
                    false, null, currentTerm, commitIndex, reason));
            pendingRead = null;
        }
    }

    private void replyWrite(RpcMessage.ClientWrite cw, boolean success, RejectReason reason, String detail) {
        net.send(new RpcMessage.ClientResponse(nodeId, "client", net.nextId(), cw.requestId(), success,
                reason, currentTerm, commitIndex, cw.key(), success ? cw.value() : null));
        if (!success) {
            logger.info("write decision: node={} requestId={} term={} commitIndex={} basis={} -> reject({})",
                    nodeId, cw.requestId(), currentTerm, commitIndex, detail, reason);
        }
    }

    // ======================================================================
    // 客户端读（多数派屏障）
    // ======================================================================

    private void handleClientRead(RpcMessage.ClientRead cr) {
        if (role != Role.LEADER) {
            net.send(new RpcMessage.ReadResult(nodeId, "client", net.nextId(), cr.requestId(),
                    false, null, currentTerm, commitIndex, RejectReason.NOT_LEADER));
            logger.info("read decision: node={} requestId={} term={} commitIndex={} basis=not-leader -> reject",
                    nodeId, cr.requestId(), currentTerm, commitIndex);
            return;
        }
        // 新一轮读屏障：向所有跟随者发心跳，收到多数派成功应答后再读本地状态机
        pendingRead = new PendingRead(cr.requestId(), cr.key(), nowTick + 60);
        pendingRead.barrierAcks.add(nodeId);
        replicateAll();
        checkPendingRead(nodeId);
    }

    private void checkPendingRead(String ackPeer) {
        if (pendingRead == null || role != Role.LEADER) {
            return;
        }
        pendingRead.barrierAcks.add(ackPeer);
        if (pendingRead.barrierAcks.size() >= cluster.quorum()) {
            // 屏障达成时，本节点 commitIndex 已是多数派可见位置（no-op 提交规则保证）
            applyCommitted();
            String requestId = pendingRead.requestId;
            String key = pendingRead.key;
            boolean found = stateMachine.containsKey(key);
            String value = stateMachine.get(key);
            pendingRead = null;
            net.send(new RpcMessage.ReadResult(nodeId, "client", net.nextId(), requestId, found,
                    value, currentTerm, commitIndex, found ? null : RejectReason.KEY_NOT_FOUND));
            logger.info("read decision: node={} requestId={} term={} commitIndex={} basis=quorum-barrier -> {}",
                    nodeId, requestId, currentTerm, commitIndex, found ? "return value" : "key not found");
        }
    }

    // ======================================================================
    // 观测
    // ======================================================================

    public NodeSnapshot snapshot() {
        List<String> summary = new ArrayList<>();
        for (int i = 1; i < log.size(); i++) {
            LogEntry e = log.get(i);
            summary.add(e.index() + ":" + e.term() + ":" + (e.requestId() == null ? "noOp" : e.requestId()));
        }
        return new NodeSnapshot(nodeId, role, currentTerm, votedFor, currentTerm, commitIndex,
                lastLogIndex(), lastLogTerm(), lastApplied,
                net.isPaused(nodeId), !net.isDead(nodeId),
                Map.copyOf(stateMachine),
                Map.copyOf(nextIndex), Map.copyOf(matchIndex), List.copyOf(summary));
    }
}
