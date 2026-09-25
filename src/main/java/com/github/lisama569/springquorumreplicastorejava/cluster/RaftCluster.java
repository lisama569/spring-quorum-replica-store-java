package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单进程多副本 Raft 键值集群。
 *
 * <p>所有副本共享一个 {@link Network}，全部推进由确定性消息泵完成：
 * 客户端请求先入队，{@link #tick()} 每轮先让领导节点补发日志/推进提交，
 * 再投递一条在途消息给接收节点处理。{@link #settle()} 持续推进到本节点拿到
 * 客户端响应或预算耗尽。因此乱序、重复、暂停、分区等场景可以重复验证。</p>
 */
public class RaftCluster {

    private static final Logger logger = LoggerFactory.getLogger(RaftCluster.class);

    private static final long MAX_TICKS = 5_000;
    private static final int MAX_WRITE_RETRIES = 5;

    private final List<String> nodeIds;
    private final Map<String, ReplicaNode> nodes = new LinkedHashMap<>();
    private final Network network;
    private final AtomicLong clientSeq = new AtomicLong();

    private long tickCount;

    public RaftCluster() {
        this(List.of("n1", "n2", "n3"), new Network());
    }

    public RaftCluster(List<String> nodeIds) {
        this(nodeIds, new Network());
    }

    public RaftCluster(List<String> nodeIds, Network network) {
        if (nodeIds == null || nodeIds.size() < 1) {
            throw new IllegalArgumentException("need at least one node");
        }
        this.nodeIds = List.copyOf(nodeIds);
        this.network = network;
        this.network.register("client");
        for (String id : this.nodeIds) {
            this.network.register(id);
            this.nodes.put(id, new ReplicaNode(id, this, network));
        }
        logger.info("cluster initialized: nodes={} quorum={}", this.nodeIds, quorum());
    }

    List<String> nodeIds() {
        return nodeIds;
    }

    int quorum() {
        return nodeIds.size() / 2 + 1;
    }

    ReplicaNode node(String id) {
        ReplicaNode n = nodes.get(id);
        if (n == null) {
            throw new IllegalArgumentException("unknown node: " + id);
        }
        return n;
    }

    /**
     * 节点因发现更高任期退回跟随时调用：若当前已有同任期领导，让其重置
     * 对该节点的复制进度并立即补发，使分区恢复后的收敛不依赖额外空闲轮。
     */
    void notifyLeaderResetPeer(String peerId) {
        for (ReplicaNode n : nodes.values()) {
            if (n.role() == Role.LEADER && n.term() == node(peerId).term()
                    && !network.isDead(n.id()) && !network.isPaused(n.id())
                    && !network.isLinkDown(n.id(), peerId)) {
                n.resetProgressFor(peerId);
            }
        }
    }

    // ======================================================================
    // 确定性推进
    // ======================================================================

    /** 一轮推进：每轮检查请求超时；只有“空闲轮”才让领导补发日志。 */
    public boolean tick() {
        tickCount++;
        onEveryTick();
        RpcMessage message = network.pump();
        if (message == null) {
            onIdleTick();
            return false;
        }
        route(message);
        return true;
    }

    private void onIdleTick() {
        for (ReplicaNode node : nodes.values()) {
            if (!network.isDead(node.id()) && !network.isPaused(node.id())) {
                node.onIdle(tickCount);
            }
        }
    }

    private void onEveryTick() {
        for (ReplicaNode node : nodes.values()) {
            if (!network.isDead(node.id()) && !network.isPaused(node.id())) {
                node.onTick(tickCount);
            }
        }
    }

    private void route(RpcMessage message) {
        String to = message.to();
        if ("client".equals(to)) {
            return; // 客户端响应留在投递结果中由调用方捕获
        }
        if (network.isDead(to) || network.isPaused(to)) {
            return;
        }
        node(to).accept(message);
    }

    /** 推进直到队列中没有可投递消息（不保证客户端请求得到响应）。 */
    public void settle() {
        settleUntilClientResponse(false);
    }

    /** 测试辅助：推进直到下一条发给 client 的响应。 */
    public RpcMessage pumpOneClientMessage() {
        return settleUntilClientResponse(true);
    }

    /** 内部使用：同 {@link #settle()}，排空当前可投递消息。 */
    private void drain() {
        settleUntilClientResponse(false);
    }

    /**
     * 推进直到捕获一条发给 client 的消息。
     * 有在途消息时优先投递；队列为空的空闲轮触发领导补发（带预算，
     * 使无法形成多数派的场景在有限轮次后以 QUORUM_UNAVAILABLE 失败）。
     */
    private RpcMessage settleUntilClientResponse(boolean wantClientMessage) {
        long start = tickCount;
        boolean active;
        int idleBudget = 120;
        int idleRounds = 0;
        do {
            tickCount++;
            onEveryTick();
            RpcMessage message = network.pump();
            if (message == null) {
                active = false;
                if (idleRounds++ < idleBudget) {
                    onIdleTick();
                }
                if (wantClientMessage && idleRounds < idleBudget) {
                    continue;
                }
                if (wantClientMessage) {
                    throw new ReplicationException(RejectReason.QUORUM_UNAVAILABLE, "settle", -1, -1,
                            "no client response within idle budget, pending links=" + network.pendingLinks());
                }
                return null;
            }
            idleRounds = 0;
            active = true;
            if ("client".equals(message.to())) {
                return message;
            }
            if (!network.isDead(message.to()) && !network.isPaused(message.to())) {
                node(message.to()).accept(message);
            }
        } while (active || (wantClientMessage && tickCount - start < MAX_TICKS));
        return null;
    }

    // ======================================================================
    // 客户端 API
    // ======================================================================

    public record WriteResult(String requestId, String leaderId, long term, long commitIndex,
                              String key, String value, boolean success,
                              RejectReason rejectReason, String detail) {
    }

    public record ReadResult(boolean found, String key, String value, String leaderId,
                             long term, long commitIndex, RejectReason rejectReason) {
    }

    public WriteResult write(String requestId, String key, String value,
                             String expectedValue, Long expectedTerm) {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId is required");
        }
        for (int attempt = 0; attempt < MAX_WRITE_RETRIES; attempt++) {
            ReplicaNode leader = findAliveLeader();
            if (leader == null) {
                // 尝试在可用节点中发起一次选举
                if (!electAnyAlive()) {
                    return new WriteResult(requestId, null, -1, -1, key, null, false,
                            RejectReason.QUORUM_UNAVAILABLE,
                            "no leader and no election quorum available");
                }
                drain();
                continue;
            }
            long requestedTerm = expectedTerm != null ? expectedTerm : leader.term();
            network.send(new RpcMessage.ClientWrite("client", leader.id(), network.nextId(),
                    requestId, key, value, expectedValue, requestedTerm));
            RpcMessage response = settleUntilClientResponse(true);
            if (response instanceof RpcMessage.ClientResponse cr) {
                boolean success = cr.success();
                String detail = success ? "committed at index " + cr.commitIndex()
                        : ("rejected: " + cr.rejectReason());
                return new WriteResult(requestId, leader.id(), cr.term(), cr.commitIndex(),
                        key, cr.value(), success, cr.rejectReason(), detail);
            }
            // 未收到响应（领导中途失联），下一轮重试
            logger.info("client write requestId={} no response on attempt {}, retrying",
                    requestId, attempt);
        }
        return new WriteResult(requestId, null, -1, -1, key, null, false,
                RejectReason.QUORUM_UNAVAILABLE, "max retries exhausted");
    }

    public ReadResult read(String key) {
        ReplicaNode leader = findAliveLeader();
        if (leader == null) {
            return new ReadResult(false, key, null, null, -1, -1, RejectReason.NO_LEADER);
        }
        String requestId = "read-" + clientSeq.incrementAndGet();
        network.send(new RpcMessage.ClientRead("client", leader.id(), network.nextId(),
                requestId, key));
        RpcMessage response = settleUntilClientResponse(true);
        if (response instanceof RpcMessage.ReadResult rr) {
            return new ReadResult(rr.found(), key, rr.value(), leader.id(),
                    rr.term(), rr.commitIndex(), rr.rejectReason());
        }
        return new ReadResult(false, key, null, leader.id(), -1, -1,
                RejectReason.QUORUM_UNAVAILABLE);
    }

    private ReplicaNode findAliveLeader() {
        // 分区期可能同时存在多个自称 leader 的节点（脑裂）；客户端必须选最高任期者，
        // 它才可能是获得多数派的领导。低任期旧主的写会因无法形成多数派而失败。
        ReplicaNode best = null;
        for (ReplicaNode node : nodes.values()) {
            if (node.role() == Role.LEADER && !network.isDead(node.id())
                    && !network.isPaused(node.id())) {
                if (best == null || node.term() > best.term()) {
                    best = node;
                }
            }
        }
        return best;
    }

    private boolean electAnyAlive() {
        for (ReplicaNode node : nodes.values()) {
            if (!network.isDead(node.id()) && !network.isPaused(node.id())) {
                startElection(node.id());
                drain();
                return findAliveLeader() != null;
            }
        }
        return false;
    }

    // ======================================================================
    // 观测
    // ======================================================================

    public List<NodeSnapshot> status() {
        List<NodeSnapshot> out = new ArrayList<>();
        for (ReplicaNode node : nodes.values()) {
            out.add(node.snapshot());
        }
        return out;
    }

    public NodeSnapshot status(String nodeId) {
        return node(nodeId).snapshot();
    }

    public String leaderId() {
        ReplicaNode leader = findAliveLeader();
        return leader == null ? null : leader.id();
    }

    // ======================================================================
    // 故障注入 / 生命周期
    // ======================================================================

    public void pause(String nodeId) {
        network.setPaused(nodeId, true);
        logger.info("admin pause node={}", nodeId);
    }

    public void resume(String nodeId, boolean dropPending) {
        if (dropPending) {
            int n = 0;
            for (String other : nodeIds) {
                if (!other.equals(nodeId)) {
                    n += network.dropPending(other, nodeId);
                }
            }
            logger.info("admin resume node={} droppedPending={}", nodeId, n);
        }
        network.setPaused(nodeId, false);
        for (ReplicaNode node : nodes.values()) {
            node.resetProgressFor(nodeId);
        }
        drain();
        logger.info("admin resume node={}", nodeId);
    }

    public void restart(String nodeId) {
        // 先标记进程存活，再由节点重放持久化状态；最后追平领导
        network.setDead(nodeId, false);
        network.setPaused(nodeId, false);
        node(nodeId).restart();
        for (ReplicaNode node : nodes.values()) {
            node.resetProgressFor(nodeId);
        }
        drain();
        logger.info("admin restart node={} drained; commitIndex={}", nodeId, node(nodeId).commitIndex());
    }

    /** 主节点故障：杀掉当前 leader 后，在存活节点中确定性地发起一次选举。 */
    public void failLeader() {
        ReplicaNode leader = null;
        for (ReplicaNode node : nodes.values()) {
            if (node.role() == Role.LEADER) {
                leader = node;
                break;
            }
        }
        if (leader == null) {
            logger.info("admin failLeader: no leader present");
            return;
        }
        network.setDead(leader.id(), true);
        drain();
        electAnyAlive();
        logger.info("admin failLeader killed={} newLeader={}", leader.id(), leaderId());
    }

    /** 隔离 A、B 两组之间的全部链路（组内保持连通）。 */
    public void partition(List<String> sideA, List<String> sideB) {
        validateNodes(sideA);
        validateNodes(sideB);
        for (String a : sideA) {
            for (String b : sideB) {
                network.setLinkDown(a, b, true);
            }
        }
        logger.info("admin partition A={} B={}", sideA, sideB);
    }

    public void healPartition() {
        for (String a : nodeIds) {
            for (String b : nodeIds) {
                if (!a.equals(b)) {
                    network.setLinkDown(a, b, false);
                }
            }
        }
        drain();
        logger.info("admin partition healed and drained, leader={} commitIndex={}",
                leaderId(), leaderCommitIndex());
    }

    private long leaderCommitIndex() {
        ReplicaNode leader = findAliveLeader();
        return leader == null ? -1 : leader.commitIndex();
    }

    /** 手动开始一次选举：向该节点投递一个携带其当前任期的 Timeout。 */
    public void startElection(String nodeId) {
        ReplicaNode n = node(nodeId);
        network.send(new RpcMessage.Timeout("cluster", nodeId, network.nextId(), n.term()));
        drain();
    }

    private void validateNodes(List<String> ids) {
        for (String id : ids) {
            if (!nodes.containsKey(id)) {
                throw new IllegalArgumentException("unknown node: " + id);
            }
        }
    }

    // ======================================================================
    // 网络故障注入
    // ======================================================================

    public void setDropProbability(String from, String to, double probability) {
        network.setDropProbability(from, to, probability);
    }

    public void setOrderMode(String from, String to, Network.OrderMode mode) {
        network.setOrderMode(from, to, mode);
    }

    public void setDuplicate(String from, String to, int extraCopies) {
        network.setDuplicate(from, to, extraCopies);
    }

    public int dropPending(String from, String to) {
        return network.dropPending(from, to);
    }

    public Network network() {
        return network;
    }
}
