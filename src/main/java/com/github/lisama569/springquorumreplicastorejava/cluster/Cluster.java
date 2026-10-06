package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;
import com.github.lisama569.springquorumreplicastorejava.model.FollowerProgress;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationProgress;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationState;
import com.github.lisama569.springquorumreplicastorejava.model.Role;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOp;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import com.github.lisama569.springquorumreplicastorejava.rpc.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 集群门面：持有全部副本与消息总线，驱动时钟，
 * 对外提供客户端读写与故障注入入口。
 *
 * <p>所有节点状态变更都在 {@link #lock} 内串行执行，
 * 因此同步消息投递不会引入并发问题。</p>
 */
public class Cluster {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final Object lock = new Object();
    private final MessageBus bus = new MessageBus();
    private final Map<String, ReplicaNode> nodes = new LinkedHashMap<>();
    private final Map<String, Consumer<Envelope>> handlers = new LinkedHashMap<>();
    private final long tickMillis;
    private ScheduledExecutorService scheduler;

    public Cluster(int nodeCount) {
        this(nodeCount, 50);
    }

    public Cluster(int nodeCount, long tickMillis) {
        this.tickMillis = tickMillis;
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            ids.add("n" + i);
        }
        for (String id : ids) {
            ReplicaNode node = new ReplicaNode(id, ids, bus);
            nodes.put(id, node);
            handlers.put(id, envelope -> {
                synchronized (lock) {
                    node.onMessage(envelope);
                }
            });
            bus.register(id, handlers.get(id));
        }
    }

    /** 启动调度（选举与心跳）。 */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-ticker");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::tickSafely, tickMillis, tickMillis, TimeUnit.MILLISECONDS);
    }

    /** 停止调度。 */
    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private void tickSafely() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("tick failed: {}", e.toString());
        }
    }

    /** 推进所有节点一个时钟格（测试可手动驱动，保证确定性）。 */
    public void tick() {
        synchronized (lock) {
            for (ReplicaNode node : nodes.values()) {
                node.tick();
            }
        }
    }

    /** 客户端写（旧签名：无条件 PUT）：路由到当前主节点。 */
    public WriteOutcome write(String key, String value, String requestId) {
        return write(WriteOp.PUT, key, value, requestId, null, null);
    }

    /**
     * 客户端写：路由到当前主节点。
     *
     * @param op              PUT 或 DELETE
     * @param expectedVersion 期望的当前版本；null 表示无条件写入
     * @param clientTerm      调用方缓存的任期；null 表示不校验
     */
    public WriteOutcome write(WriteOp op, String key, String value, String requestId,
                              Long expectedVersion, Long clientTerm) {
        synchronized (lock) {
            ReplicaNode leader = findLeader();
            if (leader == null) {
                log.info("event=write-rejected requestId={} reason=no-leader", requestId);
                return WriteOutcome.rejected(requestId, -1, -1, 0,
                        RejectionReason.NO_QUORUM, "no leader elected");
            }
            return leader.clientWrite(op, key, value, requestId, expectedVersion, clientTerm);
        }
    }

    /** 客户端删除：与写入走同一条受版本约束的路径。 */
    public WriteOutcome delete(String key, String requestId,
                               Long expectedVersion, Long clientTerm) {
        return write(WriteOp.DELETE, key, null, requestId, expectedVersion, clientTerm);
    }

    /** 客户端读：路由到当前主节点。 */
    public ReadOutcome read(String key, String requestId) {
        synchronized (lock) {
            ReplicaNode leader = findLeader();
            if (leader == null) {
                log.info("event=read-rejected requestId={} reason=no-leader", requestId);
                return ReadOutcome.rejected(key, -1, -1,
                        RejectionReason.NO_QUORUM, "no leader elected");
            }
            return leader.clientRead(key, requestId);
        }
    }

    private ReplicaNode findLeader() {
        for (ReplicaNode node : nodes.values()) {
            NodeStatus status = node.status();
            if (status.alive() && status.role() == Role.LEADER
                    && !bus.pausedNodes().contains(status.nodeId())
                    && bus.isReachableFromQuorum(status.nodeId())) {
                return node;
            }
        }
        return null;
    }

    /** 当前主节点 id，无多数派主时返回 null。 */
    public String leaderId() {
        synchronized (lock) {
            ReplicaNode leader = findLeader();
            return leader == null ? null : leader.id();
        }
    }

    /** 所有副本运行态。 */
    public List<NodeStatus> status() {
        synchronized (lock) {
            Set<String> paused = bus.pausedNodes();
            List<NodeStatus> result = new ArrayList<>();
            for (ReplicaNode node : nodes.values()) {
                NodeStatus s = node.status();
                result.add(new NodeStatus(s.nodeId(), s.role(), s.currentTerm(), s.commitIndex(),
                        s.lastApplied(), s.logSize(), paused.contains(s.nodeId()), s.alive()));
            }
            return result;
        }
    }

    /** 主节点视角的复制进度快照；无主时返回空快照。 */
    public ReplicationProgress replicationProgress() {
        synchronized (lock) {
            ReplicaNode leader = findLeader();
            if (leader == null) {
                return ReplicationProgress.noLeader();
            }
            NodeStatus leaderStatus = leader.status();
            long leaderLastLog = leaderStatus.logSize();
            Set<String> paused = bus.pausedNodes();
            List<FollowerProgress> followers = new ArrayList<>();
            for (Map.Entry<String, long[]> e : leader.replicationView().entrySet()) {
                String followerId = e.getKey();
                long sent = e.getValue()[0];
                long matched = e.getValue()[1];
                long lag = Math.max(0, leaderLastLog - matched);
                followers.add(new FollowerProgress(followerId, sent, matched, lag,
                        progressState(followerId, leader.id(), lag, paused)));
            }
            return new ReplicationProgress(leader.id(), leaderStatus.currentTerm(),
                    leaderLastLog, leaderStatus.commitIndex(), followers);
        }
    }

    private ReplicationState progressState(String followerId, String leaderId, long lag,
                                           Set<String> paused) {
        if (lag == 0) {
            return ReplicationState.IN_SYNC;
        }
        if (!nodes.get(followerId).status().alive()) {
            return ReplicationState.DOWN;
        }
        if (paused.contains(followerId) || !bus.canReach(leaderId, followerId)) {
            return ReplicationState.BLOCKED;
        }
        return ReplicationState.CATCHING_UP;
    }

    /** 暂停节点（消息层面隔离，不修改节点状态）。 */
    public void pauseNode(String nodeId) {
        bus.pause(nodeId);
    }

    /** 恢复节点。 */
    public void resumeNode(String nodeId) {
        bus.resume(nodeId);
    }

    /** 崩溃节点（保留持久状态）。 */
    public void crashNode(String nodeId) {
        synchronized (lock) {
            bus.unregister(nodeId);
            nodes.get(nodeId).crash();
        }
    }

    /** 重启节点。 */
    public void restartNode(String nodeId) {
        synchronized (lock) {
            nodes.get(nodeId).restart();
            bus.register(nodeId, handlers.get(nodeId));
        }
    }

    /** 设置分区。 */
    public void partition(Set<String> groupA, Set<String> groupB) {
        bus.partition(groupA, groupB);
    }

    /** 解除分区。 */
    public void healPartition() {
        bus.healPartition();
    }

    /** 消息总线（测试与故障注入用）。 */
    public MessageBus bus() {
        return bus;
    }

    /** 按 id 获取节点（测试用）。 */
    ReplicaNode node(String nodeId) {
        return nodes.get(nodeId);
    }
}
