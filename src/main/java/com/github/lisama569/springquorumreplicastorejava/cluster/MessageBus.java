package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.rpc.Envelope;
import com.github.lisama569.springquorumreplicastorejava.rpc.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 单进程内消息总线：负责节点间投递，并支持受控故障注入
 * （暂停节点、分区、消息重复、乱序/延迟）。
 *
 * <p>投递是同步的：发送方调用栈内直接触发接收方回调，
 * 由 {@link Cluster} 的全局锁保证串行化。</p>
 */
public class MessageBus {

    private final Map<String, Consumer<Envelope>> handlers = new ConcurrentHashMap<>();
    private final Set<String> paused = ConcurrentHashMap.newKeySet();
    private final Set<String> held = ConcurrentHashMap.newKeySet();
    private final Map<String, List<Envelope>> heldMessages = new ConcurrentHashMap<>();
    private final List<String> deliveryLog = new CopyOnWriteArrayList<>();
    private final AtomicLong seq = new AtomicLong();
    private final Random random;

    private volatile Set<String> partitionA = Set.of();
    private volatile Set<String> partitionB = Set.of();
    private volatile double duplicateRate = 0.0;

    public MessageBus() {
        this(new Random(0));
    }

    public MessageBus(Random random) {
        this.random = random;
    }

    /** 注册节点接收回调。 */
    public void register(String nodeId, Consumer<Envelope> handler) {
        handlers.put(nodeId, handler);
    }

    /** 注销节点（模拟崩溃）。 */
    public void unregister(String nodeId) {
        handlers.remove(nodeId);
    }

    /** 发送一条消息（可能被注入的故障延迟、复制或丢弃）。 */
    public void send(String from, String to, Message message) {
        Envelope envelope = new Envelope(from, to, message, seq.incrementAndGet());
        if (paused.contains(from) || paused.contains(to)) {
            log(envelope, "DROP paused");
            return;
        }
        if (isPartitioned(from, to)) {
            log(envelope, "DROP partition");
            return;
        }
        if (held.contains(to)) {
            heldMessages.computeIfAbsent(to, k -> new ArrayList<>()).add(envelope);
            log(envelope, "HOLD");
            return;
        }
        deliver(envelope, "DELIVER");
        if (duplicateRate > 0 && random.nextDouble() < duplicateRate) {
            deliver(envelope, "DELIVER duplicate");
        }
    }

    private void deliver(Envelope envelope, String action) {
        Consumer<Envelope> handler = handlers.get(envelope.to());
        if (handler == null) {
            log(envelope, "DROP no-handler(crashed)");
            return;
        }
        log(envelope, action);
        handler.accept(envelope);
    }

    private boolean isPartitioned(String from, String to) {
        return (partitionA.contains(from) && partitionB.contains(to))
                || (partitionB.contains(from) && partitionA.contains(to));
    }

    private void log(Envelope envelope, String action) {
        String detail = action;
        if (envelope.message() instanceof Message.AppendEntriesResponse response
                && !response.success()) {
            detail = action + " rejected=" + response.reason();
        }
        deliveryLog.add("seq=" + envelope.seq() + " " + detail + " "
                + envelope.from() + "->" + envelope.to() + " " + envelope.message());
    }

    /** 暂停节点：丢弃其收发消息。 */
    public void pause(String nodeId) {
        paused.add(nodeId);
    }

    /** 恢复被暂停的节点。 */
    public void resume(String nodeId) {
        paused.remove(nodeId);
    }

    /** 设置分区：两组节点之间消息互不可达。 */
    public void partition(Set<String> groupA, Set<String> groupB) {
        partitionA = Set.copyOf(groupA);
        partitionB = Set.copyOf(groupB);
    }

    /** 解除分区。 */
    public void healPartition() {
        partitionA = Set.of();
        partitionB = Set.of();
    }

    /** 设置消息重复投递概率（0..1）。 */
    public void setDuplicateRate(double rate) {
        this.duplicateRate = rate;
    }

    /** 暂存发往某节点的消息（用于乱序注入），不投递。 */
    public void hold(String nodeId) {
        held.add(nodeId);
    }

    /** 释放并逆序投递暂存的消息，模拟乱序到达。 */
    public void releaseReversed(String nodeId) {
        held.remove(nodeId);
        List<Envelope> pending = heldMessages.remove(nodeId);
        if (pending == null) {
            return;
        }
        Collections.reverse(pending);
        for (Envelope envelope : pending) {
            deliver(envelope, "DELIVER released-reversed");
        }
    }

    /** 当前被暂停的节点集合（只读视图）。 */
    public Set<String> pausedNodes() {
        return Set.copyOf(paused);
    }

    /** from 当前能否把消息投递到 to（双方未暂停且不在分区两侧）。 */
    public boolean canReach(String from, String to) {
        return !paused.contains(from) && !paused.contains(to) && !isPartitioned(from, to);
    }

    /** 该节点当前能否与多数派节点互通（自身计入）。 */
    public boolean isReachableFromQuorum(String nodeId) {
        int total = handlers.size();
        int quorum = total / 2 + 1;
        int reachable = 0;
        for (String other : handlers.keySet()) {
            if (paused.contains(nodeId) || paused.contains(other)) {
                continue;
            }
            if (!isPartitioned(nodeId, other)) {
                reachable++;
            }
        }
        return reachable >= quorum;
    }

    /** 已投递/丢弃的消息日志（用于测试断言与解释）。 */
    public List<String> deliveryLog() {
        return List.copyOf(deliveryLog);
    }
}
