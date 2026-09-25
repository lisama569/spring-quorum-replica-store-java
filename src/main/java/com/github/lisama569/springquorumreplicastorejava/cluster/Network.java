package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

/**
 * 单进程确定性消息网络。
 *
 * <ul>
 *   <li>每条有向链路 (from,to) 维护独立队列，所有消息先入队；</li>
 *   <li>{@link #pump()} 每次按固定顺序投递一条当前可投递的消息，使执行过程可重复；</li>
 *   <li>暂停的节点：发给它的消息滞留，恢复后可继续投递或丢弃；</li>
 *   <li>死亡（停机）的节点：发给它的消息立即丢弃（进程已不在，重启也看不到在途消息）；</li>
 *   <li>链路断开（分区）：该有向链路上的滞留消息视为黑洞丢弃，后续发送也被拒绝入队；</li>
 *   <li>可配置按概率丢弃、LIFO 乱序、重复副本注入。</li>
 * </ul>
 */
public class Network {

    private static final Logger log = LoggerFactory.getLogger(Network.class);

    public enum OrderMode {
        /** 先入先出（默认）。 */
        FIFO,
        /** 后入先出，用于确定性地制造乱序。 */
        LIFO
    }

    private static final class Envelope {
        final RpcMessage message;

        Envelope(RpcMessage message) {
            this.message = message;
        }
    }

    private static final class Link {
        final String from;
        final String to;
        final Deque<Envelope> queue = new ArrayDeque<>();
        double dropProbability;
        OrderMode orderMode = OrderMode.FIFO;
        int extraCopies;

        Link(String from, String to) {
            this.from = from;
            this.to = to;
        }
    }

    private final Map<String, Link> links = new LinkedHashMap<>();
    private final Set<String> registered = new HashSet<>();
    private final Set<String> paused = new HashSet<>();
    private final Set<String> dead = new HashSet<>();
    /** 已断开的无向链路。 */
    private final Set<String> downLinks = new HashSet<>();

    private long messageIdSequence;
    private final Random random = new Random(42);
    private long droppedCount;

    private static String directedKey(String from, String to) {
        return from + "->" + to;
    }

    private static String undirectedKey(String a, String b) {
        return a.compareTo(b) <= 0 ? a + "::" + b : b + "::" + a;
    }

    private Link link(String from, String to) {
        return links.computeIfAbsent(directedKey(from, to), k -> new Link(from, to));
    }

    public void register(String nodeId) {
        registered.add(nodeId);
    }

    public long nextId() {
        return ++messageIdSequence;
    }

    public void send(RpcMessage message) {
        // 断链期间消息滞留在链路队列中：分区内节点照常通信；
        // 恢复后滞留的跨分区消息由 Raft 的 term/日志一致性检查拒绝，不会污染状态。
        Link link = link(message.from(), message.to());
        link.queue.addLast(new Envelope(message));
        for (int i = 0; i < link.extraCopies; i++) {
            link.queue.addLast(new Envelope(message));
        }
    }

    /**
     * 投递一条当前可投递的消息：跳过发给死亡/暂停节点的滞留队列，
     * 在队列内应用丢掷概率，最终把消息从队列移除并返回。
     *
     * @return 实际投递给接收方的消息；没有任何可投递消息时返回 null
     */
    public RpcMessage pump() {
        for (Link link : links.values()) {
            if (link.queue.isEmpty()) {
                continue;
            }
            // 分区中的链路：消息滞留，不投递（也不丢弃）
            if (downLinks.contains(undirectedKey(link.from, link.to))) {
                continue;
            }
            String recipient = link.to;
            boolean recipientAlive = "client".equals(recipient)
                    || (!dead.contains(recipient) && registered.contains(recipient));
            if (!recipientAlive) {
                droppedCount += link.queue.size();
                link.queue.clear();
                continue;
            }
            if (paused.contains(recipient)) {
                continue;
            }
            Envelope envelope = link.orderMode == OrderMode.LIFO
                    ? link.queue.pollLast() : link.queue.pollFirst();
            if (envelope == null) {
                continue;
            }
            if (random.nextDouble(1.0d) < link.dropProbability) {
                droppedCount++;
                log.info("network drop messageId={} {}->{} type={}",
                        envelope.message.messageId(), link.from, link.to,
                        envelope.message.getClass().getSimpleName());
                continue;
            }
            return envelope.message;
        }
        return null;
    }

    /** 持续投递直到不存在任何可投递消息。 */
    public void drain() {
        int guard = 0;
        while (pump() != null) {
            if (++guard > 1_000_000) {
                throw new IllegalStateException("network drain exceeded guard (possible livelock)");
            }
        }
    }

    public void setLinkDown(String a, String b, boolean down) {
        String key = undirectedKey(a, b);
        if (down) {
            downLinks.add(key);
            log.info("partition link {}<->{} down (in-flight messages held)", a, b);
        } else {
            downLinks.remove(key);
            log.info("partition link {}<->{} healed", a, b);
        }
    }

    public boolean isLinkDown(String a, String b) {
        return downLinks.contains(undirectedKey(a, b));
    }

    public void setPaused(String nodeId, boolean value) {
        if (value) {
            paused.add(nodeId);
        } else {
            paused.remove(nodeId);
        }
        log.info("node {} paused={}", nodeId, value);
    }

    public boolean isPaused(String nodeId) {
        return paused.contains(nodeId);
    }

    public void setDead(String nodeId, boolean value) {
        if (value) {
            dead.add(nodeId);
            paused.remove(nodeId);
            int dropped = 0;
            for (Link link : links.values()) {
                if (nodeId.equals(link.from) || nodeId.equals(link.to)) {
                    dropped += link.queue.size();
                    link.queue.clear();
                }
            }
            log.info("node {} is DEAD, cleared {} in-flight messages", nodeId, dropped);
        } else {
            dead.remove(nodeId);
            log.info("node {} alive again", nodeId);
        }
    }

    public boolean isDead(String nodeId) {
        return dead.contains(nodeId);
    }

    public void setDropProbability(String from, String to, double probability) {
        link(from, to).dropProbability = probability;
    }

    public void setOrderMode(String from, String to, OrderMode mode) {
        link(from, to).orderMode = mode;
    }

    /** 后续发送到该链路的每条消息自动多产生 extraCopies 份副本（重复投递注入）。 */
    public void setDuplicate(String from, String to, int extraCopies) {
        link(from, to).extraCopies = Math.max(0, extraCopies);
    }

    public int dropPending(String from, String to) {
        Link a = links.get(directedKey(from, to));
        Link b = links.get(directedKey(to, from));
        int n = 0;
        if (a != null) {
            n += a.queue.size();
            a.queue.clear();
        }
        if (b != null) {
            n += b.queue.size();
            b.queue.clear();
        }
        droppedCount += n;
        return n;
    }

    public int pendingCount(String from, String to) {
        Link link = links.get(directedKey(from, to));
        return link == null ? 0 : link.queue.size();
    }

    public List<String> pendingLinks() {
        List<String> result = new ArrayList<>();
        for (Link link : links.values()) {
            if (!link.queue.isEmpty()) {
                result.add(link.from + "->" + link.to + ":" + link.queue.size());
            }
        }
        return result;
    }

    public long droppedCount() {
        return droppedCount;
    }
}
