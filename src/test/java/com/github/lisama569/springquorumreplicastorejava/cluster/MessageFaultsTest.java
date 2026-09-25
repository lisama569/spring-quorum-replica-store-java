package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 消息重复投递与乱序到达场景。 */
class MessageFaultsTest {

    private static final Logger log = LoggerFactory.getLogger(MessageFaultsTest.class);

    private Cluster cluster;

    @BeforeEach
    void setUp() {
        cluster = ClusterTestSupport.newCluster(3);
    }

    @AfterEach
    void tearDown() {
        cluster.shutdown();
    }

    @Test
    void duplicatedMessagesDoNotDuplicateState() {
        // 选举与复制期间每条消息都投递两次
        cluster.bus().setDuplicateRate(1.0);
        String leader = ClusterTestSupport.electLeader(cluster);

        for (int i = 1; i <= 5; i++) {
            WriteOutcome outcome = cluster.write("dk" + i, "v" + i, "dup-" + i);
            assertTrue(outcome.success(), "write " + i + " should commit");
        }
        cluster.bus().setDuplicateRate(0.0);
        for (int i = 0; i < 10; i++) {
            cluster.tick();
        }

        NodeStatus leaderStatus = ClusterTestSupport.status(cluster, leader);
        for (NodeStatus status : cluster.status()) {
            log.info("dup-state node={} logSize={} commitIndex={}",
                    status.nodeId(), status.logSize(), status.commitIndex());
            assertEquals(leaderStatus.logSize(), status.logSize(),
                    "重复投递不得产生重复日志: " + status.nodeId());
            assertEquals(leaderStatus.commitIndex(), status.commitIndex());
        }
        for (int i = 1; i <= 5; i++) {
            ReadOutcome read = cluster.read("dk" + i, "dup-read-" + i);
            assertEquals("v" + i, read.value());
        }
    }

    @Test
    void duplicateClientRequestIsIdempotent() {
        String leader = ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = cluster.write("ik", "iv", "idem-1");
        assertTrue(first.success());
        long commitIndex = first.commitIndex();

        WriteOutcome second = cluster.write("ik", "iv", "idem-1");
        log.info("idempotent second-call requestId={} success={} reason={} term={} commitIndex={}",
                second.requestId(), second.success(), second.reason(),
                second.term(), second.commitIndex());
        assertTrue(second.success());
        assertEquals(RejectionReason.DUPLICATE_REQUEST, second.reason());
        assertEquals(commitIndex, second.commitIndex(), "重复请求不得推进提交位置");

        // 状态未重复：日志没有因第二次调用增长
        long logSizeAfterDup = ClusterTestSupport.status(cluster, leader).logSize();
        assertEquals(commitIndex, logSizeAfterDup);

        // 后续新请求仍然正常
        WriteOutcome third = cluster.write("ik2", "iv2", "idem-2");
        assertTrue(third.success());
        assertEquals(RejectionReason.NONE, third.reason());
    }

    @Test
    void outOfOrderMessagesConverge() {
        String leader = ClusterTestSupport.electLeader(cluster);
        for (int i = 0; i < 3; i++) {
            cluster.tick();
        }
        String follower = cluster.status().stream()
                .map(NodeStatus::nodeId)
                .filter(id -> !id.equals(leader))
                .findFirst().orElseThrow();

        // 1) 让从节点连续错过 index=2,3（消息被丢弃，形成空洞）
        cluster.pauseNode(follower);
        WriteOutcome w1 = cluster.write("ok1", "ov1", "ord-1");
        WriteOutcome w2 = cluster.write("ok2", "ov2", "ord-2");
        assertTrue(w1.success());
        assertTrue(w2.success());
        cluster.resumeNode(follower);

        long term = ClusterTestSupport.status(cluster, leader).currentTerm();
        var leaderLog = cluster.node(leader).logSnapshot();
        assertEquals(3, leaderLog.size());

        // 2) 模拟乱序：携带 index=3 的包先于 index=2 到达，prevLog 检查必须拒绝
        var outOfOrder = new com.github.lisama569.springquorumreplicastorejava.rpc.Message
                .AppendEntriesRequest(leader, term, 2, leaderLog.get(1).term(),
                List.of(leaderLog.get(2)), 3);
        int logPosition = cluster.bus().deliveryLog().size();
        cluster.node(follower).onMessage(new com.github.lisama569.springquorumreplicastorejava.rpc
                .Envelope(leader, follower, outOfOrder, 9001));

        boolean conflictSeen = cluster.bus().deliveryLog().subList(logPosition,
                        cluster.bus().deliveryLog().size()).stream()
                .anyMatch(line -> line.contains("LOG_CONFLICT"));
        log.info("out-of-order packet rejected with conflict: {}", conflictSeen);
        assertTrue(conflictSeen, "乱序空洞必须产生可解释的 LOG_CONFLICT 判定");

        // 3) 冲突响应已同步驱动主节点回退 nextIndex 并补传完整后缀
        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }

        NodeStatus leaderStatus = ClusterTestSupport.status(cluster, leader);
        NodeStatus followerStatus = ClusterTestSupport.status(cluster, follower);
        log.info("converge node={} logSize={} commitIndex={}; node={} logSize={} commitIndex={}",
                leader, leaderStatus.logSize(), leaderStatus.commitIndex(),
                follower, followerStatus.logSize(), followerStatus.commitIndex());
        assertEquals(leaderStatus.logSize(), followerStatus.logSize());
        assertEquals(leaderStatus.commitIndex(), followerStatus.commitIndex());

        assertEquals("ov1", cluster.read("ok1", "ord-read-1").value());
        assertEquals("ov2", cluster.read("ok2", "ord-read-2").value());
    }

}
