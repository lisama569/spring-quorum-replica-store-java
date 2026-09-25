package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.Role;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 主节点故障、重选与故障恢复后的一致性。 */
class LeaderFailoverTest {

    private static final Logger log = LoggerFactory.getLogger(LeaderFailoverTest.class);

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
    void committedDataSurvivesLeaderCrashAndNewLeaderElection() {
        String oldLeader = ClusterTestSupport.electLeader(cluster);
        WriteOutcome w1 = cluster.write("k1", "v1", "failover-1");
        assertTrue(w1.success());
        log.info("committed-before-crash requestId={} term={} commitIndex={}",
                w1.requestId(), w1.term(), w1.commitIndex());

        cluster.crashNode(oldLeader);
        String newLeader = ClusterTestSupport.awaitNewLeader(cluster, oldLeader);
        assertNotEquals(oldLeader, newLeader);
        log.info("leader-changed old={} new={}", oldLeader, newLeader);

        // 故障前已提交的数据必须仍然可读
        ReadOutcome r1 = cluster.read("k1", "failover-read-1");
        assertTrue(r1.success());
        assertEquals("v1", r1.value());

        // 新主可以继续提交
        WriteOutcome w2 = cluster.write("k2", "v2", "failover-2");
        assertTrue(w2.success());
        ReadOutcome r2 = cluster.read("k2", "failover-read-2");
        assertEquals("v2", r2.value());
    }

    @Test
    void restartedOldLeaderConvergesAndCannotOverwrite() {
        String oldLeader = ClusterTestSupport.electLeader(cluster);
        cluster.write("k1", "v1", "conv-1");
        cluster.crashNode(oldLeader);

        String newLeader = ClusterTestSupport.awaitNewLeader(cluster, oldLeader);
        WriteOutcome w2 = cluster.write("k2", "v2", "conv-2");
        assertTrue(w2.success());

        cluster.restartNode(oldLeader);
        // 推进心跳让旧主追上并退为从节点
        for (int i = 0; i < 40; i++) {
            cluster.tick();
        }

        NodeStatus oldStatus = ClusterTestSupport.status(cluster, oldLeader);
        NodeStatus newStatus = ClusterTestSupport.status(cluster, newLeader);
        log.info("converged oldLeader={} role={} term={} commitIndex={} logSize={}",
                oldStatus.nodeId(), oldStatus.role(), oldStatus.currentTerm(),
                oldStatus.commitIndex(), oldStatus.logSize());
        assertEquals(Role.FOLLOWER, oldStatus.role(), "旧主必须退为从节点");
        assertEquals(newStatus.commitIndex(), oldStatus.commitIndex(),
                "旧主重启后提交位置必须追平");
        assertEquals(newStatus.logSize(), oldStatus.logSize(),
                "旧主重启后日志必须追平");

        // 历史已确认读取结果不能被回滚
        ReadOutcome r1 = cluster.read("k1", "conv-read-1");
        ReadOutcome r2 = cluster.read("k2", "conv-read-2");
        assertEquals("v1", r1.value());
        assertEquals("v2", r2.value());

        // 旧主的直接写必须被拒绝，不能覆盖已提交历史
        WriteOutcome stale = cluster.node(oldLeader).clientWrite("k2", "stale", "stale-1");
        log.info("stale-write node={} success={} reason={}", oldLeader, stale.success(),
                stale.reason());
        assertFalse(stale.success());
        assertEquals(RejectionReason.NOT_LEADER, stale.reason());
        ReadOutcome r2Again = cluster.read("k2", "conv-read-3");
        assertEquals("v2", r2Again.value());
    }
}
