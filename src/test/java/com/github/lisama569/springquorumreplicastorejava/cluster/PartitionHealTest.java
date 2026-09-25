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

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 网络分区：少数派不可提交、多数派继续服务、恢复后收敛且读不回滚。 */
class PartitionHealTest {

    private static final Logger log = LoggerFactory.getLogger(PartitionHealTest.class);

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
    void partitionedMinorityCannotCommitAndHealConverges() {
        String leader = ClusterTestSupport.electLeader(cluster);
        WriteOutcome base = cluster.write("p1", "committed-before", "pt-1");
        assertTrue(base.success());

        // 把主节点单独分区（少数派：1 vs 2）
        cluster.partition(Set.of(leader), otherNodes(leader));

        // 少数派主节点无法形成多数派，写不得在前台成功
        WriteOutcome rejected = cluster.write("p2", "should-not-commit", "pt-2");
        log.info("minority-write requestId={} success={} reason={} term={} commitIndex={}",
                rejected.requestId(), rejected.success(), rejected.reason(),
                rejected.term(), rejected.commitIndex());
        assertTrue(!rejected.success());
        assertEquals(RejectionReason.NO_QUORUM, rejected.reason());

        // 读同样无法获得多数派确认
        ReadOutcome readMinority = cluster.read("p1", "pt-read-minority");
        if (!readMinority.success()) {
            log.info("minority-read reason={}", readMinority.reason());
            assertEquals(RejectionReason.NO_QUORUM, readMinority.reason());
        }

        // 多数派一侧选出新主并继续提交
        for (int i = 0; i < 40; i++) {
            cluster.tick();
        }
        String newLeader = cluster.leaderId();
        assertNotEquals(leader, newLeader, "新主必须来自多数派一侧");
        log.info("majority elected new leader={} (old partitioned leader={})", newLeader, leader);

        WriteOutcome majorityWrite = cluster.write("p3", "committed-during", "pt-3");
        assertTrue(majorityWrite.success(), "多数派一侧写入应成功");

        // 恢复分区
        cluster.healPartition();
        for (int i = 0; i < 40; i++) {
            cluster.tick();
        }

        // 旧主退为从节点，所有节点日志与提交位置一致
        NodeStatus oldStatus = ClusterTestSupport.status(cluster, leader);
        assertEquals(Role.FOLLOWER, oldStatus.role());
        long finalCommit = ClusterTestSupport.status(cluster, newLeader).commitIndex();
        for (NodeStatus status : cluster.status()) {
            log.info("healed node={} role={} term={} commitIndex={} logSize={}",
                    status.nodeId(), status.role(), status.currentTerm(),
                    status.commitIndex(), status.logSize());
            assertEquals(finalCommit, status.commitIndex(), status.nodeId());
        }

        // 历史已确认读取不回滚，分区期间多数派新数据保留，少数派未提交写入不可见
        assertEquals("committed-before", cluster.read("p1", "pt-read-1").value());
        assertEquals("committed-during", cluster.read("p3", "pt-read-3").value());
        ReadOutcome shouldBeAbsent = cluster.read("p2", "pt-read-2");
        log.info("minority-uncommitted visible={} value={}", shouldBeAbsent.success(),
                shouldBeAbsent.value());
        assertTrue(shouldBeAbsent.success() && shouldBeAbsent.value() == null,
                "未达多数派的写恢复后不得可见");
    }

    @Test
    void briefUnavailableNodeCatchesUpAfterResume() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String victim = cluster.status().stream()
                .map(NodeStatus::nodeId)
                .filter(id -> !id.equals(leader))
                .findFirst().orElseThrow();

        cluster.pauseNode(victim);
        for (int i = 1; i <= 3; i++) {
            WriteOutcome outcome = cluster.write("u" + i, String.valueOf(i), "pu-" + i);
            assertTrue(outcome.success(), "暂停一个节点不影响多数派写入");
        }
        cluster.resumeNode(victim);
        for (int i = 0; i < 20; i++) {
            cluster.tick();
        }

        NodeStatus leaderStatus = ClusterTestSupport.status(cluster, leader);
        NodeStatus victimStatus = ClusterTestSupport.status(cluster, victim);
        assertEquals(leaderStatus.commitIndex(), victimStatus.commitIndex());
        for (int i = 1; i <= 3; i++) {
            assertEquals(String.valueOf(i),
                    cluster.read("u" + i, "pu-read-" + i).value());
        }
    }

    private Set<String> otherNodes(String excluded) {
        return cluster.status().stream()
                .map(NodeStatus::nodeId)
                .filter(id -> !id.equals(excluded))
                .collect(java.util.stream.Collectors.toSet());
    }
}
