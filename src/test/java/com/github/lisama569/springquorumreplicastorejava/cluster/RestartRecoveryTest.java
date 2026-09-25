package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 节点崩溃/重启后的提交数据一致性。 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

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
    void followerRestartCatchesUpOnMissedCommits() {
        String leader = ClusterTestSupport.electLeader(cluster);
        cluster.write("r1", "a", "rr-1");
        String follower = cluster.status().stream()
                .map(NodeStatus::nodeId)
                .filter(id -> !id.equals(leader))
                .findFirst().orElseThrow();

        cluster.crashNode(follower);
        for (int i = 0; i < 5; i++) {
            cluster.write("gap" + i, "g" + i, "rr-gap-" + i);
        }
        NodeStatus down = ClusterTestSupport.status(cluster, follower);
        NodeStatus live = ClusterTestSupport.status(cluster, leader);
        assertTrue(down.commitIndex() < live.commitIndex());
        log.info("before-restart crashed={} commitIndex={} live={} commitIndex={}",
                follower, down.commitIndex(), leader, live.commitIndex());

        cluster.restartNode(follower);
        for (int i = 0; i < 30; i++) {
            cluster.tick();
        }

        NodeStatus caughtUp = ClusterTestSupport.status(cluster, follower);
        log.info("after-restart node={} term={} commitIndex={} logSize={}",
                follower, caughtUp.currentTerm(), caughtUp.commitIndex(), caughtUp.logSize());
        assertEquals(live.commitIndex(), caughtUp.commitIndex());
        assertEquals(live.logSize(), caughtUp.logSize());
        assertEquals("a", cluster.read("r1", "rr-read-1").value());
        assertEquals("g4", cluster.read("gap4", "rr-read-2").value());
    }

    @Test
    void leaderRestartAfterMultipleTermsKeepsAllCommittedData() {
        String leader1 = ClusterTestSupport.electLeader(cluster);
        cluster.write("a", "1", "rt-1");
        cluster.crashNode(leader1);

        String leader2 = ClusterTestSupport.awaitNewLeader(cluster, leader1);
        cluster.write("b", "2", "rt-2");

        // 先恢复旧主并让其追平，再宕第二任主，保证存活多数派仍可选举
        cluster.restartNode(leader1);
        for (int i = 0; i < 30; i++) {
            cluster.tick();
        }
        cluster.crashNode(leader2);
        String leader3 = ClusterTestSupport.awaitNewLeader(cluster, leader2);
        cluster.write("c", "3", "rt-3");

        // 重启第二任旧主，它必须以从节点身份追上更高任期
        cluster.restartNode(leader2);
        for (int i = 0; i < 40; i++) {
            cluster.tick();
        }

        long finalCommit = ClusterTestSupport.status(cluster, leader3).commitIndex();
        for (NodeStatus status : cluster.status()) {
            log.info("final node={} term={} commitIndex={} logSize={}",
                    status.nodeId(), status.currentTerm(), status.commitIndex(), status.logSize());
            assertEquals(finalCommit, status.commitIndex(), status.nodeId() + " commitIndex");
        }
        assertEquals("1", cluster.read("a", "rt-r-a").value());
        assertEquals("2", cluster.read("b", "rt-r-b").value());
        assertEquals("3", cluster.read("c", "rt-r-c").value());

        // 再宕第三任主：两个曾担任过主的节点构成多数派，选出新主且历史不丢
        cluster.crashNode(leader3);
        String leader4 = ClusterTestSupport.awaitNewLeader(cluster, leader3);
        log.info("leader-4={} elected among restarted peers", leader4);
        assertEquals("1", cluster.read("a", "rt-r-a2").value());
        WriteOutcome more = cluster.write("d", "4", "rt-4");
        assertTrue(more.success());
        assertEquals("4", cluster.read("d", "rt-r-d").value());
    }
}
