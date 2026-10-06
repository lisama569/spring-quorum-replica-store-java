package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.FollowerReplication;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationState;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 主节点视角的复制进度观测：暂停/分区落后与崩溃重启追赶的区分，以及恢复后追平。 */
class ReplicationProgressTest {

    private static final Logger log = LoggerFactory.getLogger(ReplicationProgressTest.class);

    private Cluster cluster;

    @BeforeEach
    void setUp() {
        cluster = ClusterTestSupport.newCluster(3);
    }

    @AfterEach
    void tearDown() {
        cluster.shutdown();
    }

    private FollowerReplication progressOf(ReplicationStatus status, String followerId) {
        return status.followers().stream()
                .filter(f -> f.nodeId().equals(followerId))
                .findFirst()
                .orElseThrow();
    }

    private String firstFollower(String leader) {
        return cluster.status().stream()
                .filter(s -> !s.nodeId().equals(leader))
                .findFirst()
                .orElseThrow()
                .nodeId();
    }

    private void logProgress(String tag, ReplicationStatus status) {
        for (FollowerReplication f : status.followers()) {
            log.info("{} leader={} term={} lastLogIndex={} commitIndex={} follower={} "
                            + "nextIndex={} matchIndex={} lag={} state={}",
                    tag, status.leaderId(), status.term(), status.lastLogIndex(),
                    status.commitIndex(), f.nodeId(), f.nextIndex(), f.matchIndex(),
                    f.lag(), f.state());
        }
    }

    @Test
    void pausedFollowerShowsBlockedThenCatchesUp() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String follower = firstFollower(leader);
        cluster.write("k1", "v1", "rp-1");

        ReplicationStatus synced = cluster.replicationStatus();
        logProgress("assert-synced", synced);
        assertEquals(ReplicationState.IN_SYNC, progressOf(synced, follower).state());
        assertEquals(0, progressOf(synced, follower).lag());

        // 已确认读取结果（之后不得回滚）
        ReadOutcome confirmed = cluster.read("k1", "rp-read-1");
        assertTrue(confirmed.success() && confirmed.found());
        assertEquals(1, confirmed.version());

        // 暂停从副本：主继续写，该副本落后且状态为 BLOCKED
        cluster.pauseNode(follower);
        cluster.write("k2", "v2", "rp-2");
        cluster.write("k3", "v3", "rp-3");
        ReplicationStatus blocked = cluster.replicationStatus();
        logProgress("assert-blocked", blocked);
        FollowerReplication lagging = progressOf(blocked, follower);
        assertEquals(ReplicationState.BLOCKED, lagging.state());
        assertTrue(lagging.lag() > 0, "暂停期间应累积落后量");
        assertEquals(blocked.lastLogIndex() - lagging.matchIndex(), lagging.lag());

        // 恢复后追赶至 IN_SYNC，提交位置与主一致
        cluster.resumeNode(follower);
        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }
        ReplicationStatus caughtUp = cluster.replicationStatus();
        logProgress("assert-caught-up", caughtUp);
        assertEquals(ReplicationState.IN_SYNC, progressOf(caughtUp, follower).state());
        assertEquals(0, progressOf(caughtUp, follower).lag());
        assertEquals(ClusterTestSupport.status(cluster, leader).commitIndex(),
                ClusterTestSupport.status(cluster, follower).commitIndex(),
                "追平后从副本提交位置应与主一致");

        // 已确认读取不回滚
        ReadOutcome after = cluster.read("k1", "rp-read-2");
        assertEquals("v1", after.value());
        assertEquals(confirmed.version(), after.version());
    }

    @Test
    void crashedFollowerShowsDownThenCatchingUp() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String follower = firstFollower(leader);
        cluster.write("k1", "v1", "rc-1");
        ReadOutcome confirmed = cluster.read("k1", "rc-read-1");
        assertTrue(confirmed.success());

        // 崩溃：状态为 DOWN
        cluster.crashNode(follower);
        ReplicationStatus down = cluster.replicationStatus();
        logProgress("assert-down", down);
        assertEquals(ReplicationState.DOWN, progressOf(down, follower).state());

        // 崩溃期间主继续提交新条目
        cluster.write("k2", "v2", "rc-2");
        cluster.write("k3", "v3", "rc-3");

        // 重启后尚未收到新心跳：可达但落后 → CATCHING_UP（区别于 BLOCKED）
        cluster.restartNode(follower);
        ReplicationStatus catching = cluster.replicationStatus();
        logProgress("assert-catching-up", catching);
        FollowerReplication behind = progressOf(catching, follower);
        assertEquals(ReplicationState.CATCHING_UP, behind.state());
        assertTrue(behind.lag() > 0);

        // 心跳后追平：进度 IN_SYNC，提交位置与已提交内容一致
        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }
        ReplicationStatus synced = cluster.replicationStatus();
        logProgress("assert-resynced", synced);
        assertEquals(ReplicationState.IN_SYNC, progressOf(synced, follower).state());
        assertEquals(ClusterTestSupport.status(cluster, leader).commitIndex(),
                ClusterTestSupport.status(cluster, follower).commitIndex());

        // 已确认读取不回滚，崩溃期间提交的数据也可读
        assertEquals("v1", cluster.read("k1", "rc-read-2").value());
        assertEquals("v3", cluster.read("k3", "rc-read-3").value());
    }
}
