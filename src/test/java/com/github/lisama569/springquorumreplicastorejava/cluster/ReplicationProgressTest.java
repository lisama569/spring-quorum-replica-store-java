package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.FollowerProgress;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationProgress;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationState;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 主节点视角的复制进度观测：落后原因区分与恢复后追赶。 */
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

    private FollowerProgress progressOf(ReplicationProgress progress, String followerId) {
        FollowerProgress fp = progress.followers().stream()
                .filter(f -> f.nodeId().equals(followerId))
                .findFirst()
                .orElseThrow();
        log.info("assert-progress leader={} term={} leaderLastLog={} commitIndex={}"
                        + " follower={} sent={} matched={} lag={} state={}",
                progress.leaderId(), progress.term(), progress.leaderLastLogIndex(),
                progress.commitIndex(), fp.nodeId(), fp.sentIndex(), fp.matchIndex(),
                fp.lag(), fp.state());
        return fp;
    }

    private String firstFollower(String leader) {
        return cluster.status().stream()
                .map(s -> s.nodeId())
                .filter(id -> !id.equals(leader))
                .findFirst()
                .orElseThrow();
    }

    private void writeAndLog(String key, String value, String requestId) {
        WriteOutcome outcome = cluster.write(key, value, requestId);
        log.info("assert-write requestId={} success={} version={} commitIndex={} reason={}",
                requestId, outcome.success(), outcome.version(), outcome.commitIndex(),
                outcome.reason());
        assertTrue(outcome.success());
    }

    @Test
    void progressIsVisibleAndInSyncAfterWrites() {
        String leader = ClusterTestSupport.electLeader(cluster);
        writeAndLog("r1", "v1", "rp-1");
        writeAndLog("r2", "v2", "rp-2");

        ReplicationProgress progress = cluster.replicationProgress();
        assertEquals(leader, progress.leaderId());
        assertEquals(2, progress.followers().size());
        for (FollowerProgress fp : progress.followers()) {
            progressOf(progress, fp.nodeId());
            assertEquals(ReplicationState.IN_SYNC, fp.state());
            assertEquals(0, fp.lag());
            assertEquals(progress.leaderLastLogIndex(), fp.matchIndex());
        }
    }

    @Test
    void pausedFollowerIsReportedBlockedThenCatchesUp() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String victim = firstFollower(leader);
        cluster.pauseNode(victim);
        writeAndLog("r3", "v3", "rp-3");
        writeAndLog("r4", "v4", "rp-4");

        FollowerProgress blocked = progressOf(cluster.replicationProgress(), victim);
        assertEquals(ReplicationState.BLOCKED, blocked.state(),
                "被暂停导致的落后应与崩溃追赶区分");
        assertTrue(blocked.lag() > 0);

        cluster.resumeNode(victim);
        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }
        FollowerProgress synced = progressOf(cluster.replicationProgress(), victim);
        assertEquals(ReplicationState.IN_SYNC, synced.state(), "恢复后应追平");
        assertEquals(ClusterTestSupport.status(cluster, leader).commitIndex(),
                ClusterTestSupport.status(cluster, victim).commitIndex(),
                "追平后提交位置与主一致");
    }

    @Test
    void crashedFollowerIsDownThenCatchingUpThenInSync() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String victim = firstFollower(leader);
        writeAndLog("r5", "v5", "rp-5");

        cluster.crashNode(victim);
        writeAndLog("r6", "v6", "rp-6");
        writeAndLog("r7", "v7", "rp-7");

        FollowerProgress down = progressOf(cluster.replicationProgress(), victim);
        assertEquals(ReplicationState.DOWN, down.state(), "崩溃未重启应可区分");
        assertTrue(down.lag() > 0);

        cluster.restartNode(victim);
        FollowerProgress catchingUp = progressOf(cluster.replicationProgress(), victim);
        assertEquals(ReplicationState.CATCHING_UP, catchingUp.state(),
                "崩溃重启后、补日志前应处于追赶状态");
        assertTrue(catchingUp.lag() > 0);

        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }
        FollowerProgress synced = progressOf(cluster.replicationProgress(), victim);
        assertEquals(ReplicationState.IN_SYNC, synced.state(), "恢复后进度应追上");
        assertEquals(ClusterTestSupport.status(cluster, leader).commitIndex(),
                ClusterTestSupport.status(cluster, victim).commitIndex(),
                "追赶完成后与已提交内容一致");
    }

    @Test
    void confirmedReadsAreNotRolledBackAfterRecovery() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String victim = firstFollower(leader);
        writeAndLog("r8", "committed", "rp-8");

        ReadOutcome before = cluster.read("r8", "rp-read-1");
        assertTrue(before.success() && before.found());
        long versionBefore = before.version();

        cluster.crashNode(victim);
        writeAndLog("r9", "during-down", "rp-9");
        cluster.restartNode(victim);
        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }

        ReadOutcome after = cluster.read("r8", "rp-read-2");
        log.info("assert-no-rollback key=r8 versionBefore={} versionAfter={} value={}",
                versionBefore, after.version(), after.value());
        assertTrue(after.success() && after.found());
        assertEquals("committed", after.value(), "已确认的读取结果不得被回滚");
        assertEquals(versionBefore, after.version(), "已确认键的版本不得回退");
        assertEquals("during-down", cluster.read("r9", "rp-read-3").value());
    }
}
