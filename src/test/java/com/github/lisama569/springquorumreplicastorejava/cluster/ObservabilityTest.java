package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运行态观测字段：角色、任期、提交位置、同步状态（nextIndex/matchIndex/日志摘要）。
 */
class ObservabilityTest {

    @Test
    void snapshotsExposeRoleTermCommitAndSyncState() {
        RaftCluster cluster = new RaftCluster();
        cluster.startElection("n1");
        cluster.write("o-1", "k", "v", null, null);

        NodeSnapshot leader = cluster.status("n1");
        assertEquals(Role.LEADER, leader.role());
        assertEquals(1, leader.term());
        assertEquals(2, leader.commitIndex());
        assertEquals(2, leader.appliedIndex());
        assertTrue(leader.alive());
        assertFalse(leader.paused());
        assertEquals(2L, leader.matchIndex().get("n2"));
        assertEquals(2L, leader.matchIndex().get("n3"));
        assertEquals(3L, leader.nextIndex().get("n2"));
        assertTrue(leader.logSummary().contains("2:1:o-1"));

        for (String id : List.of("n2", "n3")) {
            NodeSnapshot f = cluster.status(id);
            assertEquals(Role.FOLLOWER, f.role());
            assertEquals(2, f.commitIndex());
            assertEquals("v", f.committedData().get("k"));
            assertTrue(f.nextIndex().isEmpty(), "followers keep no nextIndex table");
        }
    }

    @Test
    void pausedNodeReflectsStateAndResumes() {
        RaftCluster cluster = new RaftCluster();
        cluster.startElection("n1");
        cluster.pause("n2");
        assertTrue(cluster.status("n2").paused());
        // 只暂停一个跟随者：2/3 仍是多数派，写可以成功
        assertTrue(cluster.write("obs-0", "k0", "v0", null, null).success());

        // 再暂停 n3：领导只剩自己一票，无法形成多数派 -> 写失败
        cluster.pause("n3");
        RaftCluster.WriteResult w = cluster.write("obs-x", "k", "v", null, null);
        assertFalse(w.success());
        assertEquals(RejectReason.QUORUM_UNAVAILABLE, w.rejectReason());

        cluster.resume("n3", false);
        // n2 恢复后，2/3 多数派恢复，写成功
        cluster.resume("n2", false);
        assertFalse(cluster.status("n2").paused());
        assertTrue(cluster.write("obs-1", "k", "v", null, null).success());
    }
}
