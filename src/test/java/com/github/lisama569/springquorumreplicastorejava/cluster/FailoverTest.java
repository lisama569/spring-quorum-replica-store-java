package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 主节点故障、选举切换、旧主不能覆盖已提交历史、节点重启后数据一致。
 */
class FailoverTest {

    private RaftCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new RaftCluster();
        cluster.startElection("n1");
    }

    @Test
    void leaderFailureElectsNewLeaderAndCommittedDataSurvives() {
        assertTrue(cluster.write("f-1", "a", "1", null, null).success());
        long oldTerm = cluster.status("n1").term();
        assertEquals("n1", cluster.leaderId());

        cluster.failLeader();
        String newLeader = cluster.leaderId();
        assertNotNull(newLeader);
        assertNotEquals("n1", newLeader);
        assertTrue(cluster.status(newLeader).term() > oldTerm, "term must increase after election");

        // 已提交数据不丢
        assertEquals("1", cluster.read("a").value());

        // 新任期继续写入
        assertTrue(cluster.write("f-2", "b", "2", null, null).success());
        assertEquals("2", cluster.read("b").value());
    }

    @Test
    void staleLeaderCannotOverwriteCommittedHistory() {
        assertTrue(cluster.write("s-1", "k", "committed", null, null).success());
        assertEquals(2, cluster.status("n1").commitIndex());

        // 隔离 n1，其余节点选出新主并提交新值
        cluster.partition(List.of("n1"), List.of("n2", "n3"));
        cluster.startElection("n2");
        assertEquals("n2", cluster.leaderId());
        assertTrue(cluster.write("s-2", "k", "new", null, null).success());
        assertEquals("new", cluster.read("k").value());
        long majorityTerm = cluster.status("n2").term();

        // 旧主 n1 仍持有旧值，但无法服务多数派写；恢复分区后必须收敛到新值
        assertEquals("committed", cluster.status("n1").committedData().get("k"));
        RaftCluster.WriteResult staleWrite = cluster.write("s-3", "k", "old-write", null, null);
        // 客户端门面找到的 leader 是多数派 leader，因此 s-3 正常提交到多数派历史
        assertTrue(staleWrite.success());

        cluster.healPartition();
        cluster.settle();
        assertEquals(cluster.status("n2").term(), cluster.status("n1").term());
        for (NodeSnapshot s : cluster.status()) {
            assertEquals("old-write", s.committedData().get("k"));
            assertEquals(s.commitIndex(), cluster.status("n2").commitIndex());
            assertTrue(s.term() >= majorityTerm);
        }
    }

    @Test
    void restartedNodeRebuildsStateAndCatchesUp() {
        assertTrue(cluster.write("r-1", "x", "10", null, null).success());
        assertTrue(cluster.write("r-2", "y", "20", null, null).success());
        long committed = cluster.status("n3").commitIndex();

        // 杀掉 n3（模拟进程停止），之后集群继续提交
        cluster.network().setDead("n3", true);
        assertTrue(cluster.write("r-3", "z", "30", null, null).success());

        // 重启 n3：持久化日志/提交位保留，重放状态机并追随 leader 补齐
        cluster.restart("n3");
        NodeSnapshot s3 = cluster.status("n3");
        assertEquals(Role.FOLLOWER, s3.role());
        assertEquals("10", s3.committedData().get("x"));
        assertEquals("20", s3.committedData().get("y"));
        assertEquals("30", s3.committedData().get("z"));
        assertEquals(cluster.status("n1").commitIndex(), s3.commitIndex());
        assertTrue(committed >= 2);
    }

    @Test
    void restartedLeaderReturnsAsFollowerAndDataIsIntact() {
        assertTrue(cluster.write("rl-1", "keep", "me", null, null).success());
        cluster.network().setDead("n1", true);
        cluster.failLeader(); // n1 已死，存活节点中选出新主
        String newLeader = cluster.leaderId();
        assertNotNull(newLeader);

        cluster.restart("n1");
        assertEquals(Role.FOLLOWER, cluster.status("n1").role());
        assertEquals("me", cluster.status("n1").committedData().get("keep"));
        assertEquals("me", cluster.read("keep").value());
    }
}
