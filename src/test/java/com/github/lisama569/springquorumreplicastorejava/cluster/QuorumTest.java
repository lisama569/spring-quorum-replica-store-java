package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 多数派成功与失败、CAS 冲突、陈旧任期、未达多数派读等场景。
 */
class QuorumTest {

    private RaftCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new RaftCluster();
        cluster.startElection("n1");
        assertEquals("n1", cluster.leaderId());
    }

    @Test
    void writeSucceedsWithMajorityAndIsVisibleToQuorumRead() {
        RaftCluster.WriteResult w = cluster.write("w-1", "k1", "v1", null, null);
        assertTrue(w.success());
        assertEquals(2, w.commitIndex()); // no-op@1, kv@2

        RaftCluster.ReadResult r = cluster.read("k1");
        assertTrue(r.found());
        assertEquals("v1", r.value());
        assertEquals(2, r.commitIndex());

        // 多数派（3 个节点中的 3 个）都已应用
        for (NodeSnapshot s : cluster.status()) {
            assertEquals(2, s.commitIndex());
            assertEquals("v1", s.committedData().get("k1"));
        }
    }

    @Test
    void writeFailsWhenQuorumUnavailableAndIsNotVisible() {
        // 暂停 n2、n3：leader 无法得到多数派
        cluster.pause("n2");
        cluster.pause("n3");

        RaftCluster.WriteResult w = cluster.write("w-x", "kx", "vx", null, null);
        assertFalse(w.success());
        assertEquals(RejectReason.QUORUM_UNAVAILABLE, w.rejectReason());

        // 恢复后未提交数据不得可见
        cluster.resume("n2", true);
        cluster.resume("n3", true);
        RaftCluster.ReadResult r = cluster.read("kx");
        assertFalse(r.found());
        assertEquals(RejectReason.KEY_NOT_FOUND, r.rejectReason());
    }

    @Test
    void majorityOfThreeSucceedsWithOneFollowerDown() {
        cluster.pause("n3");
        RaftCluster.WriteResult w = cluster.write("w-2", "k2", "v2", null, null);
        assertTrue(w.success(), () -> "2/3 quorum should succeed: " + w.detail());
        cluster.resume("n3", true);
        RaftCluster.ReadResult r = cluster.read("k2");
        assertEquals("v2", r.value());
    }

    @Test
    void staleTermWriteIsRejected() {
        RaftCluster.WriteResult w = cluster.write("w-stale", "k", "v", null, 999L);
        assertFalse(w.success());
        assertEquals(RejectReason.STALE_TERM, w.rejectReason());
    }

    @Test
    void casConflictIsRejectedWithDistinctReason() {
        assertTrue(cluster.write("w-cas-1", "cas", "v1", null, null).success());
        RaftCluster.WriteResult bad = cluster.write("w-cas-2", "cas", "v2", "different", null);
        assertFalse(bad.success());
        assertEquals(RejectReason.CAS_CONFLICT, bad.rejectReason());

        RaftCluster.WriteResult good = cluster.write("w-cas-3", "cas", "v2", "v1", null);
        assertTrue(good.success());
        assertEquals("v2", cluster.read("cas").value());
    }

    @Test
    void duplicateRequestIsIdempotentAndMismatchIsRejected() {
        RaftCluster.WriteResult first = cluster.write("dup-1", "dup", "a", null, null);
        assertTrue(first.success());
        // 相同 requestId + 相同负载：幂等成功，且提交位置不变
        RaftCluster.WriteResult again = cluster.write("dup-1", "dup", "a", null, null);
        assertTrue(again.success());
        assertEquals(first.commitIndex(), again.commitIndex());

        // requestId 复用但负载不同：可区分拒绝
        RaftCluster.WriteResult mismatch = cluster.write("dup-1", "dup", "b", null, null);
        assertFalse(mismatch.success());
        assertEquals(RejectReason.DUPLICATE_MISMATCH, mismatch.rejectReason());
        assertEquals("a", cluster.read("dup").value());
    }
}
