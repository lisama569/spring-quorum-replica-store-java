package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 网络分区：多数派继续服务、少数派写不能前台成功、
 * 分区恢复后全体收敛一致，已确认的历史读取不被回滚。
 */
class PartitionTest {

    private RaftCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new RaftCluster();
        cluster.startElection("n1");
        assertTrue(cluster.write("p-0", "shared", "base", null, null).success());
    }

    @Test
    void majoritySideKeepsServingAndMinorityWritesFail() {
        // n1 单独在一侧，n2/n3 在多数派一侧
        cluster.partition(List.of("n1"), List.of("n2", "n3"));
        cluster.startElection("n2");
        assertEquals("n2", cluster.leaderId());

        // 多数派侧读写正常
        assertTrue(cluster.write("p-1", "shared", "maj-1", null, null).success());
        assertEquals("maj-1", cluster.read("shared").value());

        // n1（少数派旧主）即使收到写请求也无法形成多数派 -> 失败
        RaftCluster.WriteResult minority = writeViaNode("n1", "p-bad", "stale");
        assertFalse(minority.success());
        assertEquals(RejectReason.QUORUM_UNAVAILABLE, minority.rejectReason());

        // 恢复后所有节点一致，且历史确认值不被回滚为旧值
        cluster.healPartition();
        for (NodeSnapshot s : cluster.status()) {
            assertEquals("maj-1", s.committedData().get("shared"));
        }
        assertEquals("maj-1", cluster.read("shared").value());
    }

    @Test
    void healedClusterAppendsFurtherWritesAtOneSharedHistory() {
        cluster.partition(List.of("n1"), List.of("n2", "n3"));
        cluster.startElection("n2");
        assertTrue(cluster.write("q-1", "q", "1", null, null).success());

        cluster.healPartition();
        // 愈合后任期统一、日志一致，可继续线性写入
        long commitAfterHeal = cluster.status("n2").commitIndex();
        assertTrue(cluster.write("q-2", "q", "2", null, null).success());
        assertTrue(cluster.write("q-3", "q", "3", null, null).success());
        for (NodeSnapshot s : cluster.status()) {
            assertEquals("3", s.committedData().get("q"));
            assertEquals(cluster.leaderId() == null ? -1 : cluster.status(cluster.leaderId()).commitIndex(),
                    s.commitIndex());
            assertTrue(s.commitIndex() > commitAfterHeal);
            assertEquals(cluster.status("n2").term(), s.term());
        }
    }

    @Test
    void committedReadsAreNeverRolledBackAfterHeal() {
        // 分区前再确认一批键值
        assertTrue(cluster.write("p-2", "keep1", "v1", null, null).success());
        assertTrue(cluster.write("p-3", "keep2", "v2", null, null).success());

        cluster.partition(List.of("n1"), List.of("n2", "n3"));
        cluster.startElection("n2");
        assertTrue(cluster.write("p-4", "keep2", "v2-new", null, null).success());
        // 分区期间已对多数派确认的读取结论
        assertEquals("v2-new", cluster.read("keep2").value());
        assertEquals("v1", cluster.read("keep1").value());

        cluster.healPartition();
        assertEquals("v2-new", cluster.read("keep2").value());
        assertEquals("v1", cluster.read("keep1").value());
        assertEquals("base", cluster.read("shared").value());
    }

    /** 直接向指定节点投递客户端写（绕过门面的最高任期 leader 选择）。 */
    private RaftCluster.WriteResult writeViaNode(String nodeId, String requestId, String value) {
        long term = cluster.status(nodeId).term();
        cluster.network().send(new RpcMessage.ClientWrite("client", nodeId,
                cluster.network().nextId(), requestId, "minority-key", value, null, term));
        RpcMessage response = cluster.pumpOneClientMessage();
        if (response instanceof RpcMessage.ClientResponse cr) {
            return new RaftCluster.WriteResult(cr.requestId(), nodeId, cr.term(), cr.commitIndex(),
                    cr.key(), cr.value(), cr.success(), cr.rejectReason(),
                    cr.success() ? "ok" : cr.rejectReason().name());
        }
        return new RaftCluster.WriteResult(requestId, nodeId, -1, -1, "minority-key", null,
                false, RejectReason.QUORUM_UNAVAILABLE, "no response");
    }
}
