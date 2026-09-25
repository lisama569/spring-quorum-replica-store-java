package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息重复投递与乱序（LIFO）下的安全性：
 * 重复请求不得产生重复状态变更，乱序 AE 由日志一致性检查消化。
 */
class DeliveryTest {

    private RaftCluster cluster;

    @BeforeEach
    void setUp() {
        cluster = new RaftCluster();
        cluster.startElection("n1");
    }

    @Test
    void duplicateClientRequestProducesSingleStateChange() {
        // 领导 -> n2 链路对每条消息额外复制 2 份：AE/AE 应答/客户端链路均被重复
        cluster.setDuplicate("n1", "n2", 2);
        cluster.setDuplicate("n2", "n1", 2);

        RaftCluster.WriteResult w1 = cluster.write("dup-x", "dupkey", "v1", null, null);
        assertTrue(w1.success(), w1::detail);

        // 相同 requestId 再来一次（不同链路副本仍开着）：仍是同一条日志，提交位置不变
        RaftCluster.WriteResult w2 = cluster.write("dup-x", "dupkey", "v1", null, null);
        assertTrue(w2.success());
        assertEquals(w1.commitIndex(), w2.commitIndex());

        // 全部节点的状态一致，且重复投递未造成额外提交
        long leaderCommit = cluster.status("n1").commitIndex();
        for (NodeSnapshot s : cluster.status()) {
            assertEquals("v1", s.committedData().get("dupkey"));
            assertEquals(leaderCommit, s.commitIndex());
            long occurrences = s.logSummary().stream()
                    .filter(line -> line.endsWith(":dup-x"))
                    .count();
            assertEquals(1, occurrences, "duplicate request must appear once in log: " + s.logSummary());
        }
    }

    @Test
    void lifoReorderingOnLeaderLinkStillReplicatesCorrectly() {
        // n1 -> n3 改为后入先出，制造确定性乱序
        cluster.setOrderMode("n1", "n3", Network.OrderMode.LIFO);
        RaftCluster.WriteResult w1 = cluster.write("o-1", "o1", "a", null, null);
        assertTrue(w1.success(), w1::detail);
        RaftCluster.WriteResult w2 = cluster.write("o-2", "o2", "b", null, null);
        assertTrue(w2.success(), w2::detail);
        RaftCluster.WriteResult w3 = cluster.write("o-3", "o3", "c", null, null);
        assertTrue(w3.success(), w3::detail);

        cluster.settle();
        for (NodeSnapshot s : cluster.status()) {
            assertEquals("a", s.committedData().get("o1"));
            assertEquals("b", s.committedData().get("o2"));
            assertEquals("c", s.committedData().get("o3"));
            assertEquals(w3.commitIndex(), s.commitIndex());
        }
    }

    @Test
    void duplicatedAppendEntriesAreIdempotentOnFollowers() {
        // 对 n2、n3 均注入重复，连续多写，日志不能膨胀、提交索引单调
        cluster.setDuplicate("n1", "n2", 1);
        cluster.setDuplicate("n1", "n3", 1);
        long lastCommit = 0;
        for (int i = 0; i < 5; i++) {
            RaftCluster.WriteResult w = cluster.write("m-" + i, "mk" + i, "v" + i, null, null);
            assertTrue(w.success(), w::detail);
            assertTrue(w.commitIndex() > lastCommit);
            lastCommit = w.commitIndex();
        }
        for (NodeSnapshot s : cluster.status()) {
            assertEquals(lastCommit, s.commitIndex());
            assertEquals(5, s.committedData().size());
        }
    }
}
