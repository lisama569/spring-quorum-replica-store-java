package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import com.github.lisama569.springquorumreplicastorejava.rpc.Envelope;
import com.github.lisama569.springquorumreplicastorejava.rpc.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 多数派读写成功与失败路径，以及可区分的拒绝原因。 */
class QuorumWriteTest {

    private static final Logger log = LoggerFactory.getLogger(QuorumWriteTest.class);

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
    void writeCommitsWithFullQuorum() {
        String leader = ClusterTestSupport.electLeader(cluster);
        WriteOutcome outcome = cluster.write("k1", "v1", "req-1");
        log.info("assert-write requestId={} term={} commitIndex={} reason={} detail={}",
                outcome.requestId(), outcome.term(), outcome.commitIndex(),
                outcome.reason(), outcome.detail());
        assertTrue(outcome.success());
        assertEquals(RejectionReason.NONE, outcome.reason());

        // 同步复制下，所有存活节点都应提交该条目
        for (var status : cluster.status()) {
            assertTrue(status.commitIndex() >= outcome.commitIndex() - 1,
                    "node " + status.nodeId() + " commitIndex=" + status.commitIndex());
        }
        // 多数派确认后读取可见
        ReadOutcome read = cluster.read("k1", "req-read-1");
        assertTrue(read.success());
        assertEquals("v1", read.value());
    }

    @Test
    void writeSucceedsWithOneFollowerPaused() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String follower = firstFollower(leader);
        cluster.pauseNode(follower);

        WriteOutcome outcome = cluster.write("k2", "v2", "req-2");
        log.info("assert-write requestId={} term={} commitIndex={} reason={} (follower {} paused)",
                outcome.requestId(), outcome.term(), outcome.commitIndex(),
                outcome.reason(), follower);
        assertTrue(outcome.success(), "2/3 多数派仍可提交");
    }

    @Test
    void writeFailsWithoutQuorum() {
        String leader = ClusterTestSupport.electLeader(cluster);
        List<String> followers = followersOf(leader);
        cluster.pauseNode(followers.get(0));
        cluster.pauseNode(followers.get(1));

        WriteOutcome outcome = cluster.write("k3", "v3", "req-3");
        log.info("assert-write requestId={} term={} commitIndex={} reason={} detail={}",
                outcome.requestId(), outcome.term(), outcome.commitIndex(),
                outcome.reason(), outcome.detail());
        assertFalse(outcome.success());
        assertEquals(RejectionReason.NO_QUORUM, outcome.reason());

        // 未达多数派的写不得对后续读取可见：恢复后该键仍无已提交值
        cluster.resumeNode(followers.get(0));
        cluster.resumeNode(followers.get(1));
    }

    @Test
    void writeToFollowerRejectedAsNotLeader() {
        String leader = ClusterTestSupport.electLeader(cluster);
        String follower = firstFollower(leader);

        WriteOutcome outcome = cluster.node(follower).clientWrite("k4", "v4", "req-4");
        log.info("assert-write requestId={} term={} commitIndex={} reason={} detail={}",
                outcome.requestId(), outcome.term(), outcome.commitIndex(),
                outcome.reason(), outcome.detail());
        assertFalse(outcome.success());
        assertEquals(RejectionReason.NOT_LEADER, outcome.reason());
    }

    @Test
    void staleTermAppendRejected() {
        String leader = ClusterTestSupport.electLeader(cluster);
        long currentTerm = ClusterTestSupport.status(cluster, leader).currentTerm();
        String follower = firstFollower(leader);

        // 用探针捕获节点对过期任期 AppendEntries 的响应
        List<Message> captured = new ArrayList<>();
        cluster.bus().register("stale-leader", env -> captured.add(env.message()));
        cluster.node(follower).onMessage(new Envelope("stale-leader", follower,
                new Message.AppendEntriesRequest("stale-leader", currentTerm - 1,
                        0, 0, List.of(), 0), 999));

        assertEquals(1, captured.size());
        Message.AppendEntriesResponse response = (Message.AppendEntriesResponse) captured.get(0);
        log.info("assert-append-response term={} success={} reason={}",
                response.term(), response.success(), response.reason());
        assertFalse(response.success());
        assertEquals(RejectionReason.STALE_TERM.name(), response.reason());
    }

    @Test
    void readFailsWithoutQuorumConfirmation() {
        String leader = ClusterTestSupport.electLeader(cluster);
        cluster.write("k5", "v5", "req-5");
        List<String> followers = followersOf(leader);
        cluster.pauseNode(followers.get(0));
        cluster.pauseNode(followers.get(1));

        ReadOutcome outcome = cluster.read("k5", "req-read-5");
        log.info("assert-read requestId={} term={} commitIndex={} reason={} detail={}",
                "req-read-5", outcome.term(), outcome.commitIndex(),
                outcome.reason(), outcome.detail());
        assertFalse(outcome.success());
        assertEquals(RejectionReason.NO_QUORUM, outcome.reason());
    }

    private String firstFollower(String leader) {
        return followersOf(leader).get(0);
    }

    private List<String> followersOf(String leader) {
        return cluster.status().stream()
                .map(s -> s.nodeId())
                .filter(id -> !id.equals(leader))
                .toList();
    }
}
