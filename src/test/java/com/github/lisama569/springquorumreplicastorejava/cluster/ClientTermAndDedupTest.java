package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOp;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 写请求携带客户端任期（网关缓存）与重复请求的严格处理。 */
class ClientTermAndDedupTest {

    private static final Logger log = LoggerFactory.getLogger(ClientTermAndDedupTest.class);

    private Cluster cluster;

    @BeforeEach
    void setUp() {
        cluster = ClusterTestSupport.newCluster(3);
    }

    @AfterEach
    void tearDown() {
        cluster.shutdown();
    }

    private WriteOutcome write(String key, String value, String requestId, Long clientTerm) {
        WriteOutcome outcome = cluster.write(WriteOp.PUT, key, value, requestId, null, clientTerm);
        log.info("assert-write requestId={} key={} clientTerm={} success={} version={} term={}"
                        + " reason={} detail={}",
                requestId, key, clientTerm, outcome.success(), outcome.version(),
                outcome.term(), outcome.reason(), outcome.detail());
        return outcome;
    }

    @Test
    void staleClientTermIsRejectedWithDistinctReason() {
        String leader = ClusterTestSupport.electLeader(cluster);
        long term = ClusterTestSupport.status(cluster, leader).currentTerm();

        WriteOutcome stale = write("t1", "v1", "ct-1", term - 1);
        assertFalse(stale.success());
        assertEquals(RejectionReason.STALE_TERM, stale.reason(),
                "落后任期必须明确拒绝且原因可区分");

        ReadOutcome read = cluster.read("t1", "ct-read-1");
        assertFalse(read.found(), "落后任期的写不得产生任何状态变更");

        WriteOutcome current = write("t1", "v1", "ct-2", term);
        assertTrue(current.success(), "携带当前任期的写应正常生效");
    }

    @Test
    void gatewayCachedTermBecomesStaleAfterFailover() {
        String leader = ClusterTestSupport.electLeader(cluster);
        long cachedTerm = ClusterTestSupport.status(cluster, leader).currentTerm();
        WriteOutcome ok = write("t2", "v1", "ct-3", cachedTerm);
        assertTrue(ok.success());

        // 主节点崩溃 → 新主上任，任期推进；网关缓存的旧任期随之落后
        cluster.crashNode(leader);
        String newLeader = ClusterTestSupport.awaitNewLeader(cluster, leader);
        long newTerm = ClusterTestSupport.status(cluster, newLeader).currentTerm();
        log.info("failover oldLeader={} cachedTerm={} newLeader={} newTerm={}",
                leader, cachedTerm, newLeader, newTerm);
        assertTrue(newTerm > cachedTerm);

        WriteOutcome stale = write("t2", "v2", "ct-4", cachedTerm);
        assertFalse(stale.success());
        assertEquals(RejectionReason.STALE_TERM, stale.reason());
        assertEquals("v1", cluster.read("t2", "ct-read-2").value(), "旧任期写不得生效");
    }

    @Test
    void sameRequestIdWithDifferentContentIsRejected() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = write("d1", "v1", "dedup-1", null);
        assertTrue(first.success());

        WriteOutcome tampered = write("d1", "v2", "dedup-1", null);
        assertFalse(tampered.success());
        assertEquals(RejectionReason.REQUEST_CONFLICT, tampered.reason(),
                "同 requestId 不同内容必须明确拒绝");

        WriteOutcome differentKey = write("d2", "v1", "dedup-1", null);
        assertFalse(differentKey.success());
        assertEquals(RejectionReason.REQUEST_CONFLICT, differentKey.reason());

        ReadOutcome read = cluster.read("d1", "dedup-read-1");
        assertEquals("v1", read.value());
        assertEquals(1, read.version(), "冲突请求不得改动任何键的值与版本");
        assertFalse(cluster.read("d2", "dedup-read-2").found());
    }

    @Test
    void identicalRetryIsReplayed() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = write("d3", "v1", "dedup-2", null);
        assertTrue(first.success());

        WriteOutcome replay = write("d3", "v1", "dedup-2", null);
        assertTrue(replay.success());
        assertEquals(RejectionReason.DUPLICATE_REQUEST, replay.reason());
        assertEquals(first.version(), replay.version(), "重放返回首次结果的版本");
        assertEquals(1, cluster.read("d3", "dedup-read-3").version(),
                "重放不得推进版本");
    }

    @Test
    void legacyRequestsWithoutVersionAndTermStillWork() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome legacy = cluster.write("legacy-k", "legacy-v", "legacy-1");
        log.info("assert-legacy-write requestId={} success={} version={} reason={}",
                legacy.requestId(), legacy.success(), legacy.version(), legacy.reason());
        assertTrue(legacy.success());
        assertEquals(RejectionReason.NONE, legacy.reason());
        assertEquals(1, legacy.version(), "老请求同样获得版本（从 1 开始）");

        ReadOutcome read = cluster.read("legacy-k", "legacy-read-1");
        assertTrue(read.success() && read.found());
        assertEquals("legacy-v", read.value());
    }
}
