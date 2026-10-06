package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.RejectionReason;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 客户端任期检查与同一请求标识的严格去重。 */
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

    private void logOutcome(String tag, WriteOutcome o) {
        log.info("{} requestId={} term={} commitIndex={} version={} reason={} detail={}",
                tag, o.requestId(), o.term(), o.commitIndex(), o.version(), o.reason(), o.detail());
    }

    @Test
    void staleClientTermIsRejectedDistinctly() {
        String leader = ClusterTestSupport.electLeader(cluster);
        long term1 = ClusterTestSupport.status(cluster, leader).currentTerm();

        // 携带当前任期的写正常成功
        WriteOutcome ok = cluster.write("k1", "v1", "ct-1", null, term1);
        logOutcome("assert-current-term", ok);
        assertTrue(ok.success());

        // 主切换，任期推进
        cluster.crashNode(leader);
        String newLeader = ClusterTestSupport.awaitNewLeader(cluster, leader);
        long term2 = ClusterTestSupport.status(cluster, newLeader).currentTerm();
        assertTrue(term2 > term1, "新主任期应更大");

        // 网关重试带上缓存的旧任期：必须明确拒绝且区别于其他失败
        WriteOutcome stale = cluster.write("k2", "v2", "ct-2", null, term1);
        logOutcome("assert-stale-term", stale);
        assertFalse(stale.success());
        assertEquals(RejectionReason.STALE_TERM, stale.reason());

        // 被拒绝的请求不能产生任何状态变更
        ReadOutcome read = cluster.read("k2", "ct-read-1");
        assertTrue(read.success());
        assertFalse(read.found(), "STALE_TERM 拒绝后键不得被写入");

        // 携带新任期则放行
        WriteOutcome fresh = cluster.write("k2", "v2", "ct-3", null, term2);
        logOutcome("assert-fresh-term", fresh);
        assertTrue(fresh.success());
    }

    @Test
    void sameRequestIdWithDifferentContentIsRejected() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = cluster.write("k1", "v1", "dedup-1");
        logOutcome("assert-first", first);
        assertTrue(first.success());
        long commitAfterFirst = ClusterTestSupport.status(cluster, cluster.leaderId())
                .commitIndex();

        // 同 requestId、不同值：明确拒绝，且不动任何键
        WriteOutcome conflict = cluster.write("k1", "v2", "dedup-1");
        logOutcome("assert-conflict", conflict);
        assertFalse(conflict.success());
        assertEquals(RejectionReason.REQUEST_CONFLICT, conflict.reason());
        assertEquals("v1", cluster.read("k1", "dedup-read-1").value());
        assertEquals(commitAfterFirst,
                ClusterTestSupport.status(cluster, cluster.leaderId()).commitIndex(),
                "冲突拒绝不得推进提交位置");

        // 同 requestId、不同键同样拒绝
        WriteOutcome conflictKey = cluster.write("k9", "v1", "dedup-1");
        logOutcome("assert-conflict-key", conflictKey);
        assertFalse(conflictKey.success());
        assertEquals(RejectionReason.REQUEST_CONFLICT, conflictKey.reason());
        assertFalse(cluster.read("k9", "dedup-read-2").found());
    }

    @Test
    void identicalRetryIsReplayed() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = cluster.write("k1", "v1", "dedup-replay", 0L, null);
        logOutcome("assert-first", first);
        assertTrue(first.success());

        // 内容完全一致（含期望版本）：重放首次结果
        WriteOutcome replay = cluster.write("k1", "v1", "dedup-replay", 0L, null);
        logOutcome("assert-replay", replay);
        assertTrue(replay.success());
        assertEquals(RejectionReason.DUPLICATE_REQUEST, replay.reason());
        assertEquals(first.version(), replay.version(), "重放应返回首次写入的版本");
        assertEquals(1, cluster.read("k1", "dedup-replay-read").version(),
                "重放不得推进版本");
    }

    @Test
    void legacyRequestsWithoutVersionAndTermStillWork() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome legacy = cluster.write("legacy", "v", "legacy-1");
        logOutcome("assert-legacy", legacy);
        assertTrue(legacy.success());
        assertEquals(1, legacy.version());

        WriteOutcome legacyReplay = cluster.write("legacy", "v", "legacy-1");
        logOutcome("assert-legacy-replay", legacyReplay);
        assertTrue(legacyReplay.success());
        assertEquals(RejectionReason.DUPLICATE_REQUEST, legacyReplay.reason());

        ReadOutcome read = cluster.read("legacy", "legacy-read");
        assertTrue(read.success() && read.found());
        assertEquals("v", read.value());
    }
}
