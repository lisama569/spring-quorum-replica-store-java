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

/** 键版本与条件写入：命中/不命中、键不存在、删除与版本单调性。 */
class ConditionalWriteTest {

    private static final Logger log = LoggerFactory.getLogger(ConditionalWriteTest.class);

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
        log.info("{} requestId={} term={} commitIndex={} version={} currentVersion={} "
                        + "reason={} detail={}",
                tag, o.requestId(), o.term(), o.commitIndex(), o.version(), o.currentVersion(),
                o.reason(), o.detail());
    }

    @Test
    void conditionalWriteSucceedsWhenVersionMatches() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = cluster.write("k1", "v1", "cw-1");
        logOutcome("assert-write", first);
        assertTrue(first.success());
        assertEquals(1, first.version(), "首次写入版本应为 1");

        WriteOutcome cas = cluster.write("k1", "v2", "cw-2", 1L, null);
        logOutcome("assert-cas", cas);
        assertTrue(cas.success());
        assertEquals(2, cas.version(), "条件写命中后版本应推进到 2");

        ReadOutcome read = cluster.read("k1", "cw-read-1");
        log.info("assert-read requestId={} term={} commitIndex={} found={} version={} value={}",
                read.key(), read.term(), read.commitIndex(), read.found(), read.version(),
                read.value());
        assertTrue(read.success() && read.found());
        assertEquals("v2", read.value());
        assertEquals(2, read.version(), "读取应同时返回当前版本");
    }

    @Test
    void conditionalWriteRejectedWhenVersionMismatch() {
        ClusterTestSupport.electLeader(cluster);
        cluster.write("k1", "v1", "cm-1");

        WriteOutcome mismatch = cluster.write("k1", "v2", "cm-2", 5L, null);
        logOutcome("assert-mismatch", mismatch);
        assertFalse(mismatch.success());
        assertEquals(RejectionReason.VERSION_MISMATCH, mismatch.reason());
        assertEquals(1, mismatch.currentVersion(), "拒绝时应返回当前实际版本供上层重试");

        // 不命中不能改动键值
        ReadOutcome read = cluster.read("k1", "cm-read-1");
        assertEquals("v1", read.value());
        assertEquals(1, read.version());
    }

    @Test
    void missingKeyHasDistinguishableOutcomes() {
        ClusterTestSupport.electLeader(cluster);

        // 读不存在的键：成功但 found=false，与 NO_QUORUM 等失败可区分
        ReadOutcome miss = cluster.read("ghost", "mk-read-1");
        log.info("assert-read-miss requestId={} found={} version={} reason={}",
                miss.key(), miss.found(), miss.version(), miss.reason());
        assertTrue(miss.success());
        assertFalse(miss.found());
        assertEquals(RejectionReason.NONE, miss.reason());

        // 对不存在的键做带版本（>0）的条件写：KEY_NOT_FOUND，且返回实际版本 0
        WriteOutcome casMissing = cluster.write("ghost", "v", "mk-1", 3L, null);
        logOutcome("assert-cas-missing", casMissing);
        assertFalse(casMissing.success());
        assertEquals(RejectionReason.KEY_NOT_FOUND, casMissing.reason());
        assertEquals(0, casMissing.currentVersion());

        // 期望版本 0 表示“期望不存在”，可创建
        WriteOutcome create = cluster.write("ghost", "v", "mk-2", 0L, null);
        logOutcome("assert-create-if-absent", create);
        assertTrue(create.success());
        assertEquals(1, create.version());

        // 键已存在后再以 expectedVersion=0 写入：版本不匹配而非键不存在
        WriteOutcome notAbsent = cluster.write("ghost", "v2", "mk-3", 0L, null);
        logOutcome("assert-not-absent", notAbsent);
        assertFalse(notAbsent.success());
        assertEquals(RejectionReason.VERSION_MISMATCH, notAbsent.reason());
        assertEquals(1, notAbsent.currentVersion());
    }

    @Test
    void deleteFollowsSameVersionedPath() {
        ClusterTestSupport.electLeader(cluster);
        cluster.write("k1", "v1", "del-1");
        cluster.write("k1", "v2", "del-2");

        // 版本不对的删除被拒绝
        WriteOutcome wrongVersion = cluster.delete("k1", "del-3", 1L, null);
        logOutcome("assert-delete-mismatch", wrongVersion);
        assertFalse(wrongVersion.success());
        assertEquals(RejectionReason.VERSION_MISMATCH, wrongVersion.reason());
        assertEquals(2, wrongVersion.currentVersion());
        assertEquals("v2", cluster.read("k1", "del-read-1").value());

        // 版本匹配的删除生效，删除本身也推进版本
        WriteOutcome deleted = cluster.delete("k1", "del-4", 2L, null);
        logOutcome("assert-delete", deleted);
        assertTrue(deleted.success());
        assertEquals(3, deleted.version(), "删除也应让版本只增不减");

        ReadOutcome afterDelete = cluster.read("k1", "del-read-2");
        log.info("assert-read-deleted found={} version={}", afterDelete.found(),
                afterDelete.version());
        assertTrue(afterDelete.success());
        assertFalse(afterDelete.found());

        // 删除不存在的键：KEY_NOT_FOUND
        WriteOutcome deleteAgain = cluster.delete("k1", "del-5", 3L, null);
        logOutcome("assert-delete-tombstone", deleteAgain);
        assertFalse(deleteAgain.success());
        assertEquals(RejectionReason.KEY_NOT_FOUND, deleteAgain.reason());
        assertEquals(3, deleteAgain.currentVersion(), "墓碑版本应保留");

        // 重建后版本继续递增而不是回到 1
        WriteOutcome recreated = cluster.write("k1", "v3", "del-6", 3L, null);
        logOutcome("assert-recreate", recreated);
        assertTrue(recreated.success());
        assertEquals(4, recreated.version());
    }

    @Test
    void versionNeverRegressesAcrossWrites() {
        ClusterTestSupport.electLeader(cluster);
        long previous = 0;
        for (int i = 1; i <= 6; i++) {
            WriteOutcome outcome = cluster.write("seq", "v" + i, "seq-" + i);
            logOutcome("assert-monotonic", outcome);
            assertTrue(outcome.success());
            assertTrue(outcome.version() > previous,
                    "版本必须严格递增: previous=" + previous + " now=" + outcome.version());
            previous = outcome.version();
        }
        // 主节点切换后版本仍然不倒退
        String oldLeader = cluster.leaderId();
        cluster.crashNode(oldLeader);
        ClusterTestSupport.awaitNewLeader(cluster, oldLeader);
        WriteOutcome afterFailover = cluster.write("seq", "v7", "seq-7");
        logOutcome("assert-after-failover", afterFailover);
        assertTrue(afterFailover.success());
        assertTrue(afterFailover.version() > previous,
                "主切换后版本不得倒退: previous=" + previous + " now=" + afterFailover.version());
    }
}
