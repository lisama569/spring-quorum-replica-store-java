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

/** 版本化键与条件写入：命中、不命中、键不存在、版本单调、删除路径。 */
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

    private WriteOutcome put(String key, String value, String requestId, Long expectedVersion) {
        WriteOutcome outcome = cluster.write(WriteOp.PUT, key, value, requestId,
                expectedVersion, null);
        log.info("assert-write requestId={} key={} expectedVersion={} success={} version={}"
                        + " term={} commitIndex={} reason={} detail={}",
                requestId, key, expectedVersion, outcome.success(), outcome.version(),
                outcome.term(), outcome.commitIndex(), outcome.reason(), outcome.detail());
        return outcome;
    }

    private ReadOutcome read(String key, String requestId) {
        ReadOutcome outcome = cluster.read(key, requestId);
        log.info("assert-read requestId={} key={} success={} found={} value={} version={}"
                        + " commitIndex={} reason={}",
                requestId, key, outcome.success(), outcome.found(), outcome.value(),
                outcome.version(), outcome.commitIndex(), outcome.reason());
        return outcome;
    }

    @Test
    void conditionalWriteSucceedsWhenVersionMatches() {
        ClusterTestSupport.electLeader(cluster);
        WriteOutcome first = put("c1", "v1", "cw-1", null);
        assertTrue(first.success());
        assertEquals(1, first.version(), "首次写入版本应为 1");

        WriteOutcome second = put("c1", "v2", "cw-2", 1L);
        assertTrue(second.success());
        assertEquals(2, second.version(), "条件命中后版本应推进到 2");

        ReadOutcome read = read("c1", "cw-read-1");
        assertTrue(read.success() && read.found());
        assertEquals("v2", read.value());
        assertEquals(2, read.version(), "读取应同时返回当前版本");
    }

    @Test
    void conditionalWriteRejectedWhenVersionMismatch() {
        ClusterTestSupport.electLeader(cluster);
        put("c2", "v1", "cw-3", null);

        WriteOutcome mismatch = put("c2", "v2", "cw-4", 5L);
        assertFalse(mismatch.success());
        assertEquals(RejectionReason.VERSION_MISMATCH, mismatch.reason());
        assertEquals(1, mismatch.version(), "拒绝时应返回当前实际版本供上层重试");

        ReadOutcome read = read("c2", "cw-read-2");
        assertEquals("v1", read.value());
        assertEquals(1, read.version(), "版本不匹配的写不得改动键值与版本");
    }

    @Test
    void conditionalWriteOnMissingKeyIsDistinguishable() {
        ClusterTestSupport.electLeader(cluster);

        WriteOutcome notFound = put("c3", "v1", "cw-5", 3L);
        assertFalse(notFound.success());
        assertEquals(RejectionReason.KEY_NOT_FOUND, notFound.reason(),
                "对不存在的键指定已存在版本，应与版本冲突区分");
        assertEquals(0, notFound.version());

        // expectedVersion=0 表示“期望键不存在”，可创建
        WriteOutcome create = put("c3", "v1", "cw-6", 0L);
        assertTrue(create.success());
        assertEquals(1, create.version());

        ReadOutcome missing = read("c4", "cw-read-3");
        assertTrue(missing.success(), "键不存在不是多数派/参数类失败，读取本身仍成功");
        assertFalse(missing.found());
        assertEquals(0, missing.version());
    }

    @Test
    void versionIsMonotonicAcrossWritesAndDelete() {
        ClusterTestSupport.electLeader(cluster);
        long previous = 0;
        for (int i = 1; i <= 3; i++) {
            WriteOutcome outcome = put("c5", "v" + i, "cw-mono-" + i, null);
            assertTrue(outcome.success());
            assertTrue(outcome.version() > previous,
                    "版本必须只增不减: previous=" + previous + " now=" + outcome.version());
            previous = outcome.version();
        }

        WriteOutcome delete = cluster.delete("c5", "cw-mono-del", null, null);
        log.info("assert-delete requestId={} success={} version={} reason={}",
                "cw-mono-del", delete.success(), delete.version(), delete.reason());
        assertTrue(delete.success());
        assertTrue(delete.version() > previous, "删除也推进版本");
        previous = delete.version();

        ReadOutcome afterDelete = read("c5", "cw-read-4");
        assertFalse(afterDelete.found());
        assertEquals(previous, afterDelete.version(), "删除后版本保留（墓碑），不倒退");

        // 重新创建：版本继续递增而不是回到 1
        WriteOutcome recreate = put("c5", "v4", "cw-mono-4", null);
        assertTrue(recreate.success());
        assertTrue(recreate.version() > previous, "重建后版本不得回退");
    }

    @Test
    void deleteFollowsSameVersionedPath() {
        ClusterTestSupport.electLeader(cluster);
        put("c6", "v1", "cw-7", null);

        WriteOutcome wrongVersion = cluster.delete("c6", "cw-8", 9L, null);
        log.info("assert-delete requestId={} success={} version={} reason={}",
                "cw-8", wrongVersion.success(), wrongVersion.version(), wrongVersion.reason());
        assertFalse(wrongVersion.success());
        assertEquals(RejectionReason.VERSION_MISMATCH, wrongVersion.reason());
        assertTrue(read("c6", "cw-read-5").found(), "版本不匹配的删除不得生效");

        WriteOutcome matched = cluster.delete("c6", "cw-9", 1L, null);
        assertTrue(matched.success());
        assertFalse(read("c6", "cw-read-6").found());

        WriteOutcome deleteAbsent = cluster.delete("c6-absent", "cw-10", null, null);
        assertFalse(deleteAbsent.success());
        assertEquals(RejectionReason.KEY_NOT_FOUND, deleteAbsent.reason(),
                "删除不存在的键应有可区分结果");
    }
}
