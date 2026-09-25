package com.github.lisama569.springquorumreplicastorejava.cluster;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SmokeTest {

    @Test
    void electWriteRead() {
        RaftCluster cluster = new RaftCluster();
        cluster.startElection("n1");
        assertEquals("n1", cluster.leaderId());

        RaftCluster.WriteResult w = cluster.write("w-1", "foo", "bar", null, null);
        assertTrue(w.success(), () -> "write should succeed: " + w);
        assertEquals(1, w.commitIndex() - 1); // no-op@1, write@2 -> commitIndex 2

        RaftCluster.ReadResult r = cluster.read("foo");
        assertTrue(r.found(), "read should find foo");
        assertEquals("bar", r.value());
    }
}
