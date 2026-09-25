package com.github.lisama569.springquorumreplicastorejava.cluster;

import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;

import static org.junit.jupiter.api.Assertions.fail;

/** 测试支撑：手动驱动时钟，保证确定性。 */
final class ClusterTestSupport {

    private ClusterTestSupport() {
    }

    /** 创建不启动调度器的集群，测试通过 tick() 手动推进。 */
    static Cluster newCluster(int nodeCount) {
        return new Cluster(nodeCount, 60_000);
    }

    /** 推进时钟直到选出主节点。 */
    static String electLeader(Cluster cluster) {
        for (int i = 0; i < 300; i++) {
            cluster.tick();
            String leader = cluster.leaderId();
            if (leader != null) {
                return leader;
            }
        }
        fail("no leader elected within 300 ticks");
        return null;
    }

    /** 推进时钟直到出现与 oldLeader 不同的新主节点。 */
    static String awaitNewLeader(Cluster cluster, String oldLeader) {
        for (int i = 0; i < 300; i++) {
            cluster.tick();
            String leader = cluster.leaderId();
            if (leader != null && !leader.equals(oldLeader)) {
                return leader;
            }
        }
        fail("no new leader elected within 300 ticks, oldLeader=" + oldLeader);
        return null;
    }

    static NodeStatus status(Cluster cluster, String nodeId) {
        return cluster.status().stream()
                .filter(s -> s.nodeId().equals(nodeId))
                .findFirst()
                .orElseThrow();
    }
}
