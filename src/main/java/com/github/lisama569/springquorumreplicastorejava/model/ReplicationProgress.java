package com.github.lisama569.springquorumreplicastorejava.model;

import java.util.List;

/**
 * 集群复制进度快照（主节点视角）。
 *
 * @param leaderId           主节点标识（无主时为 "none"）
 * @param term               主节点当前任期
 * @param leaderLastLogIndex 主节点日志末端位置
 * @param commitIndex        主节点已提交位置
 * @param followers          各从副本的复制进度
 */
public record ReplicationProgress(String leaderId, long term, long leaderLastLogIndex,
                                  long commitIndex, List<FollowerProgress> followers) {

    /** 无主时的空快照。 */
    public static ReplicationProgress noLeader() {
        return new ReplicationProgress("none", -1, -1, -1, List.of());
    }
}
