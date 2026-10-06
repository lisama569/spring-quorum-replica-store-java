package com.github.lisama569.springquorumreplicastorejava.model;

import java.util.List;

/**
 * 主节点复制进度总览。
 *
 * @param leaderId     主节点标识（无多数派主时为 null）
 * @param term         主节点当前任期
 * @param commitIndex  主节点已提交位置
 * @param lastLogIndex 主节点日志末端位置
 * @param followers    各从副本的复制进度
 */
public record ReplicationStatus(String leaderId, long term, long commitIndex,
                                long lastLogIndex, List<FollowerReplication> followers) {
}
