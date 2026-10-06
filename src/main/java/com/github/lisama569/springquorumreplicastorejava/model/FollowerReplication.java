package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 主节点视角的单个从副本复制进度。
 *
 * @param nodeId     从副本标识
 * @param nextIndex  主节点下一条要发往该副本的日志位置
 * @param matchIndex 该副本已确认复制的日志位置
 * @param lag        落后的条目数（主节点日志末端 - matchIndex）
 * @param state      进度状态（区分暂停/分区落后与重启追赶）
 */
public record FollowerReplication(String nodeId, long nextIndex, long matchIndex,
                                  long lag, ReplicationState state) {
}
