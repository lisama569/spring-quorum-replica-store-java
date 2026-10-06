package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 主节点视角下单个从副本的复制进度。
 *
 * @param nodeId     从副本节点标识
 * @param sentIndex  主节点已尝试发送到的日志位置（nextIndex-1）
 * @param matchIndex 从副本已确认复制的日志位置
 * @param lag        落后主节点日志末端的条目数
 * @param state      进度状态（追平 / 追赶中 / 被暂停或分区阻塞 / 已崩溃）
 */
public record FollowerProgress(String nodeId, long sentIndex, long matchIndex, long lag,
                               ReplicationState state) {
}
