package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 副本运行态快照。
 *
 * @param nodeId      节点标识
 * @param role        当前角色
 * @param currentTerm 当前任期
 * @param commitIndex 已提交位置
 * @param lastApplied 已应用到状态机的位置
 * @param logSize     日志长度
 * @param paused      是否被故障注入暂停
 * @param alive       是否存活（未崩溃）
 */
public record NodeStatus(String nodeId, Role role, long currentTerm, long commitIndex,
                         long lastApplied, int logSize, boolean paused, boolean alive) {
}
