package com.github.lisama569.springquorumreplicastorejava.model;

/** 主节点视角下某个从副本的复制进度状态。 */
public enum ReplicationState {
    /** 已确认到主节点最新日志位置。 */
    IN_SYNC,
    /** 可达但落后（如崩溃重启后正在追赶）。 */
    CATCHING_UP,
    /** 因暂停或分区不可达而落后。 */
    BLOCKED,
    /** 已崩溃尚未重启。 */
    DOWN
}
