package com.github.lisama569.springquorumreplicastorejava.model;

/** 从副本相对主副本的复制进度状态。 */
public enum ReplicationState {
    /** 已追平主节点日志末端。 */
    IN_SYNC,
    /** 存活且可达，但落后于主节点，正在追赶（如崩溃重启后补日志）。 */
    CATCHING_UP,
    /** 因被暂停或分区而不可达，导致落后。 */
    BLOCKED,
    /** 已崩溃（未重启），不可服务。 */
    DOWN
}
