package com.github.lisama569.springquorumreplicastorejava.model;

/** 请求被拒绝时的可区分原因。 */
public enum RejectionReason {
    /** 未拒绝（成功）。 */
    NONE,
    /** 当前节点不是主节点。 */
    NOT_LEADER,
    /** 请求任期落后于当前任期。 */
    STALE_TERM,
    /** 无法形成多数派。 */
    NO_QUORUM,
    /** 与已提交历史冲突。 */
    LOG_CONFLICT,
    /** 节点被暂停/隔离，不可服务。 */
    NODE_UNAVAILABLE,
    /** 重复请求，直接返回首次结果，不产生新状态变更。 */
    DUPLICATE_REQUEST,
    /** 条件写入的期望版本与当前实际版本不一致。 */
    VERSION_MISMATCH,
    /** 目标键不存在（读取未命中 / 条件写入前置了已存在的版本）。 */
    KEY_NOT_FOUND,
    /** 同一请求标识再次提交但内容与首次不一致。 */
    REQUEST_CONFLICT
}
