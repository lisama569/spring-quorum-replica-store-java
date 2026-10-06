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
    /** 条件写入的期望版本与当前实际版本不一致，键值未被改动。 */
    VERSION_MISMATCH,
    /** 键不存在（条件写入指定了已存在版本 / 删除不存在的键）。 */
    KEY_NOT_FOUND,
    /** 同一 requestId 重复提交但内容不一致，已明确拒绝，未产生任何状态变更。 */
    REQUEST_CONFLICT
}
