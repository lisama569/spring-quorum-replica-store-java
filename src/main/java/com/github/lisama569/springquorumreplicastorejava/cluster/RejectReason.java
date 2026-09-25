package com.github.lisama569.springquorumreplicastorejava.cluster;

/**
 * 可区分的请求拒绝原因，与日志/HTTP 错误码一一对应。
 */
public enum RejectReason {
    NOT_LEADER,
    STALE_TERM,
    QUORUM_UNAVAILABLE,
    LOG_CONFLICT,
    DUPLICATE_MISMATCH,
    CAS_CONFLICT,
    NO_LEADER,
    NODE_UNAVAILABLE,
    KEY_NOT_FOUND
}
