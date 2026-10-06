package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 读请求结果。
 *
 * @param success     是否成功（多数派确认）；键未命中不算失败，由 found 区分
 * @param key         键
 * @param value       值（未命中或失败时为 null）
 * @param term        处理时的任期
 * @param commitIndex 读取依据的提交位置
 * @param reason      拒绝原因（成功时为 NONE）
 * @param detail      判定依据说明
 * @param found       键是否存在（与 NO_QUORUM 等失败可区分）
 * @param version     命中时该键的当前版本；未命中或失败时为 0 / -1
 */
public record ReadOutcome(boolean success, String key, String value, long term, long commitIndex,
                          RejectionReason reason, String detail, boolean found, long version) {

    public static ReadOutcome ok(String key, String value, long term, long commitIndex,
                                 String detail) {
        return ok(key, value, term, commitIndex, detail, value != null, 0);
    }

    public static ReadOutcome ok(String key, String value, long term, long commitIndex,
                                 String detail, boolean found, long version) {
        return new ReadOutcome(true, key, value, term, commitIndex, RejectionReason.NONE, detail,
                found, version);
    }

    public static ReadOutcome rejected(String key, long term, long commitIndex,
                                       RejectionReason reason, String detail) {
        return new ReadOutcome(false, key, null, term, commitIndex, reason, detail, false, -1);
    }
}
