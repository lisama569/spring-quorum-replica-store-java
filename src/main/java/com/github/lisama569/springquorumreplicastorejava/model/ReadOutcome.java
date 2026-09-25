package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 读请求结果。
 *
 * @param success     是否成功
 * @param key         键
 * @param value       值（未命中或失败时为 null）
 * @param term        处理时的任期
 * @param commitIndex 读取依据的提交位置
 * @param reason      拒绝原因（成功时为 NONE）
 * @param detail      判定依据说明
 */
public record ReadOutcome(boolean success, String key, String value, long term, long commitIndex,
                          RejectionReason reason, String detail) {

    public static ReadOutcome ok(String key, String value, long term, long commitIndex, String detail) {
        return new ReadOutcome(true, key, value, term, commitIndex, RejectionReason.NONE, detail);
    }

    public static ReadOutcome rejected(String key, long term, long commitIndex,
                                       RejectionReason reason, String detail) {
        return new ReadOutcome(false, key, null, term, commitIndex, reason, detail);
    }
}
