package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 读请求结果。
 *
 * @param success     是否成功（多数派确认）；键不存在时仍为成功，用 found 区分
 * @param key         键
 * @param found       键是否存在（false 时 value 为 null、version 为 0）
 * @param value       值（未命中或失败时为 null）
 * @param version     该键当前版本（不存在时为 0）
 * @param term        处理时的任期
 * @param commitIndex 读取依据的提交位置
 * @param reason      拒绝原因（成功时为 NONE）
 * @param detail      判定依据说明
 */
public record ReadOutcome(boolean success, String key, boolean found, String value, long version,
                          long term, long commitIndex, RejectionReason reason, String detail) {

    public static ReadOutcome ok(String key, boolean found, String value, long version,
                                 long term, long commitIndex, String detail) {
        return new ReadOutcome(true, key, found, value, version, term, commitIndex,
                RejectionReason.NONE, detail);
    }

    public static ReadOutcome rejected(String key, long term, long commitIndex,
                                       RejectionReason reason, String detail) {
        return new ReadOutcome(false, key, false, null, 0, term, commitIndex, reason, detail);
    }
}
