package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 写请求结果。
 *
 * @param success     是否成功（达到多数派并提交，且条件校验通过）
 * @param requestId   请求标识
 * @param term        处理时的任期
 * @param commitIndex 提交位置
 * @param version     成功后该键的新版本；版本冲突时返回当前实际版本（键不存在为 0）
 * @param reason      拒绝原因（成功时为 NONE）
 * @param detail      判定依据说明
 */
public record WriteOutcome(boolean success, String requestId, long term, long commitIndex,
                           long version, RejectionReason reason, String detail) {

    public static WriteOutcome ok(String requestId, long term, long commitIndex, long version,
                                  String detail) {
        return new WriteOutcome(true, requestId, term, commitIndex, version,
                RejectionReason.NONE, detail);
    }

    public static WriteOutcome rejected(String requestId, long term, long commitIndex,
                                        long version, RejectionReason reason, String detail) {
        return new WriteOutcome(false, requestId, term, commitIndex, version, reason, detail);
    }
}
