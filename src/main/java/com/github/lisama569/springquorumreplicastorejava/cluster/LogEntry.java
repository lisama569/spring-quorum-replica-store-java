package com.github.lisama569.springquorumreplicastorejava.cluster;

/**
 * 一条日志条目：term 为其所属任期，index 从 1 开始。
 * requestId 用于客户端请求去重，entryType 标记是普通写入还是无操作提交。
 */
public record LogEntry(long term, long index, String requestId, String key, String value,
                       String expectedValue, boolean noOp) {
}
