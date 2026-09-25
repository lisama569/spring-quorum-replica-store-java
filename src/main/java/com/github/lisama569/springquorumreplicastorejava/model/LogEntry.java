package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 复制日志条目。
 *
 * @param term      写入该条目的主节点任期
 * @param index     日志位置（从 1 开始单调递增）
 * @param key       键
 * @param value     值
 * @param requestId 客户端请求标识，用于幂等去重
 */
public record LogEntry(long term, long index, String key, String value, String requestId) {
}
