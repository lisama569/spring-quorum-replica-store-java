package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 复制日志条目。
 *
 * @param term            写入该条目的主节点任期
 * @param index           日志位置（从 1 开始单调递增）
 * @param op              操作类型（PUT/DELETE/NOOP）
 * @param key             键
 * @param value           值（DELETE/NOOP 时为 null 或占位）
 * @param expectedVersion 条件写入的期望版本；null 表示无条件写入
 * @param clientTerm      调用方（网关）缓存的任期；null 表示未携带
 * @param requestId       客户端请求标识，用于幂等去重
 */
public record LogEntry(long term, long index, WriteOp op, String key, String value,
                       Long expectedVersion, Long clientTerm, String requestId) {

    /** 无条件、未携带客户端任期的 PUT（兼容旧请求语义）。 */
    public static LogEntry put(long term, long index, String key, String value, String requestId) {
        return new LogEntry(term, index, WriteOp.PUT, key, value, null, null, requestId);
    }
}
