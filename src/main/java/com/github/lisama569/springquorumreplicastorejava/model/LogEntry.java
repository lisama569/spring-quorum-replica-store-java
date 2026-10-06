package com.github.lisama569.springquorumreplicastorejava.model;

/**
 * 复制日志条目。
 *
 * @param term            写入该条目的主节点任期
 * @param index           日志位置（从 1 开始单调递增）
 * @param key             键
 * @param value           值（删除操作时为空串占位）
 * @param requestId       客户端请求标识，用于幂等去重
 * @param expectedVersion 调用方期望的键当前版本；null 表示无条件写入
 * @param delete          是否为删除操作（删除与写入共用同一条受版本约束的路径）
 */
public record LogEntry(long term, long index, String key, String value, String requestId,
                       Long expectedVersion, boolean delete) {

    /** 兼容旧调用方：无条件、非删除写入。 */
    public LogEntry(long term, long index, String key, String value, String requestId) {
        this(term, index, key, value, requestId, null, false);
    }
}
