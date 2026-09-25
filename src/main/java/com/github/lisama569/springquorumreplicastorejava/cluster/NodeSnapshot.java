package com.github.lisama569.springquorumreplicastorejava.cluster;

import java.util.List;
import java.util.Map;

/**
 * 单个副本的可观测状态快照。
 *
 * @param logSummary 日志条目摘要（index:term:requestId），用于核对是否出现重复/回滚
 */
public record NodeSnapshot(String nodeId, Role role, long term, String votedFor,
                           long currentTerm, long commitIndex, long lastLogIndex,
                           long lastLogTerm, long appliedIndex, boolean paused, boolean alive,
                           Map<String, String> committedData,
                           Map<String, Long> nextIndex,
                           Map<String, Long> matchIndex,
                           List<String> logSummary) {
}
