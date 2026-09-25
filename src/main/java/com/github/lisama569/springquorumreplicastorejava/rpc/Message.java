package com.github.lisama569.springquorumreplicastorejava.rpc;

import com.github.lisama569.springquorumreplicastorejava.model.LogEntry;

import java.util.List;

/** 节点间复制的消息类型（密封层次）。 */
public sealed interface Message {

    /** 发送方节点 id。 */
    String from();

    /** 发送方任期。 */
    long term();

    /** 投票请求。 */
    record RequestVoteRequest(String from, long term, long lastLogIndex, long lastLogTerm)
            implements Message {
    }

    /** 投票响应。 */
    record RequestVoteResponse(String from, long term, boolean voteGranted, String reason)
            implements Message {
    }

    /** 日志复制（含心跳，entries 为空即心跳）。 */
    record AppendEntriesRequest(String from, long term, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit)
            implements Message {
    }

    /** 日志复制响应。 */
    record AppendEntriesResponse(String from, long term, boolean success, long matchIndex,
                                 String reason)
            implements Message {
    }
}
