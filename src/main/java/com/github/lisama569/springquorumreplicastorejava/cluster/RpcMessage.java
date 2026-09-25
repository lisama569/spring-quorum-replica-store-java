package com.github.lisama569.springquorumreplicastorejava.cluster;

/**
 * 单进程模拟环境下节点之间（以及 client 与 leader 之间）传递的消息。
 * 采用密封接口穷举所有消息种类，配合模式匹配处理，确保不遗漏。
 */
public sealed interface RpcMessage
        permits RpcMessage.AppendEntries, RpcMessage.AppendEntriesResult,
        RpcMessage.RequestVote, RpcMessage.RequestVoteResult,
        RpcMessage.Timeout, RpcMessage.ClientWrite, RpcMessage.ClientResponse,
        RpcMessage.ClientRead, RpcMessage.ReadResult {

    /** 发送方：节点 id，或 "client"。 */
    String from();

    /** 接收方：节点 id，或 "client"。 */
    String to();

    /** 单调消息序号，由 Network 分配，用于去重/乱序注入。 */
    long messageId();

    /** 日志复制（心跳 prevLog* 为空条目）。 */
    record AppendEntries(String from, String to, long messageId, long term, String leaderId,
                         long prevLogIndex, long prevLogTerm, long leaderCommit,
                         LogEntry entry) implements RpcMessage {
    }

    /** 日志复制应答；conflictIndex/conflictTerm 用于快速回退 nextIndex。 */
    record AppendEntriesResult(String from, String to, long messageId, long term, boolean success,
                               long matchIndex, long conflictIndex, long conflictTerm) implements RpcMessage {
    }

    record RequestVote(String from, String to, long messageId, long term,
                       long lastLogIndex, long lastLogTerm) implements RpcMessage {
    }

    record RequestVoteResult(String from, String to, long messageId, long term, boolean granted)
            implements RpcMessage {
    }

    /** 选举/心跳定时器到期。 */
    record Timeout(String from, String to, long messageId, long term) implements RpcMessage {
    }

    /**
     * 客户端写请求。expectedValue 非空时为 CAS 写：
     * 仅当 key 当前已提交值等于 expectedValue 才接受。
     */
    record ClientWrite(String from, String to, long messageId, String requestId, String key,
                       String value, String expectedValue, Long expectedTerm) implements RpcMessage {
    }

    record ClientResponse(String from, String to, long messageId, String requestId, boolean success,
                          RejectReason rejectReason, long term, long commitIndex,
                          String key, String value) implements RpcMessage {
    }

    record ClientRead(String from, String to, long messageId, String requestId, String key)
            implements RpcMessage {
    }

    record ReadResult(String from, String to, long messageId, String requestId, boolean found,
                      String value, long term, long commitIndex, RejectReason rejectReason)
            implements RpcMessage {
    }
}
