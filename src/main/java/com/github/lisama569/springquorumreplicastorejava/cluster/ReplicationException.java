package com.github.lisama569.springquorumreplicastorejava.cluster;

/**
 * 复制/读写失败异常，携带可区分的拒绝原因以及判定时的任期与提交位置。
 */
public class ReplicationException extends RuntimeException {

    private final RejectReason reason;
    private final long term;
    private final long commitIndex;
    private final String requestId;

    public ReplicationException(RejectReason reason, String requestId, long term, long commitIndex, String message) {
        super(message);
        this.reason = reason;
        this.term = term;
        this.commitIndex = commitIndex;
        this.requestId = requestId;
    }

    public RejectReason getReason() {
        return reason;
    }

    public long getTerm() {
        return term;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public String getRequestId() {
        return requestId;
    }
}
