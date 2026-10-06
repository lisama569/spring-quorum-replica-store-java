package com.github.lisama569.springquorumreplicastorejava.model;

/** 日志条目承载的写操作类型。 */
public enum WriteOp {
    /** 写入/覆盖键值。 */
    PUT,
    /** 删除键（与 PUT 走同一条受版本约束的写入路径）。 */
    DELETE,
    /** 空操作（主节点上任时写入，用于推进当前任期提交位置）。 */
    NOOP
}
