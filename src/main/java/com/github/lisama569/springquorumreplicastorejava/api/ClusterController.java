package com.github.lisama569.springquorumreplicastorejava.api;

import com.github.lisama569.springquorumreplicastorejava.cluster.Cluster;
import com.github.lisama569.springquorumreplicastorejava.model.NodeStatus;
import com.github.lisama569.springquorumreplicastorejava.model.ReadOutcome;
import com.github.lisama569.springquorumreplicastorejava.model.ReplicationStatus;
import com.github.lisama569.springquorumreplicastorejava.model.WriteOutcome;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 集群对外 REST 接口：读写、观测与故障注入。 */
@RestController
@RequestMapping("/api/cluster")
public class ClusterController {

    private final Cluster cluster;

    public ClusterController(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * 写请求体。requestId 缺省时由服务端生成（不提供幂等去重）。
     * expectedVersion / clientTerm 为可选：缺省时保持旧的无条件写入语义。
     */
    public record WriteRequest(String key, String value, String requestId,
                               Long expectedVersion, Long clientTerm) {
    }

    /** 删除请求体：与写入共用受版本约束的路径。 */
    public record DeleteRequest(String key, String requestId,
                                Long expectedVersion, Long clientTerm) {
    }

    /** 分区请求体。 */
    public record PartitionRequest(Set<String> groupA, Set<String> groupB) {
    }

    @PostMapping("/write")
    public ResponseEntity<WriteOutcome> write(@RequestBody WriteRequest request) {
        String requestId = request.requestId() != null ? request.requestId()
                : UUID.randomUUID().toString();
        WriteOutcome outcome = cluster.write(request.key(), request.value(), requestId,
                request.expectedVersion(), request.clientTerm());
        return outcome.success() ? ResponseEntity.ok(outcome)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(outcome);
    }

    @PostMapping("/delete")
    public ResponseEntity<WriteOutcome> delete(@RequestBody DeleteRequest request) {
        String requestId = request.requestId() != null ? request.requestId()
                : UUID.randomUUID().toString();
        WriteOutcome outcome = cluster.delete(request.key(), requestId,
                request.expectedVersion(), request.clientTerm());
        return outcome.success() ? ResponseEntity.ok(outcome)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(outcome);
    }

    @GetMapping("/replication")
    public ReplicationStatus replication() {
        return cluster.replicationStatus();
    }

    @GetMapping("/read")
    public ResponseEntity<ReadOutcome> read(@RequestParam String key,
                                            @RequestParam(required = false) String requestId) {
        String id = requestId != null ? requestId : UUID.randomUUID().toString();
        ReadOutcome outcome = cluster.read(key, id);
        return outcome.success() ? ResponseEntity.ok(outcome)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(outcome);
    }

    @GetMapping("/status")
    public List<NodeStatus> status() {
        return cluster.status();
    }

    @GetMapping("/leader")
    public Map<String, String> leader() {
        String leaderId = cluster.leaderId();
        return leaderId != null ? Map.of("leader", leaderId) : Map.of("leader", "none");
    }

    @PostMapping("/fault/pause/{nodeId}")
    public Map<String, String> pause(@PathVariable String nodeId) {
        cluster.pauseNode(nodeId);
        return Map.of("paused", nodeId);
    }

    @PostMapping("/fault/resume/{nodeId}")
    public Map<String, String> resume(@PathVariable String nodeId) {
        cluster.resumeNode(nodeId);
        return Map.of("resumed", nodeId);
    }

    @PostMapping("/fault/crash/{nodeId}")
    public Map<String, String> crash(@PathVariable String nodeId) {
        cluster.crashNode(nodeId);
        return Map.of("crashed", nodeId);
    }

    @PostMapping("/fault/restart/{nodeId}")
    public Map<String, String> restart(@PathVariable String nodeId) {
        cluster.restartNode(nodeId);
        return Map.of("restarted", nodeId);
    }

    @PostMapping("/fault/partition")
    public Map<String, String> partition(@RequestBody PartitionRequest request) {
        cluster.partition(request.groupA(), request.groupB());
        return Map.of("partitioned", request.groupA() + "|" + request.groupB());
    }

    @PostMapping("/fault/heal")
    public Map<String, String> heal() {
        cluster.healPartition();
        return Map.of("healed", "all");
    }
}
