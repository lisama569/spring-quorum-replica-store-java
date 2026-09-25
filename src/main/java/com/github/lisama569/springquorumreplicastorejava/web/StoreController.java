package com.github.lisama569.springquorumreplicastorejava.web;

import com.github.lisama569.springquorumreplicastorejava.cluster.Network;
import com.github.lisama569.springquorumreplicastorejava.cluster.NodeSnapshot;
import com.github.lisama569.springquorumreplicastorejava.cluster.RaftCluster;
import com.github.lisama569.springquorumreplicastorejava.cluster.RejectReason;
import com.github.lisama569.springquorumreplicastorejava.cluster.ReplicationException;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 键值存储与运维/故障注入的 HTTP 门面。所有写读都返回
 * requestId/term/commitIndex/判定依据，便于本地重复验证与解释。
 */
@RestController
@RequestMapping("/api")
public class StoreController {

    private final RaftCluster cluster;
    private long autoRequestId;

    public StoreController(RaftCluster cluster) {
        this.cluster = cluster;
    }

    @PostMapping("/kv/{key}")
    public synchronized Map<String, Object> write(@PathVariable String key,
                                                  @RequestBody Map<String, Object> body) {
        String value = asString(body.get("value"));
        if (value == null) {
            throw new IllegalArgumentException("body.value is required");
        }
        String requestId = asString(body.getOrDefault("requestId", null));
        if (requestId == null || requestId.isBlank()) {
            requestId = "w-" + (++autoRequestId);
        }
        String expectedValue = asString(body.get("expectedValue"));
        Long expectedTerm = body.get("expectedTerm") == null
                ? null : ((Number) body.get("expectedTerm")).longValue();

        RaftCluster.WriteResult result = cluster.write(requestId, key, value, expectedValue, expectedTerm);
        if (!result.success()) {
            throw new ReplicationException(result.rejectReason(), result.requestId(),
                    result.term(), result.commitIndex(), result.detail());
        }
        Map<String, Object> out = base(result.term(), result.commitIndex());
        out.put("success", true);
        out.put("requestId", result.requestId());
        out.put("leaderId", result.leaderId());
        out.put("key", result.key());
        out.put("value", result.value());
        out.put("basis", result.detail());
        return out;
    }

    @GetMapping("/kv/{key}")
    public synchronized Map<String, Object> read(@PathVariable String key) {
        RaftCluster.ReadResult result = cluster.read(key);
        if (result.rejectReason() == RejectReason.KEY_NOT_FOUND) {
            throw new ReplicationException(RejectReason.KEY_NOT_FOUND, null,
                    result.term(), result.commitIndex(), "key not found: " + key);
        }
        if (result.rejectReason() != null) {
            throw new ReplicationException(result.rejectReason(), null,
                    result.term(), result.commitIndex(), "read rejected");
        }
        Map<String, Object> out = base(result.term(), result.commitIndex());
        out.put("success", true);
        out.put("found", result.found());
        out.put("leaderId", result.leaderId());
        out.put("key", result.key());
        out.put("value", result.value());
        out.put("basis", "quorum-barrier read");
        return out;
    }

    @GetMapping("/status")
    public synchronized Map<String, Object> status() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("leaderId", cluster.leaderId());
        out.put("nodes", cluster.status());
        return out;
    }

    @GetMapping("/status/{nodeId}")
    public synchronized NodeSnapshot status(@PathVariable String nodeId) {
        return cluster.status(nodeId);
    }

    // ---------- 故障注入 / 生命周期 ----------

    @PostMapping("/admin/nodes/{nodeId}/pause")
    public synchronized Map<String, Object> pause(@PathVariable String nodeId) {
        cluster.pause(nodeId);
        return ok();
    }

    @PostMapping("/admin/nodes/{nodeId}/resume")
    public synchronized Map<String, Object> resume(@PathVariable String nodeId,
                                                   @RequestParam(defaultValue = "false") boolean dropPending) {
        cluster.resume(nodeId, dropPending);
        return ok();
    }

    @PostMapping("/admin/nodes/{nodeId}/restart")
    public synchronized Map<String, Object> restart(@PathVariable String nodeId) {
        cluster.restart(nodeId);
        return ok();
    }

    @PostMapping("/admin/leader/fail")
    public synchronized Map<String, Object> failLeader() {
        String old = cluster.leaderId();
        cluster.failLeader();
        Map<String, Object> out = ok();
        out.put("oldLeader", old);
        out.put("newLeader", cluster.leaderId());
        return out;
    }

    @PostMapping("/admin/election/{nodeId}")
    public synchronized Map<String, Object> elect(@PathVariable String nodeId) {
        cluster.startElection(nodeId);
        Map<String, Object> out = ok();
        out.put("leaderId", cluster.leaderId());
        return out;
    }

    @PostMapping("/admin/partition")
    public synchronized Map<String, Object> partition(@RequestBody Map<String, List<String>> body) {
        List<String> sideA = body.get("sideA");
        List<String> sideB = body.get("sideB");
        if (sideA == null || sideB == null) {
            throw new IllegalArgumentException("body requires sideA and sideB arrays");
        }
        cluster.partition(sideA, sideB);
        return ok();
    }

    @PostMapping("/admin/partition/heal")
    public synchronized Map<String, Object> heal() {
        cluster.healPartition();
        Map<String, Object> out = ok();
        out.put("leaderId", cluster.leaderId());
        return out;
    }

    @PostMapping("/admin/network/drop")
    public synchronized Map<String, Object> drop(@RequestParam String from, @RequestParam String to,
                                                 @RequestParam double probability) {
        cluster.setDropProbability(from, to, probability);
        return ok();
    }

    @PostMapping("/admin/network/order")
    public synchronized Map<String, Object> order(@RequestParam String from, @RequestParam String to,
                                                  @RequestParam Network.OrderMode mode) {
        cluster.setOrderMode(from, to, mode);
        return ok();
    }

    @PostMapping("/admin/network/duplicate")
    public synchronized Map<String, Object> duplicate(@RequestParam String from, @RequestParam String to,
                                                      @RequestParam int extraCopies) {
        cluster.setDuplicate(from, to, extraCopies);
        return ok();
    }

    private static Map<String, Object> ok() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("success", true);
        return map;
    }

    private static Map<String, Object> base(long term, long commitIndex) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("term", term);
        map.put("commitIndex", commitIndex);
        return map;
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
