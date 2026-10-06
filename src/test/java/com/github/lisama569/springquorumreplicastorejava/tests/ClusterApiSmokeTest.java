package com.github.lisama569.springquorumreplicastorejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** REST 端到端冒烟：启动真实 Spring 容器与集群调度器，验证读写/观测/故障注入。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ClusterApiSmokeTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper mapper = new ObjectMapper();

    private String base(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void writeReadStatusAndFaultInjectionOverHttp() throws Exception {
        // 等待首个主节点选出（真实时钟）
        String leader = null;
        for (int i = 0; i < 60; i++) {
            ResponseEntity<String> r = rest.getForEntity(base("/api/cluster/leader"), String.class);
            JsonNode body = mapper.readTree(r.getBody());
            if (!"none".equals(body.get("leader").asText())) {
                leader = body.get("leader").asText();
                break;
            }
            Thread.sleep(50);
        }
        assertNotNull(leader, "应在 3s 内选出主节点");

        // 写
        Map<String, String> req = Map.of("key", "http-k", "value", "http-v",
                "requestId", "http-req-1");
        ResponseEntity<String> write = rest.postForEntity(base("/api/cluster/write"),
                new HttpEntity<>(req), String.class);
        assertEquals(HttpStatus.OK, write.getStatusCode());
        JsonNode writeBody = mapper.readTree(write.getBody());
        assertTrue(writeBody.get("success").asBoolean());
        assertEquals("NONE", writeBody.get("reason").asText());
        assertTrue(writeBody.get("commitIndex").asLong() > 0);

        // 读
        ResponseEntity<String> read = rest.getForEntity(
                base("/api/cluster/read?key=http-k&requestId=http-read-1"), String.class);
        assertEquals(HttpStatus.OK, read.getStatusCode());
        JsonNode readBody = mapper.readTree(read.getBody());
        assertEquals("http-v", readBody.get("value").asText());

        // 观测字段
        ResponseEntity<String> status = rest.getForEntity(base("/api/cluster/status"), String.class);
        JsonNode statusBody = mapper.readTree(status.getBody());
        assertTrue(statusBody.isArray());
        assertEquals(3, statusBody.size());
        JsonNode first = statusBody.get(0);
        assertNotNull(first.get("role"));
        assertNotNull(first.get("currentTerm"));
        assertNotNull(first.get("commitIndex"));
        assertNotNull(first.get("lastApplied"));
        assertNotNull(first.get("logSize"));
        assertNotNull(first.get("paused"));
        assertNotNull(first.get("alive"));

        // 幂等重复请求
        ResponseEntity<String> dup = rest.postForEntity(base("/api/cluster/write"),
                new HttpEntity<>(req), String.class);
        JsonNode dupBody = mapper.readTree(dup.getBody());
        assertTrue(dupBody.get("success").asBoolean());
        assertEquals("DUPLICATE_REQUEST", dupBody.get("reason").asText());

        // 故障注入：暂停/恢复
        ResponseEntity<String> pause = rest.postForEntity(
                base("/api/cluster/fault/pause/n3"), null, String.class);
        assertEquals(HttpStatus.OK, pause.getStatusCode());
        JsonNode statusAfterPause = mapper.readTree(
                rest.getForEntity(base("/api/cluster/status"), String.class).getBody());
        assertTrue(statusAfterPause.get(2).get("paused").asBoolean());

        ResponseEntity<String> resume = rest.postForEntity(
                base("/api/cluster/fault/resume/n3"), null, String.class);
        assertEquals(HttpStatus.OK, resume.getStatusCode());
    }

    @Test
    void versionedWriteDeleteAndReplicationOverHttp() throws Exception {
        // 等待主节点
        String leader = null;
        for (int i = 0; i < 60; i++) {
            JsonNode body = mapper.readTree(
                    rest.getForEntity(base("/api/cluster/leader"), String.class).getBody());
            if (!"none".equals(body.get("leader").asText())) {
                leader = body.get("leader").asText();
                break;
            }
            Thread.sleep(50);
        }
        assertNotNull(leader);

        // 条件写：期望不存在（版本 0）→ 创建成功，返回新版本 1
        ResponseEntity<String> create = rest.postForEntity(base("/api/cluster/write"),
                new HttpEntity<>(Map.of("key", "vk", "value", "v1", "requestId", "vw-1",
                        "expectedVersion", 0)), String.class);
        JsonNode createBody = mapper.readTree(create.getBody());
        assertTrue(createBody.get("success").asBoolean());
        assertEquals(1, createBody.get("version").asLong());

        // 条件写：版本不匹配 → 409 VERSION_MISMATCH，并带回当前版本
        ResponseEntity<String> mismatch = rest.postForEntity(base("/api/cluster/write"),
                new HttpEntity<>(Map.of("key", "vk", "value", "v2", "requestId", "vw-2",
                        "expectedVersion", 7)), String.class);
        assertEquals(HttpStatus.CONFLICT, mismatch.getStatusCode());
        JsonNode mismatchBody = mapper.readTree(mismatch.getBody());
        assertEquals("VERSION_MISMATCH", mismatchBody.get("reason").asText());
        assertEquals(1, mismatchBody.get("currentVersion").asLong());

        // 落后任期 → 409 STALE_TERM
        ResponseEntity<String> stale = rest.postForEntity(base("/api/cluster/write"),
                new HttpEntity<>(Map.of("key", "vk", "value", "v2", "requestId", "vw-3",
                        "clientTerm", 0)), String.class);
        assertEquals(HttpStatus.CONFLICT, stale.getStatusCode());
        assertEquals("STALE_TERM", mapper.readTree(stale.getBody()).get("reason").asText());

        // 同 requestId 不同内容 → 409 REQUEST_CONFLICT
        ResponseEntity<String> conflict = rest.postForEntity(base("/api/cluster/write"),
                new HttpEntity<>(Map.of("key", "vk", "value", "other", "requestId", "vw-1",
                        "expectedVersion", 0)), String.class);
        assertEquals(HttpStatus.CONFLICT, conflict.getStatusCode());
        assertEquals("REQUEST_CONFLICT",
                mapper.readTree(conflict.getBody()).get("reason").asText());

        // 读取带版本；条件删除；删除后未命中但 found 字段可区分
        JsonNode readBody = mapper.readTree(rest.getForEntity(
                base("/api/cluster/read?key=vk&requestId=vw-read-1"), String.class).getBody());
        assertTrue(readBody.get("found").asBoolean());
        long version = readBody.get("version").asLong();

        ResponseEntity<String> delete = rest.postForEntity(base("/api/cluster/delete"),
                new HttpEntity<>(Map.of("key", "vk", "requestId", "vw-del-1",
                        "expectedVersion", version)), String.class);
        assertEquals(HttpStatus.OK, delete.getStatusCode());
        JsonNode gone = mapper.readTree(rest.getForEntity(
                base("/api/cluster/read?key=vk&requestId=vw-read-2"), String.class).getBody());
        assertTrue(gone.get("success").asBoolean());
        assertTrue(!gone.get("found").asBoolean());

        // 复制进度观测字段
        JsonNode replication = mapper.readTree(
                rest.getForEntity(base("/api/cluster/replication"), String.class).getBody());
        assertEquals(leader, replication.get("leaderId").asText());
        assertTrue(replication.get("followers").isArray());
        JsonNode follower = replication.get("followers").get(0);
        assertNotNull(follower.get("nextIndex"));
        assertNotNull(follower.get("matchIndex"));
        assertNotNull(follower.get("lag"));
        assertNotNull(follower.get("state"));
    }
}
