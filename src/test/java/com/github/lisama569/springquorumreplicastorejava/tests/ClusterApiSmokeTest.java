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
}
