package com.github.lisama569.springquorumreplicastorejava.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * HTTP 门面：成功写读、可区分错误码、状态观测字段、故障注入端到端。
 * 每个测试方法后重建上下文，保证集群为初始 3 节点干净状态。
 */
@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class StoreControllerTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void writeReadAndStatusEndToEnd() throws Exception {
        mvc.perform(post("/api/admin/election/n1"))
                .andExpect(status().isOk());

        mvc.perform(post("/api/kv/color")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"http-1","value":"blue"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.requestId").value("http-1"))
                .andExpect(jsonPath("$.term").exists())
                .andExpect(jsonPath("$.commitIndex").exists())
                .andExpect(jsonPath("$.leaderId").value("n1"));

        mvc.perform(get("/api/kv/color"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("blue"))
                .andExpect(jsonPath("$.basis").value("quorum-barrier read"));

        mvc.perform(get("/api/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.leaderId").value("n1"))
                .andExpect(jsonPath("$.nodes.length()").value(3))
                .andExpect(jsonPath("$.nodes[0].role").exists())
                .andExpect(jsonPath("$.nodes[0].commitIndex").exists())
                .andExpect(jsonPath("$.nodes[0].lastLogIndex").exists())
                .andExpect(jsonPath("$.nodes[0].appliedIndex").exists())
                .andExpect(jsonPath("$.nodes[0].committedData").exists())
                .andExpect(jsonPath("$.nodes[0].logSummary").isArray());
    }

    @Test
    void distinctRejectReasonsOverHttp() throws Exception {
        mvc.perform(post("/api/admin/election/n1")).andExpect(status().isOk());

        // 陈旧任期 -> 409 STALE_TERM
        mvc.perform(post("/api/kv/k1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"h-stale","value":"v","expectedTerm":999}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejectReason").value("STALE_TERM"))
                .andExpect(jsonPath("$.term").exists())
                .andExpect(jsonPath("$.commitIndex").exists());

        // CAS 冲突 -> 409 CAS_CONFLICT
        mvc.perform(post("/api/kv/cas")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"h-cas","value":"v","expectedValue":"nope"}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejectReason").value("CAS_CONFLICT"));

        // 读不存在的键 -> 404 KEY_NOT_FOUND
        mvc.perform(get("/api/kv/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.rejectReason").value("KEY_NOT_FOUND"));
    }

    @Test
    void leaderFailoverOverHttpKeepsCommittedData() throws Exception {
        mvc.perform(post("/api/admin/election/n1")).andExpect(status().isOk());
        mvc.perform(post("/api/kv/hf")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"hf-1","value":"survive"}
                                """))
                .andExpect(status().isOk());

        mvc.perform(post("/api/admin/leader/fail"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.oldLeader").value("n1"))
                .andExpect(jsonPath("$.newLeader").isString());

        mvc.perform(get("/api/kv/hf"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("survive"));
    }

    @Test
    void partitionAndHealOverHttp() throws Exception {
        mvc.perform(post("/api/admin/election/n1")).andExpect(status().isOk());
        mvc.perform(post("/api/kv/pa")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"pa-0","value":"before"}
                                """))
                .andExpect(status().isOk());

        mvc.perform(post("/api/admin/partition")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sideA":["n1"],"sideB":["n2","n3"]}
                                """))
                .andExpect(status().isOk());
        mvc.perform(post("/api/admin/election/n2")).andExpect(status().isOk());
        mvc.perform(post("/api/kv/pa")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"pa-1","value":"after"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.leaderId").value("n2"));

        mvc.perform(post("/api/admin/partition/heal")).andExpect(status().isOk());
        mvc.perform(get("/api/kv/pa"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.value").value("after"));
    }
}
