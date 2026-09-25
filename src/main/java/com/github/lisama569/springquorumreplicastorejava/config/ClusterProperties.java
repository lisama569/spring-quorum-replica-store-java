package com.github.lisama569.springquorumreplicastorejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "quorum")
public class ClusterProperties {

    private List<String> nodes = List.of("n1", "n2", "n3");

    public List<String> getNodes() {
        return nodes;
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }
}
