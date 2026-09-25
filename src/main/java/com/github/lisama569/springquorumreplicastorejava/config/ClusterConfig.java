package com.github.lisama569.springquorumreplicastorejava.config;

import com.github.lisama569.springquorumreplicastorejava.cluster.Network;
import com.github.lisama569.springquorumreplicastorejava.cluster.RaftCluster;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ClusterProperties.class)
public class ClusterConfig {

    @Bean
    public Network network() {
        return new Network();
    }

    @Bean
    public RaftCluster raftCluster(ClusterProperties properties, Network network) {
        return new RaftCluster(properties.getNodes(), network);
    }
}
