package com.github.lisama569.springquorumreplicastorejava.api;

import com.github.lisama569.springquorumreplicastorejava.cluster.Cluster;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 集群 Bean 装配。 */
@Configuration
public class ClusterConfig {

    @Bean(destroyMethod = "shutdown")
    public Cluster cluster(@Value("${cluster.nodes:3}") int nodeCount) {
        Cluster cluster = new Cluster(nodeCount);
        cluster.start();
        return cluster;
    }
}
