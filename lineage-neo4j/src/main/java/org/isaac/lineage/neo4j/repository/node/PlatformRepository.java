package org.isaac.lineage.neo4j.repository.node;

import org.isaac.lineage.neo4j.domain.node.PlatformNode;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/29 16:52
 * @since 1.0.0
 */
@Repository
public interface PlatformRepository extends Neo4jRepository<PlatformNode, String> {

    /**
     * CLUSTER_FROM_PLATFORM
     */
    @Query("MATCH (p:Platform),(c:Cluster) " +
            "WHERE p.platformName = c.platformName  " +
            "CREATE (c)-[r:CLUSTER_FROM_PLATFORM]->(p)")
    void createRelWithCluster();

    /**
     * CLUSTER_FROM_PLATFORM
     */
    @Query("MATCH (c:Cluster)-[r:CLUSTER_FROM_PLATFORM]->(p:Platform) " +
            "WHERE c.platformName = p.platformName " +
            "DELETE r")
    void deleteRelWithCluster();
}
