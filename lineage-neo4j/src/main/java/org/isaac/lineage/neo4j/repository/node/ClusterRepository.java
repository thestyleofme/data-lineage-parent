package org.isaac.lineage.neo4j.repository.node;

import org.isaac.lineage.neo4j.domain.node.ClusterNode;
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
public interface ClusterRepository extends Neo4jRepository<ClusterNode, String> {

    /**
     * SCHEMA_FROM_CLUSTER
     */
    @Query("MATCH (c:Cluster),(s:Schema) " +
            "WHERE c.platformName = s.platformName  " +
            "AND c.clusterName = s.clusterName  " +
            "MERGE (s)-[r:SCHEMA_FROM_CLUSTER]->(c)")
    void createRelWithSchema();

    /**
     * SCHEMA_FROM_CLUSTER
     */
    @Query("MATCH (s:Schema)-[r:SCHEMA_FROM_CLUSTER]->(c:Cluster) " +
            "WHERE c.platformName = s.platformName  " +
            "AND c.clusterName = s.clusterName  " +
            "DELETE r")
    void deleteRelWithSchema();
}
