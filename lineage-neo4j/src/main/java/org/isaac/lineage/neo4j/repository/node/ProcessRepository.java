package org.isaac.lineage.neo4j.repository.node;

import java.util.Optional;

import org.isaac.lineage.neo4j.domain.node.ProcessNode;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/10 14:26
 * @since 1.0.0
 */
@Repository
public interface ProcessRepository extends Neo4jRepository<ProcessNode, String> {

    /**
     * PROCESS_INPUT
     *
     * @param tablePk   tableNode.pk
     * @param processPk processNode.pk
     */
    @Query("MATCH (t:Table),(p:Process) " +
            "WHERE t.platformName = p.platformName  " +
            "AND t.clusterName = p.clusterName  " +
            "AND t.pk = $tablePk  " +
            "AND p.pk = $processPk  " +
            "CREATE (t)-[r:PROCESS_INPUT]->(p)")
    void createRelProcessInput(@Param("tablePk") String tablePk, @Param("processPk") String processPk);

    /**
     * PROCESS_INPUT
     *
     * @param tablePk   tableNode.pk
     * @param processPk processNode.pk
     * @return ProcessNode
     */
    @Query("MATCH (t:Table)-[r:PROCESS_INPUT]->(p:Process) " +
            "WHERE t.platformName = p.platformName  " +
            "AND t.clusterName = p.clusterName  " +
            "AND t.pk = $tablePk  " +
            "AND p.pk = $processPk  " +
            "return p")
    Optional<ProcessNode> existsRelProcessInput(@Param("tablePk") String tablePk, @Param("processPk") String processPk);

    /**
     * PROCESS_OUTPUT
     *
     * @param tablePk   tableNode.pk
     * @param processPk processNode.pk
     */
    @Query("MATCH (t:Table),(p:Process) " +
            "WHERE t.platformName = p.platformName  " +
            "AND t.clusterName = p.clusterName  " +
            "AND t.pk = $tablePk  " +
            "AND p.pk = $processPk  " +
            "CREATE (p)-[r:PROCESS_OUTPUT]->(t)")
    void createRelProcessOutput(@Param("processPk") String processPk, @Param("tablePk") String tablePk);

    /**
     * PROCESS_OUT
     *
     * @param tablePk   tableNode.pk
     * @param processPk processNode.pk
     * @return ProcessNode
     */
    @Query("MATCH (p:Process)-[r:PROCESS_OUTPUT]->(t:Table) " +
            "WHERE t.platformName = p.platformName  " +
            "AND t.clusterName = p.clusterName  " +
            "AND t.pk = $tablePk  " +
            "AND p.pk = $processPk  " +
            "return p")
    Optional<ProcessNode> existsProcessOutput(@Param("processPk") String processPk, @Param("tablePk") String tablePk);
}
