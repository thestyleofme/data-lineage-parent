package org.isaac.lineage.neo4j.repository.node;

import java.util.List;

import org.isaac.lineage.neo4j.domain.node.SchemaNode;
import org.isaac.lineage.neo4j.domain.result.SchemaQueryResult;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/16 10:54
 * @since 1.0.0
 */
@Repository
public interface SchemaRepository extends Neo4jRepository<SchemaNode, String> {

    /**
     * TABLE_FROM_SCHEMA
     */
    @Query("MATCH (s:Schema),(table:Table) " +
            "WHERE table.platformName = s.platformName  " +
            "AND table.clusterName = s.clusterName " +
            "AND table.schemaName = s.schemaName " +
            "CREATE (table)-[r:TABLE_FROM_SCHEMA]->(s)")
    void createRelWithTable();

    /**
     * TABLE_FROM_SCHEMA
     */
    @Query("MATCH (table:Table)-[r:TABLE_FROM_SCHEMA]->(s:Schema) " +
            "WHERE table.platformName = s.platformName " +
            "AND table.clusterName = s.clusterName " +
            "AND table.schemaName = s.schemaName " +
            "DELETE r")
    void deleteRelWithTable();

    /**
     * 血缘分析查找该库下有哪些表
     *
     * @param schemaName 库
     * @return DbQueryResult
     */
    @Query("MATCH (table:Table)-[r:TABLE_FROM_SCHEMA]->(s:Schema) " +
            "WHERE s.schemaName = $0" +
            "RETURN s,table")
    List<SchemaQueryResult> queryTables(String schemaName);
}
