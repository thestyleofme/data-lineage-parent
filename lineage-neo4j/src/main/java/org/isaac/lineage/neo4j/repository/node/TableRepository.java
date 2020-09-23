package org.isaac.lineage.neo4j.repository.node;

import org.isaac.lineage.neo4j.domain.node.TableNode;
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
public interface TableRepository extends Neo4jRepository<TableNode, Long> {

    /**
     * 给Table节点设置唯一约束
     */
    @Query("CREATE CONSTRAINT ON (table:Table) ASSERT table.pk IS UNIQUE")
    void createConstraint();

    /**
     * 创建与field之间的关系
     */
    @Query("MATCH (field:Field),(table:Table) " +
            "WHERE field.platform = table.platform " +
            "AND field.cluster = table.cluster " +
            "AND field.databaseName = table.databaseName " +
            "AND field.tableName = table.tableName " +
            "CREATE (field)-[:FIELD_FROM_TABLE]->(table)")
    void createRelationshipWithField();

    /**
     * 刷新与field之间的关系
     */
    @Query("MATCH (field:Field)-[r:FIELD_FROM_TABLE]->(table:Table) " +
            "WHERE field.platform = table.platform " +
            "AND field.cluster = table.cluster " +
            "AND field.databaseName = table.databaseName " +
            "AND field.tableName = table.tableName " +
            "DELETE r " +
            "CREATE (field)-[:FIELD_FROM_TABLE]->(table)")
    void refreshRelationshipWithField();
}
