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
     * 根据pk判断节点是否存在
     *
     * @param pk 主键
     * @return true/false
     */
    boolean existsByPk(String pk);

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
            "DELETE r")
    void deleteRelationshipWithField();

    /**
     * 创建与table之间的关系
     */
    @Query("MATCH (table1:Table),(table2:Table) " +
            "WHERE table1.platform = table2.platform " +
            "AND table1.cluster = table2.cluster " +
            "AND table1.databaseName = table2.databaseName " +
            "AND table1.createTableFrom = table2.tableName " +
            "CREATE (table1)-[:CREATE_TABLE_AS_SELECT]->(table2)")
    void createRelationshipWithTable();

    /**
     * 刷新与table之间的关系
     */
    @Query("MATCH (table1:Table)-[r:CREATE_TABLE_AS_SELECT]->(table2:Table) " +
            "WHERE table1.platform = table2.platform " +
            "AND table1.cluster = table2.cluster " +
            "AND table1.databaseName = table2.databaseName " +
            "AND table1.createTableFrom = table2.tableName " +
            "DELETE r")
    void deleteRelationshipWithTable();
}
