package org.isaac.lineage.neo4j.repository.node;

import java.util.List;

import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.result.DbQueryResult;
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
public interface DatabaseRepository extends Neo4jRepository<DatabaseNode, Long> {

    /**
     * 根据pk判断节点是否存在
     *
     * @param pk 主键
     * @return true/false
     */
    boolean existsByPk(String pk);

    /**
     * 给Database节点设置唯一约束
     */
    @Query("CREATE CONSTRAINT ON (db:Database) ASSERT db.pk IS UNIQUE")
    void createConstraint();

    /**
     * 创建与table之间的关系
     */
    @Query("MATCH (db:Database),(table:Table) " +
            "WHERE table.platform = db.platform  " +
            "AND table.cluster = db.cluster " +
            "AND table.databaseName = db.databaseName " +
            "CREATE (table)-[r:TABLE_FROM_DATABASE]->(db)")
    void createRelationshipWithTable();

    /**
     * 刷新与table之间的关系
     */
    @Query("MATCH (table:Table)-[r:TABLE_FROM_DATABASE]->(db:Database) " +
            "WHERE table.platform = db.platform " +
            "AND table.cluster = db.cluster " +
            "AND table.databaseName = db.databaseName " +
            "DELETE r")
    void deleteRelationshipWithTable();

    /**
     * 血缘分析查找该库下有哪些表
     *
     * @param databaseName 库
     * @return DbQueryResult
     */
    @Query("MATCH (table:Table)-[r:TABLE_FROM_DATABASE]->(db:Database) " +
            "WHERE db.databaseName = $0" +
            "RETURN db,table")
    List<DbQueryResult> queryTables(String databaseName);
}
