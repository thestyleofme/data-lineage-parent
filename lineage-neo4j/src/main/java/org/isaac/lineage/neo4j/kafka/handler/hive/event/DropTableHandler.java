package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.List;
import java.util.Map;

import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.SchemaNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;
import org.isaac.lineage.neo4j.utils.LineageUtil;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/14 10:35
 * @since 1.0.0
 */
public class DropTableHandler {

    private DropTableHandler() {
        throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    public static void handler(LineageMapping lineageMapping,
                               HiveHookMessage hiveHookMessage,
                               Map<String, Object> attributes) {
        List<String> dropTableList = (List<String>) attributes.get("dropTable");
        for (String dbAndTable : dropTableList) {
            // 生成node信息
            genAllNode(lineageMapping, hiveHookMessage, dbAndTable);
        }
        // node的冗余字段处理
        LineageUtil.doNodeNormal(lineageMapping, hiveHookMessage);
    }

    private static void genAllNode(LineageMapping lineageMapping,
                                   HiveHookMessage hiveHookMessage,
                                   String dbAndTable) {
        // platform cluster
        LineageUtil.genHivePlatformAndClusterNode(lineageMapping, hiveHookMessage);
        String[] splits = dbAndTable.split("\\.");
        String db = splits[0];
        String table = splits[1];
        // schema
        genSchemaNode(lineageMapping, db);
        // table
        genTableNode(lineageMapping, hiveHookMessage, db, table);
    }

    private static void genTableNode(LineageMapping lineageMapping,
                                     HiveHookMessage hiveHookMessage,
                                     String db,
                                     String table) {
        TableNode tableNode = TableNode.builder()
                .schemaName(db)
                .tableName(table)
                .sql(hiveHookMessage.getQueryStr())
                .build();
        // status赋值为DELETED
        tableNode.setStatus(NeoConstant.Status.DELETED);
        lineageMapping.getTableNodeList().add(tableNode);
    }

    private static void genSchemaNode(LineageMapping lineageMapping, String db) {
        SchemaNode schemaNode = SchemaNode.builder()
                .schemaName(db)
                .build();
        lineageMapping.getSchemaNodeList().add(schemaNode);
    }
}
