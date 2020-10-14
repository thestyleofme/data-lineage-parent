package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.Map;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.SchemaNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;
import org.isaac.lineage.neo4j.utils.JsonUtil;
import org.isaac.lineage.neo4j.utils.LineageUtil;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/23 11:53
 * @since 1.0.0
 */
public class CreateTableHandler {

    private CreateTableHandler() {
        throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    public static void handle(LineageMapping lineageMapping,
                              HiveHookMessage hiveHookMessage,
                              Map<String, Object> attributes) {
        Map<String, Object> hiveTableMap = (Map<String, Object>) attributes.get("hive_table");
        BaseAttribute createTableEvent = JsonUtil.toObj(JsonUtil.toJson(hiveTableMap), BaseAttribute.class);
        // 生成node信息
        genAllNode(lineageMapping, hiveHookMessage, createTableEvent);
        // node的冗余字段处理
        LineageUtil.doNodeNormal(lineageMapping, hiveHookMessage);
    }

    private static void genAllNode(LineageMapping lineageMapping,
                                   HiveHookMessage hiveHookMessage,
                                   BaseAttribute createTableEvent) {
        // platform cluster
        LineageUtil.genHivePlatformAndClusterNode(lineageMapping, hiveHookMessage);
        // schema
        genSchemaNode(lineageMapping, createTableEvent);
        // table
        genTableNode(lineageMapping, hiveHookMessage, createTableEvent);
        // field
        genFieldNode(lineageMapping, createTableEvent);
    }

    private static void genSchemaNode(LineageMapping lineageMapping,
                                      BaseAttribute createTableEvent) {
        SchemaNode schemaNode = SchemaNode.builder()
                .schemaName(createTableEvent.getDb())
                .build();
        lineageMapping.getSchemaNodeList().add(schemaNode);
    }

    private static void genTableNode(LineageMapping lineageMapping,
                                     HiveHookMessage hiveHookMessage,
                                     BaseAttribute createTableEvent) {
        TableNode tableNode = TableNode.builder()
                .schemaName(createTableEvent.getDb())
                .tableName(createTableEvent.getName())
                .sql(hiveHookMessage.getQueryStr())
                .build();
        lineageMapping.getTableNodeList().add(tableNode);
    }

    private static void genFieldNode(LineageMapping lineageMapping,
                                     BaseAttribute createTableEvent) {
        ArrayList<FieldNode> list = new ArrayList<>();
        createTableEvent.getColumns().forEach(columnsDTO -> {
            FieldNode fieldNode = FieldNode.builder().build();
            fieldNode.setSchemaName(createTableEvent.getDb());
            fieldNode.setTableName(createTableEvent.getName());
            fieldNode.setFieldName(columnsDTO.getName());
            fieldNode.setFieldType(columnsDTO.getType());
            list.add(fieldNode);
        });
        lineageMapping.getFieldNodeList().addAll(list);
    }

}
