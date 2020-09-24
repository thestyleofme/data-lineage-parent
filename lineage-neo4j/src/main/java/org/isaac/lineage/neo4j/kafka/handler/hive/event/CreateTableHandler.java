package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveEventType;
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
        Map<String, Object> hiveTableMap = (Map<String, Object>) attributes.get(HiveEventType.HIVE_TABLE.getName());
        CreateTableEvent createTableEvent = JsonUtil.toObj(JsonUtil.toJson(hiveTableMap), CreateTableEvent.class);
        // db
        genDbNode(lineageMapping, createTableEvent);
        // table
        genTableNode(lineageMapping, hiveHookMessage, createTableEvent);
        // field
        genFieldNode(lineageMapping, createTableEvent);
        // normal
        LineageUtil.genNormalDbNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalTableNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalFieldNode(lineageMapping, hiveHookMessage);
    }

    private static void genFieldNode(LineageMapping lineageMapping,
                                     CreateTableEvent createTableEvent) {
        ArrayList<FieldNode> list = new ArrayList<>();
        createTableEvent.getColumns().forEach(columnsDTO -> {
            FieldNode fieldNode = FieldNode.builder().build();
            fieldNode.setDatabaseName(createTableEvent.getDb());
            fieldNode.setTableName(createTableEvent.getName());
            fieldNode.setFieldName(columnsDTO.getName());
            fieldNode.setFieldType(columnsDTO.getType());
            list.add(fieldNode);
        });
        lineageMapping.setFieldNodeList(list);
    }

    private static void genTableNode(LineageMapping lineageMapping,
                                     HiveHookMessage hiveHookMessage,
                                     CreateTableEvent createTableEvent) {
        TableNode tableNode = TableNode.builder().build();
        tableNode.setDatabaseName(createTableEvent.getDb());
        tableNode.setTableName(createTableEvent.getName());
        tableNode.setSql(hiveHookMessage.getQueryStr());
        lineageMapping.setTableNodeList(Collections.singletonList(tableNode));
    }

    private static void genDbNode(LineageMapping lineageMapping, CreateTableEvent createTableEvent) {
        DatabaseNode databaseNode = DatabaseNode.builder().build();
        databaseNode.setDatabaseName(createTableEvent.getDb());
        lineageMapping.setDatabaseNodeList(Collections.singletonList(databaseNode));
    }


}
