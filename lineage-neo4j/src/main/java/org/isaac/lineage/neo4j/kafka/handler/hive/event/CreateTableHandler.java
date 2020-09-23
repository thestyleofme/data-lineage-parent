package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveEventType;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;
import org.isaac.lineage.neo4j.utils.JsonUtil;

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
        genNormal(lineageMapping, hiveHookMessage);
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
        // todo extConfig
        lineageMapping.setTableNodeList(Collections.singletonList(tableNode));
    }

    private static void genDbNode(LineageMapping lineageMapping, CreateTableEvent createTableEvent) {
        DatabaseNode databaseNode = DatabaseNode.builder().build();
        databaseNode.setDatabaseName(createTableEvent.getDb());
        lineageMapping.setDatabaseNode(databaseNode);
    }

    private static void genNormal(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        String platform = hiveHookMessage.getPlatform();
        String cluster = hiveHookMessage.getCluster();
        // db
        DatabaseNode databaseNode = lineageMapping.getDatabaseNode();
        Optional.ofNullable(platform).ifPresent(databaseNode::setPlatform);
        Optional.ofNullable(cluster).ifPresent(databaseNode::setCluster);
        databaseNode.setPk(String.format(DatabaseNode.PK_FORMAT,
                databaseNode.getPlatform(),
                databaseNode.getCluster(),
                databaseNode.getDatabaseName()));
        // table
        lineageMapping.getTableNodeList().forEach(tableNode -> {
            Optional.ofNullable(platform).ifPresent(tableNode::setPlatform);
            Optional.ofNullable(cluster).ifPresent(tableNode::setCluster);
            tableNode.setPk(String.format(TableNode.PK_FORMAT,
                    tableNode.getPlatform(),
                    tableNode.getCluster(),
                    tableNode.getDatabaseName(),
                    tableNode.getTableName()));
        });
        // field
        lineageMapping.getFieldNodeList().forEach(fieldNode -> {
            Optional.ofNullable(platform).ifPresent(fieldNode::setPlatform);
            Optional.ofNullable(cluster).ifPresent(fieldNode::setCluster);
            fieldNode.setPk(String.format(FieldNode.PK_FORMAT,
                    fieldNode.getPlatform(),
                    fieldNode.getCluster(),
                    fieldNode.getDatabaseName(),
                    fieldNode.getTableName(),
                    fieldNode.getFieldName()));
        });
    }

}
