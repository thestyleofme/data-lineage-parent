package org.isaac.lineage.neo4j.utils;

import java.util.Optional;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/24 13:51
 * @since 1.0.0
 */
public class LineageUtil {

    private LineageUtil(){
        throw new IllegalStateException();
    }

    public static void genNormalDbNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage){
        // db
        lineageMapping.getDatabaseNodeList().forEach(databaseNode -> {
            Optional.ofNullable(hiveHookMessage.getPlatform()).ifPresent(databaseNode::setPlatform);
            Optional.ofNullable(hiveHookMessage.getCluster()).ifPresent(databaseNode::setCluster);
            databaseNode.setPk(String.format(DatabaseNode.PK_FORMAT,
                    databaseNode.getPlatform(),
                    databaseNode.getCluster(),
                    databaseNode.getDatabaseName()));
        });
    }

    public static void genNormalTableNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage){
        // table
        lineageMapping.getTableNodeList().forEach(tableNode -> {
            Optional.ofNullable(hiveHookMessage.getPlatform()).ifPresent(tableNode::setPlatform);
            Optional.ofNullable(hiveHookMessage.getCluster()).ifPresent(tableNode::setCluster);
            tableNode.setPk(String.format(TableNode.PK_FORMAT,
                    tableNode.getPlatform(),
                    tableNode.getCluster(),
                    tableNode.getDatabaseName(),
                    tableNode.getTableName()));
        });
    }

    public static void genNormalFieldNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        // field
        lineageMapping.getFieldNodeList().forEach(fieldNode -> {
            Optional.ofNullable(hiveHookMessage.getPlatform()).ifPresent(fieldNode::setPlatform);
            Optional.ofNullable(hiveHookMessage.getCluster()).ifPresent(fieldNode::setCluster);
            fieldNode.setPk(String.format(FieldNode.PK_FORMAT,
                    fieldNode.getPlatform(),
                    fieldNode.getCluster(),
                    fieldNode.getDatabaseName(),
                    fieldNode.getTableName(),
                    fieldNode.getFieldName()));
        });
    }
}
