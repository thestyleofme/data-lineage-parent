package org.isaac.lineage.neo4j.utils;

import java.util.Collections;
import java.util.Optional;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.NodeQualifiedName;
import org.isaac.lineage.neo4j.domain.node.ClusterNode;
import org.isaac.lineage.neo4j.domain.node.PlatformNode;
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

    private LineageUtil() {
        throw new IllegalStateException();
    }

    public static void genHivePlatformAndClusterNode(LineageMapping lineageMapping,
                                                     HiveHookMessage hiveHookMessage) {
        // platform
        String platformName = hiveHookMessage.getPlatformName();
        PlatformNode platformNode = PlatformNode.builder().build();
        Optional.ofNullable(platformName).ifPresent(platformNode::setPlatformName);
        platformNode.setPk(platformNode.getPlatformName());
        lineageMapping.setPlatformNodeList(Collections.singletonList(platformNode));
        // cluster
        String clusterName = hiveHookMessage.getClusterName();
        ClusterNode clusterNode = ClusterNode.builder()
                .platformName(platformNode.getPlatformName())
                .build();
        Optional.ofNullable(clusterName).ifPresent(clusterNode::setClusterName);
        clusterNode.setPk(NodeQualifiedName.ofCluster(platformNode.getPlatformName(),
                clusterNode.getClusterName()).toString());
        lineageMapping.setClusterNodeList(Collections.singletonList(clusterNode));
    }

    public static void doNodeNormal(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        LineageUtil.genNormalSchemaNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalTableNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalFieldNode(lineageMapping, hiveHookMessage);
    }

    public static void genNormalSchemaNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        // schema
        lineageMapping.getSchemaNodeList().forEach(schemaNode -> {
            Optional.ofNullable(hiveHookMessage.getPlatformName()).ifPresent(schemaNode::setPlatformName);
            Optional.ofNullable(hiveHookMessage.getClusterName()).ifPresent(schemaNode::setClusterName);
            schemaNode.setPk(NodeQualifiedName.ofSchema(schemaNode.getPlatformName(),
                    schemaNode.getClusterName(),
                    schemaNode.getSchemaName()).toString());
        });
    }

    public static void genNormalTableNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        // table
        lineageMapping.getTableNodeList().forEach(tableNode -> {
            Optional.ofNullable(hiveHookMessage.getPlatformName()).ifPresent(tableNode::setPlatformName);
            Optional.ofNullable(hiveHookMessage.getClusterName()).ifPresent(tableNode::setClusterName);
            tableNode.setPk(NodeQualifiedName.ofTable(tableNode.getPlatformName(),
                    tableNode.getClusterName(),
                    tableNode.getSchemaName(),
                    tableNode.getTableName()).toString());
        });
    }

    public static void genNormalFieldNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        // field
        lineageMapping.getFieldNodeList().forEach(fieldNode -> {
            Optional.ofNullable(hiveHookMessage.getPlatformName()).ifPresent(fieldNode::setPlatformName);
            Optional.ofNullable(hiveHookMessage.getClusterName()).ifPresent(fieldNode::setClusterName);
            fieldNode.setPk(NodeQualifiedName.ofField(fieldNode.getPlatformName(),
                    fieldNode.getClusterName(),
                    fieldNode.getSchemaName(),
                    fieldNode.getTableName(),
                    fieldNode.getFieldName()).toString());
        });
    }
}
