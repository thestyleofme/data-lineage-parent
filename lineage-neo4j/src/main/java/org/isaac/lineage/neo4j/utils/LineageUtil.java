package org.isaac.lineage.neo4j.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.NodeQualifiedName;
import org.isaac.lineage.neo4j.domain.node.ClusterNode;
import org.isaac.lineage.neo4j.domain.node.PlatformNode;
import org.isaac.lineage.neo4j.domain.node.ProcessNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;
import org.springframework.util.DigestUtils;

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
        LineageUtil.genNormalTableNode(lineageMapping.getTableNodeList(), hiveHookMessage);
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

    public static void genNormalTableNode(List<TableNode> tableNodeList, HiveHookMessage hiveHookMessage) {
        // table
        tableNodeList.forEach(tableNode ->
                doNormalTableNode(hiveHookMessage, tableNode)
        );
    }

    public static void doNormalTableNode(HiveHookMessage hiveHookMessage, TableNode tableNode) {
        Optional.ofNullable(hiveHookMessage.getPlatformName()).ifPresent(tableNode::setPlatformName);
        Optional.ofNullable(hiveHookMessage.getClusterName()).ifPresent(tableNode::setClusterName);
        tableNode.setPk(NodeQualifiedName.ofTable(tableNode.getPlatformName(),
                tableNode.getClusterName(),
                tableNode.getSchemaName(),
                tableNode.getTableName()).toString());
    }

    public static void doNormalProcessNode(HiveHookMessage hiveHookMessage, ProcessNode processNode) {
        Optional.ofNullable(hiveHookMessage.getPlatformName()).ifPresent(processNode::setPlatformName);
        Optional.ofNullable(hiveHookMessage.getClusterName()).ifPresent(processNode::setClusterName);
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

    public static String genProcessNodePk(ProcessNode processNode) {
        // sourceNodePkList：x,y
        // targetNodePk: z
        // md5(targetNodePk + sourceNodePkList排序后使用下划线'_'连接)
        List<String> sourceNodePkList = processNode.getSourceNodePkList();
        sourceNodePkList.sort(Comparator.naturalOrder());
        String key = processNode.getTargetNodePk() + "_" + String.join("_", sourceNodePkList);
        return DigestUtils.md5DigestAsHex(key.getBytes());
    }
}
