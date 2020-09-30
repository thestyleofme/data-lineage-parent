package org.isaac.lineage.neo4j.kafka;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.*;
import org.isaac.lineage.neo4j.repository.node.*;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/23 17:24
 * @since 1.0.0
 */
@Component
@Slf4j
public class LineageExecutor {

    private final PlatformRepository platformRepository;
    private final ClusterRepository clusterRepository;
    private final SchemaRepository schemaRepository;
    private final TableRepository tableRepository;
    private final FieldRepository fieldRepository;

    public LineageExecutor(PlatformRepository platformRepository,
                           ClusterRepository clusterRepository,
                           SchemaRepository schemaRepository,
                           TableRepository tableRepository,
                           FieldRepository fieldRepository) {
        this.platformRepository = platformRepository;
        this.clusterRepository = clusterRepository;
        this.schemaRepository = schemaRepository;
        this.tableRepository = tableRepository;
        this.fieldRepository = fieldRepository;
    }

    public void handle(LineageMapping lineageMapping) {
        // 创建节点 已存在该节点则忽略
        createNode(lineageMapping);
        // 创建关系 先删除以前的关系 重新生成
        createRelationship();
    }

    private void createNode(LineageMapping lineageMapping) {
        // platform
        Optional.ofNullable(lineageMapping.getPlatformNodeList())
                .ifPresent(platformNodes -> platformNodes.forEach(this::handlePlatformNode));
        // cluster
        Optional.ofNullable(lineageMapping.getClusterNodeList())
                .ifPresent(clusterNodes -> clusterNodes.forEach(this::handleClusterNode));
        // schema
        Optional.ofNullable(lineageMapping.getSchemaNodeList())
                .ifPresent(schemaNodes -> schemaNodes.forEach(this::handleSchemaNode));
        // table
        Optional.ofNullable(lineageMapping.getTableNodeList())
                .ifPresent(tableNodes -> tableNodes.forEach(this::handleTableNode));
        // field
        Optional.ofNullable(lineageMapping.getFieldNodeList())
                .ifPresent(fieldNodes -> fieldNodes.forEach(this::handleFieldNode));
    }

    private void handlePlatformNode(PlatformNode platformNode) {
        platformRepository.save(platformNode);
    }

    private void handleClusterNode(ClusterNode clusterNode) {
        clusterRepository.save(clusterNode);
    }

    private void handleSchemaNode(SchemaNode schemaNode) {
        schemaRepository.save(schemaNode);
    }

    private void handleTableNode(TableNode tableNode) {
        tableRepository.save(tableNode);
    }

    private void handleFieldNode(FieldNode fieldNode) {
        fieldRepository.save(fieldNode);
    }

    private void createRelationship() {
        // CLUSTER_FROM_PLATFORM
        platformRepository.deleteRelationshipWithCluster();
        platformRepository.createRelationshipWithCluster();
        // SCHEMA_FROM_CLUSTER
        clusterRepository.deleteRelationshipWithSchema();
        clusterRepository.createRelationshipWithSchema();
        // TABLE_FROM_SCHEMA
        schemaRepository.deleteRelationshipWithTable();
        schemaRepository.createRelationshipWithTable();
        // FIELD_FROM_TABLE
        tableRepository.deleteRelationshipWithField();
        tableRepository.createRelationshipWithField();
        // CREATE_TABLE_AS_SELECT
        tableRepository.deleteRelationshipCreateTableAsSelect();
        tableRepository.createRelationshipCreateTableAsSelect();
        // INSERT_OVERWRITE_TABLE_SELECT
        tableRepository.deleteRelationshipInsertOverwriteTableSelect();
        tableRepository.createRelationshipInsertOverwriteTableSelect();
    }

}
