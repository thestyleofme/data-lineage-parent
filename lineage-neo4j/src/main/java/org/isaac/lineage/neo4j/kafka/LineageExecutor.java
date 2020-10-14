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
    private final ProcessRepository processRepository;

    public LineageExecutor(PlatformRepository platformRepository,
                           ClusterRepository clusterRepository,
                           SchemaRepository schemaRepository,
                           TableRepository tableRepository,
                           FieldRepository fieldRepository,
                           ProcessRepository processRepository) {
        this.platformRepository = platformRepository;
        this.clusterRepository = clusterRepository;
        this.schemaRepository = schemaRepository;
        this.tableRepository = tableRepository;
        this.fieldRepository = fieldRepository;
        this.processRepository = processRepository;
    }

    public void handle(LineageMapping lineageMapping) {
        // 创建节点 已存在该节点则更新
        saveNode(lineageMapping);
        // 创建关系 先删除以前的关系 重新生成
        saveNormalRelationship();
    }

    private void saveNode(LineageMapping lineageMapping) {
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
        // process
        Optional.ofNullable(lineageMapping.getProcessNodeList())
                .ifPresent(processNodes -> processNodes.forEach(this::handleProcessNode));
    }

    private void handleProcessNode(ProcessNode processNode) {
        processRepository.save(processNode);
        // PROCESS_INPUT
        String processNodePk = processNode.getPk();
        processNode.getSourceNodePkList().forEach(tablePk -> {
                    Optional<ProcessNode> optional = processRepository.existsRelProcessInput(tablePk, processNodePk);
                    if (!optional.isPresent()) {
                        processRepository.createRelProcessInput(tablePk, processNodePk);
                    }
                }
        );
        // PROCESS_OUTPUT
        String targetNodePk = processNode.getTargetNodePk();
        Optional<ProcessNode> optional = processRepository.existsProcessOutput(processNodePk, targetNodePk);
        if (!optional.isPresent()) {
            processRepository.createRelProcessOutput(processNodePk, targetNodePk);
        }
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

    private void saveNormalRelationship() {
        // CLUSTER_FROM_PLATFORM
        platformRepository.deleteRelWithCluster();
        platformRepository.createRelWithCluster();
        // SCHEMA_FROM_CLUSTER
        clusterRepository.deleteRelWithSchema();
        clusterRepository.createRelWithSchema();
        // TABLE_FROM_SCHEMA
        schemaRepository.deleteRelWithTable();
        schemaRepository.createRelWithTable();
        // FIELD_FROM_TABLE
        tableRepository.deleteRelWithField();
        tableRepository.createRelWithField();
    }

}
