package org.isaac.lineage.neo4j.kafka;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.repository.node.DatabaseRepository;
import org.isaac.lineage.neo4j.repository.node.FieldRepository;
import org.isaac.lineage.neo4j.repository.node.TableRepository;
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

    private final DatabaseRepository databaseRepository;
    private final TableRepository tableRepository;
    private final FieldRepository fieldRepository;

    public LineageExecutor(DatabaseRepository databaseRepository,
                           TableRepository tableRepository,
                           FieldRepository fieldRepository) {
        this.databaseRepository = databaseRepository;
        this.tableRepository = tableRepository;
        this.fieldRepository = fieldRepository;
    }

    public void handle(LineageMapping lineageMapping) {
        // 创建节点 已存在该节点则忽略
        createNode(lineageMapping);
        // 创建关系 先删除以前的关系 重新生成
        createRelationship();
    }

    private void createRelationship() {
        // TABLE_FROM_DATABASE
        databaseRepository.deleteRelationshipWithTable();
        databaseRepository.createRelationshipWithTable();
        // FIELD_FROM_TABLE
        tableRepository.deleteRelationshipWithField();
        tableRepository.createRelationshipWithField();
        // CREATE_TABLE_AS
        tableRepository.deleteRelationshipCreateTableAsSelect();
        tableRepository.createRelationshipCreateTableAsSelect();
        // INSERT_OVERWRITE_TABLE_SELECT
        tableRepository.deleteRelationshipInsertOverwriteTableSelect();
        tableRepository.createRelationshipInsertOverwriteTableSelect();
    }

    private void handleDbNode(DatabaseNode databaseNode) {
        if (!databaseRepository.existsByPk(databaseNode.getPk())) {
            databaseRepository.save(databaseNode);
            databaseRepository.createConstraint();
        }
    }

    private void handleTableNode(TableNode tableNode) {
        if (!tableRepository.existsByPk(tableNode.getPk())) {
            tableRepository.save(tableNode);
            tableRepository.createConstraint();
        }
    }

    private void handleFieldNode(FieldNode fieldNode) {
        if (!fieldRepository.existsByPk(fieldNode.getPk())) {
            fieldRepository.save(fieldNode);
            fieldRepository.createConstraint();
        }
    }

    private void createNode(LineageMapping lineageMapping) {
        // db
        Optional.ofNullable(lineageMapping.getDatabaseNodeList())
                .ifPresent(databaseNodes -> databaseNodes.forEach(this::handleDbNode));
        // table
        Optional.ofNullable(lineageMapping.getTableNodeList())
                .ifPresent(tableNodes -> tableNodes.forEach(this::handleTableNode));
        // field
        Optional.ofNullable(lineageMapping.getFieldNodeList())
                .ifPresent(fieldNodes -> fieldNodes.forEach(this::handleFieldNode));
    }

}
