package org.isaac.lineage.neo4j.kafka.handler.hive;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.annotation.SourceType;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.kafka.handler.BaseLineageHandler;
import org.isaac.lineage.neo4j.repository.node.TableRepository;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/14 9:45
 * @since 1.0.0
 */
@SourceType("HIVE-HOOK")
@Slf4j
@Component
public class HiveLineageHandler implements BaseLineageHandler {

    private final TableRepository tableRepository;

    public HiveLineageHandler(TableRepository tableRepository) {
        this.tableRepository = tableRepository;
    }

    @Override
    public void handle(LineageMapping lineageMapping) {
        // CREATE_TABLE_AS_SELECT
        tableRepository.deleteRelCreateTableAsSelect();
        tableRepository.createRelCreateTableAsSelect();
        // INSERT_OVERWRITE_TABLE_SELECT
        tableRepository.deleteRelInsertOverwriteTableSelect();
        tableRepository.createRelInsertOverwriteTableSelect();
    }
}
