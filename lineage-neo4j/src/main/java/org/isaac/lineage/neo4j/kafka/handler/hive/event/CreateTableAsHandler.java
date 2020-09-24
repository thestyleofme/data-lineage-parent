package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;
import org.isaac.lineage.neo4j.utils.JsonUtil;
import org.isaac.lineage.neo4j.utils.LineageUtil;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/24 10:10
 * @since 1.0.0
 */
@Slf4j
public class CreateTableAsHandler {

    private CreateTableAsHandler() {
        throw new IllegalStateException();
    }

    public static void handle(LineageMapping lineageMapping,
                              HiveHookMessage hiveHookMessage,
                              Map<String, Object> attributes) {
        CreateTableAsEvent createTableAsEvent = JsonUtil.toObj(JsonUtil.toJson(attributes), CreateTableAsEvent.class);
        // db
        genDbNode(lineageMapping, createTableAsEvent);
        // table
        genTableNode(lineageMapping, hiveHookMessage, createTableAsEvent);
        // field
        genFieldNode(lineageMapping, createTableAsEvent);
        // normal
        LineageUtil.genNormalDbNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalTableNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalFieldNode(lineageMapping, hiveHookMessage);
    }

    private static void genFieldNode(LineageMapping lineageMapping, CreateTableAsEvent createTableAsEvent) {
        ArrayList<FieldNode> list = new ArrayList<>();
        createTableAsEvent.getOutputs().forEach(outputsDTO ->
                outputsDTO.getColumns().forEach(columnsDTO -> {
                    FieldNode fieldNode = genColumn(columnsDTO);
                    fieldNode.setDatabaseName(outputsDTO.getDb());
                    fieldNode.setTableName(outputsDTO.getName());
                    list.add(fieldNode);
                }));
        createTableAsEvent.getInputs().forEach(inputsDTO ->
                inputsDTO.getColumns().forEach(columnsDTO -> {
                    FieldNode fieldNode = genColumn(columnsDTO);
                    fieldNode.setDatabaseName(inputsDTO.getDb());
                    fieldNode.setTableName(inputsDTO.getName());
                    list.add(fieldNode);
                }));
        lineageMapping.setFieldNodeList(list);
    }

    private static FieldNode genColumn(CreateTableAsEvent.ColumnsDTO columnsDTO) {
        return FieldNode.builder()
                .fieldName(columnsDTO.getName())
                .fieldType(columnsDTO.getType())
                .build();
    }

    private static void genTableNode(LineageMapping lineageMapping,
                                     HiveHookMessage hiveHookMessage,
                                     CreateTableAsEvent createTableAsEvent) {
        ArrayList<TableNode> list = new ArrayList<>();
        String createTableFrom = null;
        for (CreateTableAsEvent.InputsDTO inputsDTO : createTableAsEvent.getInputs()) {
            createTableFrom = inputsDTO.getName();
            list.add(TableNode.builder()
                    .databaseName(inputsDTO.getDb())
                    .tableName(inputsDTO.getName())
                    .build());
        }
        for (CreateTableAsEvent.OutputsDTO outputsDTO : createTableAsEvent.getOutputs()) {
            list.add(TableNode.builder()
                    .databaseName(outputsDTO.getDb())
                    .tableName(outputsDTO.getName())
                    .sql(hiveHookMessage.getQueryStr())
                    .createTableFrom(createTableFrom)
                    .build());
        }
        lineageMapping.setTableNodeList(list);
    }

    private static void genDbNode(LineageMapping lineageMapping, CreateTableAsEvent createTableAsEvent) {
        ArrayList<DatabaseNode> list = new ArrayList<>();
        createTableAsEvent.getInputs().forEach(inputsDTO ->
                list.add(DatabaseNode.builder().databaseName(inputsDTO.getDb()).build())
        );
        createTableAsEvent.getOutputs().forEach(outputsDTO ->
                list.add(DatabaseNode.builder().databaseName(outputsDTO.getDb()).build())
        );
        lineageMapping.setDatabaseNodeList(list);
    }

}
