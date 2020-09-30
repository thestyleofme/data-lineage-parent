package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.NodeQualifiedName;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.SchemaNode;
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
        // 生成node信息
        doCreateNode(lineageMapping, hiveHookMessage, createTableAsEvent);
        // node的冗余字段处理
        LineageUtil.doNodeNormal(lineageMapping, hiveHookMessage);
    }

    private static void doCreateNode(LineageMapping lineageMapping,
                                     HiveHookMessage hiveHookMessage,
                                     CreateTableAsEvent createTableAsEvent) {
        // platform cluster
        LineageUtil.genHivePlatformAndClusterNode(lineageMapping, hiveHookMessage);
        // db
        genSchemaNode(lineageMapping, createTableAsEvent);
        // table
        genTableNode(lineageMapping, hiveHookMessage, createTableAsEvent);
        // field
        genFieldNode(lineageMapping, createTableAsEvent);
    }

    private static void genSchemaNode(LineageMapping lineageMapping,
                                      CreateTableAsEvent createTableAsEvent) {
        ArrayList<SchemaNode> list = new ArrayList<>();
        createTableAsEvent.getInputs().forEach(inputsDTO ->
                list.add(SchemaNode.builder().schemaName(inputsDTO.getDb()).build())
        );
        createTableAsEvent.getOutputs().forEach(outputsDTO ->
                list.add(SchemaNode.builder().schemaName(outputsDTO.getDb()).build())
        );
        lineageMapping.setSchemaNodeList(list);
    }

    private static void genTableNode(LineageMapping lineageMapping,
                                     HiveHookMessage hiveHookMessage,
                                     CreateTableAsEvent createTableAsEvent) {
        ArrayList<TableNode> list = new ArrayList<>();
        String inputTable = null;
        String schema = null;
        for (CreateTableAsEvent.InputsDTO inputsDTO : createTableAsEvent.getInputs()) {
            inputTable = inputsDTO.getName();
            schema = inputsDTO.getDb();
            list.add(TableNode.builder()
                    .schemaName(schema)
                    .tableName(inputTable)
                    .build());
        }
        String queryStr = hiveHookMessage.getQueryStr().toLowerCase();
        String inputTablePk = NodeQualifiedName.ofTable(hiveHookMessage.getPlatformName(),
                hiveHookMessage.getClusterName(),
                Objects.requireNonNull(schema),
                inputTable).toString();
        for (CreateTableAsEvent.OutputsDTO outputsDTO : createTableAsEvent.getOutputs()) {
            if (queryStr.startsWith("create table")) {
                // create table as select
                list.add(TableNode.builder()
                        .schemaName(outputsDTO.getDb())
                        .tableName(outputsDTO.getName())
                        .sql(queryStr)
                        .createTableFrom(inputTablePk)
                        .build());
            } else if (queryStr.startsWith("insert overwrite")) {
                // insert overwrite
                list.add(TableNode.builder()
                        .schemaName(outputsDTO.getDb())
                        .tableName(outputsDTO.getName())
                        .insertOverwriteSql(queryStr)
                        .insertOverwriteFrom(inputTablePk)
                        .build());
            }
        }
        lineageMapping.setTableNodeList(list);
    }

    private static void genFieldNode(LineageMapping lineageMapping,
                                     CreateTableAsEvent createTableAsEvent) {
        ArrayList<FieldNode> list = new ArrayList<>();
        createTableAsEvent.getOutputs().forEach(outputsDTO ->
                outputsDTO.getColumns().forEach(columnsDTO -> {
                    FieldNode fieldNode = genColumn(columnsDTO);
                    fieldNode.setSchemaName(outputsDTO.getDb());
                    fieldNode.setTableName(outputsDTO.getName());
                    list.add(fieldNode);
                }));
        createTableAsEvent.getInputs().forEach(inputsDTO ->
                inputsDTO.getColumns().forEach(columnsDTO -> {
                    FieldNode fieldNode = genColumn(columnsDTO);
                    fieldNode.setSchemaName(inputsDTO.getDb());
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

}
