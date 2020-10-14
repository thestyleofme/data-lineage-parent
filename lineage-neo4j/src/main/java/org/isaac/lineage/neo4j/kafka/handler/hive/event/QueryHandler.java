package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.NodeQualifiedName;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveHookMessage;
import org.isaac.lineage.neo4j.utils.JsonUtil;
import org.isaac.lineage.neo4j.utils.LineageUtil;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/14 11:19
 * @since 1.0.0
 */
public class QueryHandler {

    private QueryHandler() {
        throw new IllegalStateException();
    }

    public static void handle(LineageMapping lineageMapping,
                              HiveHookMessage hiveHookMessage,
                              Map<String, Object> attributes) {
        HiveTableProcessEvent hiveTableProcessEvent = JsonUtil.toObj(JsonUtil.toJson(attributes), HiveTableProcessEvent.class);
        // 生成node信息
        genAllNode(lineageMapping, hiveHookMessage, hiveTableProcessEvent);
        // node的冗余字段处理
        LineageUtil.genNormalSchemaNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalFieldNode(lineageMapping, hiveHookMessage);
    }

    private static void genAllNode(LineageMapping lineageMapping,
                                   HiveHookMessage hiveHookMessage,
                                   HiveTableProcessEvent hiveTableProcessEvent) {
        // platform cluster
        LineageUtil.genHivePlatformAndClusterNode(lineageMapping, hiveHookMessage);
        // db
        HiveTableProcessUtil.genSchemaNode(lineageMapping, hiveTableProcessEvent);
        // table process
        genTableAndProcessNode(lineageMapping, hiveHookMessage, hiveTableProcessEvent);
        // field
        HiveTableProcessUtil.genFieldNode(lineageMapping, hiveTableProcessEvent);
    }

    private static void genTableAndProcessNode(LineageMapping lineageMapping,
                                               HiveHookMessage hiveHookMessage,
                                               HiveTableProcessEvent hiveTableProcessEvent) {
        List<TableNode> tableList = new ArrayList<>();
        List<BaseAttribute> inputs = hiveTableProcessEvent.getInputs();
        List<BaseAttribute> outputs = hiveTableProcessEvent.getOutputs();
        // 单纯的两个node之间有关系
        doTwoTableNode(hiveHookMessage, tableList, inputs, outputs);
        LineageUtil.genNormalTableNode(tableList, hiveHookMessage);
        lineageMapping.getTableNodeList().addAll(tableList);
    }

    private static void doTwoTableNode(HiveHookMessage hiveHookMessage,
                                       List<TableNode> tableList,
                                       List<BaseAttribute> inputs,
                                       List<BaseAttribute> outputs) {
        // 这里 inputs outputs其实都只有一个
        BaseAttribute input = inputs.get(0);
        String inputTable = input.getName();
        String schema = input.getDb();
        TableNode inputTableNode = TableNode.builder()
                .schemaName(schema)
                .tableName(inputTable)
                .build();
        tableList.add(inputTableNode);
        String queryStr = hiveHookMessage.getQueryStr().toLowerCase();
        String inputTablePk = NodeQualifiedName.ofTable(hiveHookMessage.getPlatformName(),
                hiveHookMessage.getClusterName(),
                Objects.requireNonNull(schema),
                inputTable).toString();
        for (BaseAttribute outputsDTO : outputs) {
            if (queryStr.startsWith("insert overwrite")) {
                // insert overwrite
                tableList.add(TableNode.builder()
                        .schemaName(outputsDTO.getDb())
                        .tableName(outputsDTO.getName())
                        .insertOverwriteSql(queryStr)
                        .insertOverwriteFrom(inputTablePk)
                        .build());
            }
        }
    }
}
