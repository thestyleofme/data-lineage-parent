package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.NodeQualifiedName;
import org.isaac.lineage.neo4j.domain.node.ProcessNode;
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

    private static final int MULTI_NODE_NUMBER = 2;

    private CreateTableAsHandler() {
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
        // 判断是否多个node有关系
        if ((inputs.size() + outputs.size()) > MULTI_NODE_NUMBER) {
            List<ProcessNode> processNodeList = new ArrayList<>();
            doMultiNode(hiveHookMessage, tableList, processNodeList, inputs, outputs);
            lineageMapping.setProcessNodeList(processNodeList);
        } else {
            // 单纯的两个node之间有关系
            doTwoTableNode(hiveHookMessage, tableList, inputs, outputs);
            LineageUtil.genNormalTableNode(tableList, hiveHookMessage);
        }
        lineageMapping.getTableNodeList().addAll(tableList);
    }

    private static void doMultiNode(HiveHookMessage hiveHookMessage,
                                    List<TableNode> tableList,
                                    List<ProcessNode> processNodeList,
                                    List<BaseAttribute> inputs,
                                    List<BaseAttribute> outputs) {
        // ProcessNode 一个
        String queryStr = hiveHookMessage.getQueryStr();
        ProcessNode processNode = ProcessNode.builder()
                .type(hiveHookMessage.getOperationName())
                .event(queryStr).build();
        LineageUtil.doNormalProcessNode(hiveHookMessage, processNode);
        // 输入table 多个
        TableNode tableNode;
        List<String> sourceNodePkList = new ArrayList<>(inputs.size());
        for (BaseAttribute input : inputs) {
            tableNode = TableNode.builder()
                    .schemaName(input.getDb())
                    .tableName(input.getName())
                    .build();
            LineageUtil.doNormalTableNode(hiveHookMessage, tableNode);
            sourceNodePkList.add(tableNode.getPk());
            tableList.add(tableNode);
        }
        processNode.setSourceNodePkList(sourceNodePkList);
        // 输出table 一个
        for (BaseAttribute output : outputs) {
            tableNode = TableNode.builder()
                    .schemaName(output.getDb())
                    .tableName(output.getName())
                    .build();
            LineageUtil.doNormalTableNode(hiveHookMessage, tableNode);
            processNode.setTargetNodePk(tableNode.getPk());
            tableList.add(tableNode);
        }
        // process node pk
        String processNodePk = LineageUtil.genProcessNodePk(processNode);
        processNode.setPk(processNodePk);
        processNodeList.add(processNode);
    }

    private static void doTwoTableNode(HiveHookMessage hiveHookMessage,
                                       List<TableNode> tableList,
                                       List<BaseAttribute> inputs,
                                       List<BaseAttribute> outputs) {
        // 这里 inputs outputs其实都只有一个
        BaseAttribute input = inputs.get(0);
        String inputTable = input.getName();
        String schema = input.getDb();
        tableList.add(TableNode.builder()
                .schemaName(schema)
                .tableName(inputTable)
                .build());
        String queryStr = hiveHookMessage.getQueryStr().toLowerCase();
        String inputTablePk = NodeQualifiedName.ofTable(hiveHookMessage.getPlatformName(),
                hiveHookMessage.getClusterName(),
                Objects.requireNonNull(schema),
                inputTable).toString();
        BaseAttribute output = outputs.get(0);
        tableList.add(TableNode.builder()
                .schemaName(output.getDb())
                .tableName(output.getName())
                .sql(queryStr)
                .createTableFrom(inputTablePk)
                .build());
    }
}
