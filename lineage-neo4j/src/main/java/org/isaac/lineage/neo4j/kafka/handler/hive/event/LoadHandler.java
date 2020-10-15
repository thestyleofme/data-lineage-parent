package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.ProcessNode;
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
 * @author isaac 2020/10/15 13:45
 * @since 1.0.0
 */
public class LoadHandler {

    private LoadHandler() {
        throw new IllegalStateException();
    }

    public static void handler(LineageMapping lineageMapping,
                               HiveHookMessage hiveHookMessage,
                               Map<String, Object> attributes) {
        HiveTableProcessEvent hiveTableProcessEvent = JsonUtil.toObj(JsonUtil.toJson(attributes), HiveTableProcessEvent.class);
        // 生成node信息
        genAllNode(lineageMapping, hiveHookMessage, hiveTableProcessEvent);
    }

    private static void genAllNode(LineageMapping lineageMapping,
                                   HiveHookMessage hiveHookMessage,
                                   HiveTableProcessEvent hiveTableProcessEvent) {
        // platform cluster
        LineageUtil.genHivePlatformAndClusterNode(lineageMapping, hiveHookMessage);
        // db table field process
        handler(lineageMapping, hiveHookMessage, hiveTableProcessEvent);
        // node的冗余字段处理
        LineageUtil.genNormalSchemaNode(lineageMapping, hiveHookMessage);
        LineageUtil.genNormalFieldNode(lineageMapping, hiveHookMessage);
    }

    private static void handler(LineageMapping lineageMapping,
                                HiveHookMessage hiveHookMessage,
                                HiveTableProcessEvent hiveTableProcessEvent) {
        // input以及output都是1个
        List<BaseAttribute> outputs = hiveTableProcessEvent.getOutputs();
        String tablePk = null;
        for (BaseAttribute output : outputs) {
            // schema
            lineageMapping.getSchemaNodeList().add(SchemaNode.builder()
                    .schemaName(output.getDb()).build());
            // table
            TableNode tableNode = TableNode.builder()
                    .schemaName(output.getDb())
                    .tableName(output.getName())
                    .build();
            LineageUtil.doNormalTableNode(hiveHookMessage, tableNode);
            tablePk = tableNode.getPk();
            lineageMapping.getTableNodeList().add(tableNode);
        }
        // field
        HiveTableProcessUtil.genFieldNode(lineageMapping, outputs);
        // process
        String queryStr = hiveHookMessage.getQueryStr().toLowerCase();
        for (BaseAttribute input : hiveTableProcessEvent.getInputs()) {
            ProcessNode processNode = ProcessNode.builder()
                    .type("HIVE_LOAD")
                    .event(queryStr)
                    .targetNodePk(Objects.requireNonNull(tablePk))
                    .build();
            processNode.getExtra().put("path", input.getPath());
            processNode.setPk(LineageUtil.genProcessNodePk(processNode));
            lineageMapping.getProcessNodeList().add(processNode);
        }
    }
}
