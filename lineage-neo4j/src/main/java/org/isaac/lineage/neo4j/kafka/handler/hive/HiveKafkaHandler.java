package org.isaac.lineage.neo4j.kafka.handler.hive;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.annotation.SourceType;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.kafka.handler.BaseKafkaHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.event.CreateTableAsHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.event.CreateTableHandler;
import org.isaac.lineage.neo4j.utils.JsonUtil;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;


/**
 * <p>
 * 处理hive hook产生的血缘信息
 * </p>
 *
 * @author isaac 2020/9/15 15:03
 * @since 1.0.0
 */
@SourceType("HIVE-HOOK")
@Slf4j
@Component
public class HiveKafkaHandler implements BaseKafkaHandler {

    private static final String OPERATION_NAME_QUERY = "QUERY";

    @Override
    public LineageMapping handle(String record) {
        log.debug("handle HIVE-HOOK message......");
        HiveHookMessage hiveHookMessage = JsonUtil.toObj(record, HiveHookMessage.class);
        // 查询不做血缘分析
        if (OPERATION_NAME_QUERY.equals(hiveHookMessage.getOperationName())) {
            return null;
        }
        // 解析为 db table field节点
        LineageMapping lineageMapping = new LineageMapping();
        handleNode(lineageMapping, hiveHookMessage);
        return lineageMapping;
    }

    /**
     * 将kafka消息解析为neo4j node
     *
     * @param lineageMapping  LineageMapping
     * @param hiveHookMessage HiveHookMessage
     */
    private void handleNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        // 生成database table field节点
        generateLineageNode(lineageMapping, hiveHookMessage);
    }

    private void generateLineageNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        Map<String, Object> attributes = hiveHookMessage.getAttributes();
        if (CollectionUtils.isEmpty(attributes)) {
            return;
        }
        switch (HiveEventType.valueOf(hiveHookMessage.getTypeName().toUpperCase())){
            case HIVE_TABLE:
                CreateTableHandler.handle(lineageMapping, hiveHookMessage, attributes);
                break;
            case HIVE_PROCESS:
                CreateTableAsHandler.handle(lineageMapping, hiveHookMessage, attributes);
                break;
            default:
                break;
        }
    }
}
