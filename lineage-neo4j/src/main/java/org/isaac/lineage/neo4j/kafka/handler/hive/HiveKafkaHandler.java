package org.isaac.lineage.neo4j.kafka.handler.hive;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.annotation.SourceType;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.kafka.handler.BaseKafkaHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.event.CreateTableAsHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.event.CreateTableHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.event.DropTableHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.event.QueryHandler;
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

    @Override
    public LineageMapping handle(String record) {
        log.debug("handle HIVE-HOOK message......");
        HiveHookMessage hiveHookMessage = JsonUtil.toObj(record, HiveHookMessage.class);
        // 解析为 platform cluster schema table field process节点
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
        generateLineageNode(lineageMapping, hiveHookMessage);
    }

    private void generateLineageNode(LineageMapping lineageMapping, HiveHookMessage hiveHookMessage) {
        Map<String, Object> attributes = hiveHookMessage.getAttributes();
        // attributes为空，我们不需要记录血缘
        if (CollectionUtils.isEmpty(attributes)) {
            return;
        }
        switch (HiveOperationEnum.valueOf(hiveHookMessage.getOperationName().toUpperCase())) {
            case QUERY:
                QueryHandler.handle(lineageMapping, hiveHookMessage, attributes);
                break;
            case CREATETABLE:
                CreateTableHandler.handle(lineageMapping, hiveHookMessage, attributes);
                break;
            case CREATETABLE_AS_SELECT:
                CreateTableAsHandler.handle(lineageMapping, hiveHookMessage, attributes);
                break;
            case DROPTABLE:
                DropTableHandler.handler(lineageMapping,hiveHookMessage, attributes);
                break;
            default:
                break;
        }
    }

}
