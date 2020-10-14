package org.isaac.lineage.neo4j.kafka;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.context.KafkaHandlerContext;
import org.isaac.lineage.neo4j.domain.KafkaMessage;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.kafka.handler.BaseKafkaHandler;
import org.isaac.lineage.neo4j.kafka.handler.BaseLineageHandler;
import org.isaac.lineage.neo4j.utils.JsonUtil;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 14:26
 * @since 1.0.0
 */
@Slf4j
@Component
public class KafkaMessageHandler {

    private final KafkaHandlerContext kafkaHandlerContext;
    private final LineageExecutor lineageExecutor;

    public KafkaMessageHandler(KafkaHandlerContext kafkaHandlerContext,
                               LineageExecutor lineageExecutor) {
        this.kafkaHandlerContext = kafkaHandlerContext;
        this.lineageExecutor = lineageExecutor;
    }

    @KafkaListener(topics = "TOPIC_METADATA_LINEAGE", groupId = "lineage_handler_group")
    public void handle(String record) {
        log.info("handle kafka message......");
        if (StringUtils.isEmpty(record)) {
            return;
        }
        KafkaMessage kafkaMessage = JsonUtil.toObj(record, KafkaMessage.class);
        String sourceType = kafkaMessage.getSourceType();
        if (StringUtils.isEmpty(sourceType)) {
            return;
        }
        BaseKafkaHandler kafkaHandler = kafkaHandlerContext.getKafkaHandler(sourceType);
        LineageMapping lineageMapping = kafkaHandler.handle(record);
        if (lineageMapping == null) {
            return;
        }
        // neo4j进行血缘储存
        log.debug("neo4j started processing...");
        lineageExecutor.handle(lineageMapping);
        // 扩展点
        BaseLineageHandler lineageHandler = kafkaHandlerContext.getLineageHandler(sourceType);
        lineageHandler.handle(lineageMapping);
    }
}
