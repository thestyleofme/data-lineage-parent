package org.isaac.lineage.neo4j.kafka;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.context.KafkaHandlerContext;
import org.isaac.lineage.neo4j.handler.BaseKafkaHandler;
import org.isaac.lineage.neo4j.pojo.KafkaMessage;
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

    public KafkaMessageHandler(KafkaHandlerContext kafkaHandlerContext) {
        this.kafkaHandlerContext = kafkaHandlerContext;
    }

    @KafkaListener(topics = "TOPIC-METADATA-LINEAGE", groupId = "lineage_handler_group")
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
        kafkaHandler.handle(record);
    }
}
