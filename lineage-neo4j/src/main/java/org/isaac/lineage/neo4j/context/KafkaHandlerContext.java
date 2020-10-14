package org.isaac.lineage.neo4j.context;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.exceptions.LineageException;
import org.isaac.lineage.neo4j.kafka.handler.BaseKafkaHandler;
import org.isaac.lineage.neo4j.kafka.handler.BaseLineageHandler;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 15:00
 * @since 1.0.0
 */
@Component
@Slf4j
public class KafkaHandlerContext {

    private static final Map<String, BaseKafkaHandler> KAFKA_HANDLER_MAP = new HashMap<>();
    private static final Map<String, BaseLineageHandler> LINEAGE_HANDLER_MAP = new HashMap<>();

    public void register(String sourceType, BaseKafkaHandler baseKafkaHandler) {
        if (KAFKA_HANDLER_MAP.containsKey(sourceType)) {
            log.error("sourceType {} exists", sourceType);
            throw new LineageException("error.neo4j.source_type.exist");
        }
        KAFKA_HANDLER_MAP.put(sourceType, baseKafkaHandler);
    }

    public void register(String sourceType, BaseLineageHandler baseLineageHandler) {
        if (LINEAGE_HANDLER_MAP.containsKey(sourceType)) {
            log.error("sourceType {} exists", sourceType);
            throw new LineageException("error.neo4j.source_type.exist");
        }
        LINEAGE_HANDLER_MAP.put(sourceType, baseLineageHandler);
    }

    public BaseKafkaHandler getKafkaHandler(String sourceType) {
        BaseKafkaHandler baseKafkaHandler = KAFKA_HANDLER_MAP.get(sourceType.toUpperCase());
        if (baseKafkaHandler == null) {
            throw new LineageException("error.neo4j.source_type.not_exist");
        }
        return baseKafkaHandler;
    }

    public BaseLineageHandler getLineageHandler (String sourceType) {
        BaseLineageHandler baseLineageHandler = LINEAGE_HANDLER_MAP.get(sourceType.toUpperCase());
        if (baseLineageHandler == null) {
            throw new LineageException("error.neo4j.source_type.not_exist");
        }
        return baseLineageHandler;
    }
}
