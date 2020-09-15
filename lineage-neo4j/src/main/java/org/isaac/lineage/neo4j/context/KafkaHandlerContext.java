package org.isaac.lineage.neo4j.context;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.exceptions.LineageException;
import org.isaac.lineage.neo4j.handler.BaseKafkaHandler;
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

    private static final Map<String, BaseKafkaHandler> HANDLER_MAP = new HashMap<>();

    public void register(String sourceType, BaseKafkaHandler baseKafkaHandler) {
        if (HANDLER_MAP.containsKey(sourceType)) {
            log.error("sourceType {} exists", sourceType);
            throw new LineageException("error.neo4j.source_type.exist");
        }
        HANDLER_MAP.put(sourceType, baseKafkaHandler);
    }

    public BaseKafkaHandler getKafkaHandler(String sourceType) {
        BaseKafkaHandler baseKafkaHandler = HANDLER_MAP.get(sourceType.toUpperCase());
        if (baseKafkaHandler == null) {
            throw new LineageException("error.neo4j.source_type.not_exist");
        }
        return baseKafkaHandler;
    }
}
