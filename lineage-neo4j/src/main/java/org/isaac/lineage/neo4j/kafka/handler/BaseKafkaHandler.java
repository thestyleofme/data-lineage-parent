package org.isaac.lineage.neo4j.kafka.handler;

import org.isaac.lineage.neo4j.domain.LineageMapping;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 14:48
 * @since 1.0.0
 */
public interface BaseKafkaHandler {

    /**
     * 处理kafka消息
     *
     * @param record Kafka Message
     * @return LineageMapping
     */
    LineageMapping handle(String record);

}
