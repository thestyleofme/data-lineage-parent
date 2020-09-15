package org.isaac.lineage.neo4j.handler;

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
     */
    void handle(String record);

}
