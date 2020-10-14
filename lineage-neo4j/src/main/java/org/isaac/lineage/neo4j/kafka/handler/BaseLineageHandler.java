package org.isaac.lineage.neo4j.kafka.handler;


import org.isaac.lineage.neo4j.domain.LineageMapping;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/14 9:36
 * @since 1.0.0
 */
public interface BaseLineageHandler {

    /**
     * 提供口子，根据LineageMapping进行特殊处理，方便扩展
     *
     * @param lineageMapping LineageMapping
     */
    default void handle(LineageMapping lineageMapping) {

    }
}
