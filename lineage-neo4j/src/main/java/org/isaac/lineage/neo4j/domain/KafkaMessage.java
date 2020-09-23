package org.isaac.lineage.neo4j.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * <p>
 * kafka message（json）中必须有的属性值
 * </p>
 *
 * @author isaac 2020/9/15 14:48
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaMessage {

    /**
     * 消息的来源类型，如HIVE-HOOK
     */
    private String sourceType;
}
