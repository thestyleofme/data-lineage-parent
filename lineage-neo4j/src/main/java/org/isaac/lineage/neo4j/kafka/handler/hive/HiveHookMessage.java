package org.isaac.lineage.neo4j.kafka.handler.hive;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/23 11:44
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HiveHookMessage {

    private Long queryStartTime;
    private String createdBy;
    private Long createTime;
    private String ipAddress;
    private String typeName;
    private String operationName;
    private String executorAddress;
    private Map<String, Object> attributes;
    private Map<String, Object> queryInfo;
    private String queryId;
    private String queryStr;

    //===============================================================================
    //  normal
    //===============================================================================

    private String platformName;
    private String clusterName;

}
