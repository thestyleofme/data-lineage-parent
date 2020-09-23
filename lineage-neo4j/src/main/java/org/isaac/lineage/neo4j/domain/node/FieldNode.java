package org.isaac.lineage.neo4j.domain.node;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 19:43
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Builder
@NodeEntity(label = "Field")
public class FieldNode {

    @Id
    @GeneratedValue
    private Long id;

    /**
     * 字段名称
     */
    private String fieldName;
    /**
     * 字段类型
     */
    private String fieldType;
    /**
     * 字段所在表的所在数据库名称
     */
    private String databaseName;
    /**
     * 字段所在的表名
     */
    private String tableName;

    //===============================================================================
    //  基础
    //===============================================================================

    public static final String PK_FORMAT = "%s/%s/%s/%s/%s";

    /**
     * 唯一值约束,${platform}/${cluster}/xx
     */
    private String pk;
    /**
     * 节点所属集群，一般是datasourceCode_tenantId，未指定的话 默认是DEFAULT
     */
    @Builder.Default
    private String platform = "DEFAULT";
    /**
     * 所属集群
     */
    @Builder.Default
    private String cluster = "DEFAULT";
    /**
     * 该节点是否被删除
     */
    private boolean deleted;
    /**
     * 扩展信息
     */
    private Map<String, Object> extConfig;

}