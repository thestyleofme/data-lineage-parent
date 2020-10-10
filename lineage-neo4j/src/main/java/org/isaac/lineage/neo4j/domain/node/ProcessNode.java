package org.isaac.lineage.neo4j.domain.node;

import java.util.List;

import lombok.*;
import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Transient;

/**
 * <p>
 * 主要是处理多个节点之间的关系
 * Process节点主键及pk字段生成规则如下：
 * 示例：
 * sourceNodePkList：x,y
 * targetNodePk: z
 * md5(targetNodePk + sourceNodePkList排序后使用下划线'_'连接)
 * </p>
 *
 * @author isaac 2020/10/09 9:53
 * @since 1.0.0
 */
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@NodeEntity(NeoConstant.Type.NODE_PROCESS)
public class ProcessNode extends BaseNodeEntity {

    /**
     * 处理类型
     */
    private String type;
    /**
     * 处理事件详情，如具体sql语句，create table xxx
     */
    private String event;
    /**
     * 存储Node.pk
     * 示例：
     * create table A as select B.col1, C.col2 from B,C where xxx
     * sourceNode: B, C
     * targetNode: A
     */
    @Transient
    private List<String> sourceNodePkList;
    @Transient
    private String targetNodePk;

}
