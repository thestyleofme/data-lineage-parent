package org.isaac.lineage.neo4j.domain.node;

import lombok.*;
import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.neo4j.ogm.annotation.NodeEntity;

/**
 * <p>
 * Schema Node, if database contain catalog,use database = catalog.schema
 * </p>
 *
 * @author JupiterMouse 2020/09/28
 * @since 1.0
 */
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@NodeEntity(NeoConstant.Type.NODE_SCHEMA)
public class SchemaNode extends BaseNodeEntity {

    private String schemaName;

    //===============================================================================
    //  Other information fields
    //===============================================================================

    /**
     * 创建database的sql
     */
    private String sql;
}
