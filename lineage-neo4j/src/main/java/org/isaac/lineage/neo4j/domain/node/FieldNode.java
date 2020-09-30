package org.isaac.lineage.neo4j.domain.node;

import lombok.*;
import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.neo4j.ogm.annotation.NodeEntity;

/**
 * <p>
 * Node Field
 * </p>
 *
 * @author isaac 2020/09/28
 * @since 1.0
 */
@EqualsAndHashCode(callSuper = true)
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@NodeEntity(NeoConstant.Type.NODE_FIELD)
public class FieldNode extends BaseNodeEntity {

    private String schemaName;
    private String tableName;
    private String fieldName;
    private String fieldType;

    //===============================================================================
    //  Other information fields
    //===============================================================================


}
