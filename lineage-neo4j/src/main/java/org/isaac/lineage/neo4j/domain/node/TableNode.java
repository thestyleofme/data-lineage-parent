package org.isaac.lineage.neo4j.domain.node;

import lombok.*;
import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.neo4j.ogm.annotation.NodeEntity;

/**
 * <p>
 * Table Node
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
@NodeEntity(NeoConstant.Type.NODE_TABLE)
public class TableNode extends BaseNodeEntity {

    private String schemaName;
    private String tableName;

    //===============================================================================
    //  Other information fields
    //===============================================================================

    /**
     * 创表语句
     */
    private String sql;
    /**
     * create table table1 as select * from table2
     * createTableFrom: table2
     */
    private String createTableFrom;
    /**
     * insert overwrite table1 select * from table2
     * insertOverwriteFrom: table2
     */
    private String insertOverwriteFrom;
    private String insertOverwriteSql;
}

