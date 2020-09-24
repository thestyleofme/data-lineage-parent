package org.isaac.lineage.neo4j.domain.relationship;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.neo4j.ogm.annotation.*;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 20:02
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Builder
@RelationshipEntity(type = "INSERT_OVERWRITE_TABLE_SELECT")
public class InsertOverwriteTable {

    @Id
    @GeneratedValue
    private Long id;

    /**
     * INSERT OVERWRITE TABLE table1 SELECT * FROM table2
     * table1: insertTableNode
     * table2: selectTableNode
     */
    @StartNode
    private TableNode insertTableNode;

    /**
     * 查询数据的table
     */
    @EndNode
    private TableNode selectTableNode;
}
