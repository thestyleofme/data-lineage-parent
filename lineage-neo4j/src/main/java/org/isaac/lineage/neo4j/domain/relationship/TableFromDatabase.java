package org.isaac.lineage.neo4j.domain.relationship;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
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
@RelationshipEntity(type = "TABLE_FROM_DATABASE")
public class TableFromDatabase {

    @Id
    @GeneratedValue
    private Long id;

    @StartNode
    private TableNode tableNode;

    @EndNode
    private DatabaseNode databaseNode;
}
