package org.isaac.lineage.neo4j.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/23 10:43
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LineageMapping {

    private DatabaseNode databaseNode;
    private List<TableNode> tableNodeList;
    private List<FieldNode> fieldNodeList;
}
