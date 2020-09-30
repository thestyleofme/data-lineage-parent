package org.isaac.lineage.neo4j.domain.result;

import lombok.Data;
import org.isaac.lineage.neo4j.domain.node.SchemaNode;
import org.isaac.lineage.neo4j.domain.node.TableNode;
import org.springframework.data.neo4j.annotation.QueryResult;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/22 20:50
 * @since 1.0.0
 */
@QueryResult
@Data
public class SchemaQueryResult {

    private SchemaNode schema;
    private TableNode table;
}
