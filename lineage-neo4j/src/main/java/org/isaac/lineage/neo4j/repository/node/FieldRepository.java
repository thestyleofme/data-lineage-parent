package org.isaac.lineage.neo4j.repository.node;

import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/16 10:54
 * @since 1.0.0
 */
@Repository
public interface FieldRepository extends Neo4jRepository<FieldNode, String> {

}
