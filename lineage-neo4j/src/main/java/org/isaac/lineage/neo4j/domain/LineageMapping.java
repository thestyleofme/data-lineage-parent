package org.isaac.lineage.neo4j.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.isaac.lineage.neo4j.domain.node.*;

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

    private List<PlatformNode> platformNodeList;
    private List<ClusterNode> clusterNodeList;
    private List<SchemaNode> schemaNodeList;
    private List<TableNode> tableNodeList;
    private List<FieldNode> fieldNodeList;
    private List<ProcessNode> processNodeList;
}
