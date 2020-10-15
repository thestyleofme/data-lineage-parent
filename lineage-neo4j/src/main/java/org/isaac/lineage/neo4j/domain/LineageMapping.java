package org.isaac.lineage.neo4j.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private List<PlatformNode> platformNodeList = new ArrayList<>();
    private List<ClusterNode> clusterNodeList = new ArrayList<>();
    private List<SchemaNode> schemaNodeList = new ArrayList<>();
    private List<TableNode> tableNodeList = new ArrayList<>();
    private List<FieldNode> fieldNodeList = new ArrayList<>();
    private List<ProcessNode> processNodeList = new ArrayList<>();

    private Map<String, Object> extMap = new HashMap<>();
}
