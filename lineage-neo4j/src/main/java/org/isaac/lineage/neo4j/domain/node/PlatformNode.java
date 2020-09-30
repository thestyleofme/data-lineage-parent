package org.isaac.lineage.neo4j.domain.node;

import lombok.*;
import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.neo4j.ogm.annotation.NodeEntity;

/**
 * <p>
 * Cluster Node
 * </p>
 *
 * @author JupiterMouse 2020/09/28
 * @since 1.0
 */
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@NodeEntity(NeoConstant.Type.NODE_PLATFORM)
public class PlatformNode extends BaseNodeEntity {

    @Builder.Default
    private String platformName = "DEFAULT";
}
