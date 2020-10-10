package org.isaac.lineage.neo4j.domain.node;

import java.time.LocalDateTime;

import lombok.Getter;
import lombok.Setter;
import org.isaac.lineage.neo4j.contants.NeoConstant;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.Index;
import org.neo4j.ogm.annotation.Version;

/**
 * <p>
 * Node attribute abstraction
 * </p>
 *
 * @author JupiterMouse 2020/09/27
 * @since 1.0
 */
@Setter
@Getter
public abstract class BaseNodeEntity extends BaseEntity {

    @Id
    @Index(unique = true)
    private String pk;

    private String status = NeoConstant.Status.ACTIVE;
    private String createdBy;
    private String updatedBy;
    private LocalDateTime createTime;
    private LocalDateTime updateTime = LocalDateTime.now();

    /**
     * Optimistic lock field
     */
    @Version
    private Long version;
    /**
     * Display field
     */
    private String name;

    //===============================================================================
    //  redundant field
    //===============================================================================

    private Long tenantId;
    private String datasourceCode;
    private String clusterName = "DEFAULT";
    private String platformName = "DEFAULT";
}
