package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/24 9:59
 * @since 1.0.0
 */
@NoArgsConstructor
@Data
public class CreateTableAsEvent {

    private List<BaseAttribute> outputs;
    private List<BaseAttribute> inputs;

}
