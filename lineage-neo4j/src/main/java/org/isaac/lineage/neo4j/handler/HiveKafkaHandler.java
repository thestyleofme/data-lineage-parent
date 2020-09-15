package org.isaac.lineage.neo4j.handler;

import lombok.extern.slf4j.Slf4j;
import org.isaac.lineage.neo4j.annotation.SourceType;
import org.springframework.stereotype.Component;


/**
 * <p>
 * 处理hive hook产生的血缘信息
 * </p>
 *
 * @author isaac 2020/9/15 15:03
 * @since 1.0.0
 */
@SourceType("HIVE-HOOK")
@Slf4j
@Component
public class HiveKafkaHandler implements BaseKafkaHandler {

    @Override
    public void handle(String record) {
        log.debug("handle HIVE-HOOK message......");
    }
}
