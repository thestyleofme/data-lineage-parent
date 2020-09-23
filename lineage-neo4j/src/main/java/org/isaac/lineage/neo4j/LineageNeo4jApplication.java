package org.isaac.lineage.neo4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.neo4j.repository.config.EnableNeo4jRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 14:14
 * @since 1.0.0
 */
@SpringBootApplication
@EnableTransactionManagement
@EnableNeo4jRepositories(basePackages = {
        "org.isaac.lineage.neo4j.repository"
})
@EntityScan(basePackages = "org.isaac.lineage.neo4j.domain")
public class LineageNeo4jApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(LineageNeo4jApplication.class);

    public static void main(String[] args) {
        try {
            SpringApplication.run(LineageNeo4jApplication.class, args);
        } catch (Exception e) {
            LOGGER.error("application start error", e);
        }
    }
}
