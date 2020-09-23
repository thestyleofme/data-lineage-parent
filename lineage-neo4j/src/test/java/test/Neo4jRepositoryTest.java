package test;

import java.util.List;

import org.isaac.lineage.neo4j.LineageNeo4jApplication;
import org.isaac.lineage.neo4j.domain.node.DatabaseNode;
import org.isaac.lineage.neo4j.domain.result.DbQueryResult;
import org.isaac.lineage.neo4j.repository.node.DatabaseRepository;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/22 20:01
 * @since 1.0.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = LineageNeo4jApplication.class)
public class Neo4jRepositoryTest {

    @Autowired
    private DatabaseRepository databaseRepository;

    @Test
    public void testQuery() {
        List<DatabaseNode> all = (List<DatabaseNode>) databaseRepository.findAll();
        List<DbQueryResult> queryResults = databaseRepository.queryTables("test");
        Assert.assertNotNull(all);
    }
}
