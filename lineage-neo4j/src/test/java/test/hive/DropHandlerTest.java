package test.hive;

import org.isaac.lineage.neo4j.LineageNeo4jApplication;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.kafka.LineageExecutor;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveKafkaHandler;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveLineageHandler;
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
 * @author isaac 2020/10/14 14:15
 * @since 1.0.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = LineageNeo4jApplication.class)
public class DropHandlerTest {

    @Autowired
    private HiveKafkaHandler hiveKafkaHandler;
    @Autowired
    private LineageExecutor lineageExecutor;
    @Autowired
    private HiveLineageHandler hiveLineageHandler;

    @Test
    public void test() {
        // language=JSON
        String record = "{\n" +
                "    \"ipAddress\": \"10.211.144.85\",\n" +
                "    \"typeName\": \"hive_table\",\n" +
                "    \"operationName\": \"DROPTABLE\",\n" +
                "    \"queryId\": \"hive_20201014100601_8ef75057-34da-40e6-9d88-8162b1918032\",\n" +
                "    \"queryStartTime\": 1602641161864,\n" +
                "    \"sourceType\": \"HIVE-HOOK\",\n" +
                "    \"createdBy\": \"hive\",\n" +
                "    \"createTime\": 1602641162846,\n" +
                "    \"clusterName\": \"DEFAULT\",\n" +
                "    \"executorAddress\": \"172.23.16.70\",\n" +
                "    \"attributes\": {\n" +
                "        \"dropTable\": [\n" +
                "            \"test.lineage_test002\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"platformName\": \"DEFAULT\",\n" +
                "    \"queryInfo\": {\n" +
                "        \n" +
                "    },\n" +
                "    \"queryStr\": \"drop table lineage_test002\"\n" +
                "}";
        LineageMapping lineageMapping = hiveKafkaHandler.handle(record);
        Assert.assertNotNull(lineageMapping);
        lineageExecutor.handle(lineageMapping);
        hiveLineageHandler.handle(lineageMapping);
    }
}
