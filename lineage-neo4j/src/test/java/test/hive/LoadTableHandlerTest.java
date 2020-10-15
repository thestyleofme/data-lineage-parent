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
 * @author isaac 2020/09/22 20:01
 * @since 1.0.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = LineageNeo4jApplication.class)
public class LoadTableHandlerTest {

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
                "    \"typeName\": \"hive_process\",\n" +
                "    \"operationName\": \"LOAD\",\n" +
                "    \"queryId\": \"hive_20201015123224_4e3b1bac-2a3d-4a10-99cb-c8096633acdb\",\n" +
                "    \"queryStartTime\": 1602736344035,\n" +
                "    \"sourceType\": \"HIVE-HOOK\",\n" +
                "    \"createdBy\": \"hive\",\n" +
                "    \"createTime\": 1602736344945,\n" +
                "    \"clusterName\": \"DEFAULT\",\n" +
                "    \"executorAddress\": \"172.23.16.70\",\n" +
                "    \"attributes\": {\n" +
                "        \"outputs\": [\n" +
                "            {\n" +
                "                \"owner\": \"hive\",\n" +
                "                \"temporary\": false,\n" +
                "                \"lastAccessTime\": 1602736207000,\n" +
                "                \"qualifiedName\": \"test.lineage_test005\",\n" +
                "                \"columns\": [\n" +
                "                    {\n" +
                "                        \"name\": \"c1\",\n" +
                "                        \"type\": \"int\"\n" +
                "                    },\n" +
                "                    {\n" +
                "                        \"name\": \"c2\",\n" +
                "                        \"type\": \"string\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"storageDesc\": {\n" +
                "                    \"bucketCols\": [\n" +
                "                        \n" +
                "                    ],\n" +
                "                    \"entity_type\": \"hive_storage_desc\",\n" +
                "                    \"qualifiedName\": \"test.lineage_test005_storage\",\n" +
                "                    \"storedAsSubDirectories\": false,\n" +
                "                    \"location\": \"hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/test.db/lineage_test005\",\n" +
                "                    \"compressed\": false,\n" +
                "                    \"inputFormat\": \"org.apache.hadoop.mapred.TextInputFormat\",\n" +
                "                    \"parameters\": {\n" +
                "                        \n" +
                "                    },\n" +
                "                    \"outputFormat\": \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\n" +
                "                    \"table\": \"lineage_test005\",\n" +
                "                    \"serdeInfo\": {\n" +
                "                        \"serializationLib\": \"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\",\n" +
                "                        \"parameters\": {\n" +
                "                            \"serialization.format\": \",\",\n" +
                "                            \"field.delim\": \",\"\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"numBuckets\": -1\n" +
                "                },\n" +
                "                \"tableType\": \"MANAGED_TABLE\",\n" +
                "                \"entity_type\": \"hive_table\",\n" +
                "                \"createTime\": 1602736207000,\n" +
                "                \"name\": \"lineage_test005\",\n" +
                "                \"parameters\": {\n" +
                "                    \"totalSize\": \"26\",\n" +
                "                    \"numRows\": \"0\",\n" +
                "                    \"rawDataSize\": \"0\",\n" +
                "                    \"numFiles\": \"1\",\n" +
                "                    \"transient_lastDdlTime\": \"1602736344\",\n" +
                "                    \"bucketing_version\": \"2\"\n" +
                "                },\n" +
                "                \"db\": \"test\",\n" +
                "                \"retention\": 0\n" +
                "            }\n" +
                "        ],\n" +
                "        \"inputs\": [\n" +
                "            {\n" +
                "                \"path\": \"hdfs://hdspdemo001.hand-china.com:8020/hive_load_lineage_test005.csv\",\n" +
                "                \"entity_type\": \"hdfs_path\",\n" +
                "                \"name\": \"/hive_load_lineage_test005.csv\"\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"platformName\": \"DEFAULT\",\n" +
                "    \"queryInfo\": {\n" +
                "        \n" +
                "    },\n" +
                "    \"queryStr\": \"load data inpath '/hive_load_lineage_test005.csv' into table lineage_test005\"\n" +
                "}";
        LineageMapping lineageMapping = hiveKafkaHandler.handle(record);
        Assert.assertNotNull(lineageMapping);
        lineageExecutor.handle(lineageMapping);
        hiveLineageHandler.handle(lineageMapping);
    }

}
