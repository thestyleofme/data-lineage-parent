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
public class QueryHandlerTest {

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
                "    \"queryStartTime\": 1600864825903,\n" +
                "    \"sourceType\": \"HIVE-HOOK\",\n" +
                "    \"createdBy\": \"hive\",\n" +
                "    \"createTime\": 1600864836920,\n" +
                "    \"ipAddress\": \"10.211.144.85\",\n" +
                "    \"typeName\": \"hive_process\",\n" +
                "    \"operationName\": \"QUERY\",\n" +
                "    \"executorAddress\": \"172.23.16.70\",\n" +
                "    \"attributes\": {\n" +
                "        \"outputs\": [\n" +
                "            {\n" +
                "                \"owner\": \"hive\",\n" +
                "                \"temporary\": false,\n" +
                "                \"lastAccessTime\": 1600864813000,\n" +
                "                \"qualifiedName\": \"test.lineage_test002\",\n" +
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
                "                    \"qualifiedName\": \"test.lineage_test002_storage\",\n" +
                "                    \"storedAsSubDirectories\": false,\n" +
                "                    \"location\": \"hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/test.db/lineage_test002\",\n" +
                "                    \"compressed\": false,\n" +
                "                    \"inputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",\n" +
                "                    \"parameters\": {\n" +
                "                        \n" +
                "                    },\n" +
                "                    \"outputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",\n" +
                "                    \"table\": \"lineage_test002\",\n" +
                "                    \"serdeInfo\": {\n" +
                "                        \"serializationLib\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\",\n" +
                "                        \"parameters\": {\n" +
                "                            \"serialization.format\": \"1\"\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"numBuckets\": -1\n" +
                "                },\n" +
                "                \"tableType\": \"MANAGED_TABLE\",\n" +
                "                \"entity_type\": \"hive_table\",\n" +
                "                \"createTime\": 1600864813000,\n" +
                "                \"name\": \"lineage_test002\",\n" +
                "                \"parameters\": {\n" +
                "                    \"totalSize\": \"396\",\n" +
                "                    \"numRows\": \"2\",\n" +
                "                    \"rawDataSize\": \"4\",\n" +
                "                    \"COLUMN_STATS_ACCURATE\": \"{\\\"BASIC_STATS\\\":\\\"true\\\",\\\"COLUMN_STATS\\\":{\\\"c1\\\":\\\"true\\\",\\\"c2\\\":\\\"true\\\"}}\",\n" +
                "                    \"numFiles\": \"1\",\n" +
                "                    \"transient_lastDdlTime\": \"1600864836\",\n" +
                "                    \"bucketing_version\": \"2\"\n" +
                "                },\n" +
                "                \"db\": \"test\",\n" +
                "                \"retention\": 0\n" +
                "            }\n" +
                "        ],\n" +
                "        \"inputs\": [\n" +
                "            {\n" +
                "                \"owner\": \"hive\",\n" +
                "                \"temporary\": false,\n" +
                "                \"lastAccessTime\": 1600864769000,\n" +
                "                \"qualifiedName\": \"test.lineage_test001\",\n" +
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
                "                    \"qualifiedName\": \"test.lineage_test001_storage\",\n" +
                "                    \"storedAsSubDirectories\": false,\n" +
                "                    \"location\": \"hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/test.db/lineage_test001\",\n" +
                "                    \"compressed\": false,\n" +
                "                    \"inputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",\n" +
                "                    \"parameters\": {\n" +
                "                        \n" +
                "                    },\n" +
                "                    \"outputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",\n" +
                "                    \"table\": \"lineage_test001\",\n" +
                "                    \"serdeInfo\": {\n" +
                "                        \"serializationLib\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\",\n" +
                "                        \"parameters\": {\n" +
                "                            \"serialization.format\": \"1\"\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"numBuckets\": -1\n" +
                "                },\n" +
                "                \"tableType\": \"MANAGED_TABLE\",\n" +
                "                \"entity_type\": \"hive_table\",\n" +
                "                \"createTime\": 1600864769000,\n" +
                "                \"name\": \"lineage_test001\",\n" +
                "                \"parameters\": {\n" +
                "                    \"totalSize\": \"826\",\n" +
                "                    \"numRows\": \"2\",\n" +
                "                    \"rawDataSize\": \"4\",\n" +
                "                    \"COLUMN_STATS_ACCURATE\": \"{\\\"BASIC_STATS\\\":\\\"true\\\",\\\"COLUMN_STATS\\\":{\\\"c1\\\":\\\"true\\\",\\\"c2\\\":\\\"true\\\"}}\",\n" +
                "                    \"numFiles\": \"2\",\n" +
                "                    \"transient_lastDdlTime\": \"1600864801\",\n" +
                "                    \"bucketing_version\": \"2\"\n" +
                "                },\n" +
                "                \"db\": \"test\",\n" +
                "                \"retention\": 0\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"queryInfo\": {\n" +
                "        \n" +
                "    },\n" +
                "    \"queryId\": \"hive_20200923204025_bfe1df13-bb60-4961-a646-8d53a32cf4c1\",\n" +
                "    \"queryStr\": \"INSERT OVERWRITE TABLE lineage_test002\\r\\nSELECT *\\r\\nFROM lineage_test001\"\n" +
                "}";
        LineageMapping lineageMapping = hiveKafkaHandler.handle(record);
        Assert.assertNotNull(lineageMapping);
        lineageExecutor.handle(lineageMapping);
        hiveLineageHandler.handle(lineageMapping);
    }
}
