package test;

import org.isaac.lineage.neo4j.LineageNeo4jApplication;
import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.kafka.LineageExecutor;
import org.isaac.lineage.neo4j.kafka.handler.hive.HiveKafkaHandler;
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
public class HiveKafkaHandlerTest {

    @Autowired
    private HiveKafkaHandler hiveKafkaHandler;
    @Autowired
    private LineageExecutor lineageExecutor;

    @Test
    public void test() {
        String record = "{\n" +
                "\t\"queryStartTime\": 1599631938701,\n" +
                "\t\"sourceType\": \"HIVE-HOOK\",\n" +
                "\t\"createdBy\": \"hive\",\n" +
                "\t\"createTime\": 1599631940347,\n" +
                "\t\"ipAddress\": \"10.211.144.85\",\n" +
                "\t\"typeName\": \"hive_table\",\n" +
                "\t\"operationName\": \"CREATETABLE\",\n" +
                "\t\"executorAddress\": \"172.23.16.70\",\n" +
                "\t\"attributes\": {\n" +
                "\t\t\"hive_table\": {\n" +
                "\t\t\t\"owner\": \"hive\",\n" +
                "\t\t\t\"temporary\": false,\n" +
                "\t\t\t\"lastAccessTime\": 1599631939000,\n" +
                "\t\t\t\"qualifiedName\": \"default.lineage_test001\",\n" +
                "\t\t\t\"columns\": [{\n" +
                "\t\t\t\t\"name\": \"c1\",\n" +
                "\t\t\t\t\"type\": \"int\"\n" +
                "\t\t\t}, {\n" +
                "\t\t\t\t\"name\": \"c2\",\n" +
                "\t\t\t\t\"type\": \"string\"\n" +
                "\t\t\t}],\n" +
                "\t\t\t\"storageDesc\": {\n" +
                "\t\t\t\t\"bucketCols\": [],\n" +
                "\t\t\t\t\"entity_type\": \"hive_storage_desc\",\n" +
                "\t\t\t\t\"qualifiedName\": \"default.lineage_test001_storage\",\n" +
                "\t\t\t\t\"storedAsSubDirectories\": false,\n" +
                "\t\t\t\t\"location\": \"hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/lineage_test001\",\n" +
                "\t\t\t\t\"compressed\": false,\n" +
                "\t\t\t\t\"inputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",\n" +
                "\t\t\t\t\"parameters\": {},\n" +
                "\t\t\t\t\"outputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",\n" +
                "\t\t\t\t\"table\": \"lineage_test001\",\n" +
                "\t\t\t\t\"serdeInfo\": {\n" +
                "\t\t\t\t\t\"serializationLib\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\",\n" +
                "\t\t\t\t\t\"parameters\": {\n" +
                "\t\t\t\t\t\t\"serialization.format\": \"1\"\n" +
                "\t\t\t\t\t}\n" +
                "\t\t\t\t},\n" +
                "\t\t\t\t\"numBuckets\": -1\n" +
                "\t\t\t},\n" +
                "\t\t\t\"tableType\": \"MANAGED_TABLE\",\n" +
                "\t\t\t\"entity_type\": \"hive_table\",\n" +
                "\t\t\t\"createTime\": 1599631939000,\n" +
                "\t\t\t\"name\": \"lineage_test001\",\n" +
                "\t\t\t\"parameters\": {\n" +
                "\t\t\t\t\"totalSize\": \"0\",\n" +
                "\t\t\t\t\"numRows\": \"0\",\n" +
                "\t\t\t\t\"rawDataSize\": \"0\",\n" +
                "\t\t\t\t\"COLUMN_STATS_ACCURATE\": \"{\\\"BASIC_STATS\\\":\\\"true\\\",\\\"COLUMN_STATS\\\":{\\\"c1\\\":\\\"true\\\",\\\"c2\\\":\\\"true\\\"}}\",\n" +
                "\t\t\t\t\"numFiles\": \"0\",\n" +
                "\t\t\t\t\"transient_lastDdlTime\": \"1599631939\",\n" +
                "\t\t\t\t\"bucketing_version\": \"2\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"db\": \"default\",\n" +
                "\t\t\t\"retention\": 0\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"queryInfo\": {},\n" +
                "\t\"queryId\": \"hive_20200909141218_d02e67b6-ae71-4e61-8669-d22764ebaf8a\",\n" +
                "\t\"queryStr\": \"create table lineage_test001\\r\\n(\\r\\n    c1 INT,\\r\\n    c2 STRING\\r\\n) STORED AS PARQUET\"\n" +
                "}\n";
        LineageMapping lineageMapping = hiveKafkaHandler.handle(record);
        Assert.assertNotNull(lineageMapping);
        lineageExecutor.handle(lineageMapping);
    }
}
