package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/23 15:41
 * @since 1.0.0
 */
@NoArgsConstructor
@Data
public class CreateTableEvent {


    /**
     * owner : hive
     * temporary : false
     * lastAccessTime : 1599631939000
     * qualifiedName : default.lineage_test001
     * columns : [{"name":"c1","type":"int"},{"name":"c2","type":"string"}]
     * storageDesc : {"bucketCols":[],"entity_type":"hive_storage_desc","qualifiedName":"default.lineage_test001_storage","storedAsSubDirectories":false,"location":"hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/lineage_test001","compressed":false,"inputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat","parameters":{},"outputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat","table":"lineage_test001","serdeInfo":{"serializationLib":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe","parameters":{"serialization.format":"1"}},"numBuckets":-1}
     * tableType : MANAGED_TABLE
     * entity_type : hive_table
     * createTime : 1599631939000
     * name : lineage_test001
     * parameters : {"totalSize":"0","numRows":"0","rawDataSize":"0","COLUMN_STATS_ACCURATE":"{\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"c1\":\"true\",\"c2\":\"true\"}}","numFiles":"0","transient_lastDdlTime":"1599631939","bucketing_version":"2"}
     * db : default
     * retention : 0
     */

    private String owner;
    private Boolean temporary;
    private Long lastAccessTime;
    private String qualifiedName;
    private StorageDescDTO storageDesc;
    private String tableType;
    @JsonProperty("entity_type")
    private String entityType;
    private Long createTime;
    private String name;
    private ParametersDTO parameters;
    private String db;
    private Integer retention;
    private List<ColumnsDTO> columns;

    @NoArgsConstructor
    @Data
    public static class StorageDescDTO {
        /**
         * bucketCols : []
         * entity_type : hive_storage_desc
         * qualifiedName : default.lineage_test001_storage
         * storedAsSubDirectories : false
         * location : hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/lineage_test001
         * compressed : false
         * inputFormat : org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
         * parameters : {}
         * outputFormat : org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
         * table : lineage_test001
         * serdeInfo : {"serializationLib":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe","parameters":{"serialization.format":"1"}}
         * numBuckets : -1
         */

        @JsonProperty("entity_type")
        private String entityType;
        private String qualifiedName;
        private Boolean storedAsSubDirectories;
        private String location;
        private Boolean compressed;
        private String inputFormat;
        private ParametersDTO parameters;
        private String outputFormat;
        private String table;
        private SerdeInfoDTO serdeInfo;
        private Integer numBuckets;
        private List<?> bucketCols;

        @NoArgsConstructor
        @Data
        public static class ParametersDTO {
        }

        @NoArgsConstructor
        @Data
        public static class SerdeInfoDTO {
            /**
             * serializationLib : org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
             * parameters : {"serialization.format":"1"}
             */

            private String serializationLib;
            private ParametersDTO parameters;

            @NoArgsConstructor
            @Data
            public static class ParametersDTO {
                @JsonProperty("serialization.format")
                private String serializationFormat;
            }
        }
    }

    @NoArgsConstructor
    @Data
    public static class ParametersDTO {
        /**
         * totalSize : 0
         * numRows : 0
         * rawDataSize : 0
         * COLUMN_STATS_ACCURATE : {"BASIC_STATS":"true","COLUMN_STATS":{"c1":"true","c2":"true"}}
         * numFiles : 0
         * transient_lastDdlTime : 1599631939
         * bucketing_version : 2
         */

        private String totalSize;
        private String numRows;
        private String rawDataSize;
        @JsonProperty("COLUMN_STATS_ACCURATE")
        private String columnStatsAccurate;
        private String numFiles;
        @JsonProperty("transient_lastDdlTime")
        private String transientLastDdlTime;
        @JsonProperty("bucketing_version")
        private String bucketingVersion;
    }

    @NoArgsConstructor
    @Data
    public static class ColumnsDTO {
        /**
         * name : c1
         * type : int
         */

        private String name;
        private String type;
    }
}
