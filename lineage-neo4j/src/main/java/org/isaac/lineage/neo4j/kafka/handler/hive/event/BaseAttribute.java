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
 * @author isaac 2020/10/09 11:55
 * @since 1.0.0
 */
@NoArgsConstructor
@Data
public class BaseAttribute {

    /**
     * owner : hive
     * temporary : false
     * lastAccessTime : 1600864938000
     * qualifiedName : test.lineage_test003
     * columns : [{"name":"c1","type":"int"},{"name":"c2","type":"string"}]
     * storageDesc : {"bucketCols":[],"entity_type":"hive_storage_desc","qualifiedName":"test.lineage_test003_storage","storedAsSubDirectories":false,"location":"hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/test.db/lineage_test003","compressed":false,"inputFormat":"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat","parameters":{},"outputFormat":"org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat","table":"lineage_test003","serdeInfo":{"serializationLib":"org.apache.hadoop.hive.ql.io.orc.OrcSerde","parameters":{"serialization.format":"1"}},"numBuckets":-1}
     * tableType : MANAGED_TABLE
     * entity_type : hive_table
     * createTime : 1600864938000
     * name : lineage_test003
     * parameters : {"totalSize":"318","numRows":"2","rawDataSize":"186","COLUMN_STATS_ACCURATE":"{\"BASIC_STATS\":\"true\"}","numFiles":"1","transient_lastDdlTime":"1600864939","bucketing_version":"2"}
     * db : test
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
         * qualifiedName : test.lineage_test003_storage
         * storedAsSubDirectories : false
         * location : hdfs://hdspdemo001.hand-china.com:8020/warehouse/tablespace/managed/hive/test.db/lineage_test003
         * compressed : false
         * inputFormat : org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
         * parameters : {}
         * outputFormat : org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat
         * table : lineage_test003
         * serdeInfo : {"serializationLib":"org.apache.hadoop.hive.ql.io.orc.OrcSerde","parameters":{"serialization.format":"1"}}
         * numBuckets : -1
         */

        @JsonProperty("entity_type")
        private String entityType;
        private String qualifiedName;
        private Boolean storedAsSubDirectories;
        private String location;
        private Boolean compressed;
        private String inputFormat;
        private StorageDescDTO.ParametersDTO parameters;
        private String outputFormat;
        private String table;
        private StorageDescDTO.SerdeInfoDTO serdeInfo;
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
             * serializationLib : org.apache.hadoop.hive.ql.io.orc.OrcSerde
             * parameters : {"serialization.format":"1"}
             */

            private String serializationLib;
            private StorageDescDTO.SerdeInfoDTO.ParametersDTO parameters;

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
         * totalSize : 318
         * numRows : 2
         * rawDataSize : 186
         * COLUMN_STATS_ACCURATE : {"BASIC_STATS":"true"}
         * numFiles : 1
         * transient_lastDdlTime : 1600864939
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
