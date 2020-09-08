package org.isaac.hive.hook.events;

import java.net.URI;
import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyKey;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.isaac.hive.hook.HiveHookContext;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:45
 * @since 1.0.0
 */
public abstract class BaseHiveEvent {

    public static final String HIVE_ENTITY_TYPE = "entity_type";

    public static final String HIVE_TYPE_DEFAULT = "hive_default";

    public static final String HIVE_TYPE_DB = "hive_db";
    public static final String HIVE_TYPE_TABLE = "hive_table";
    public static final String HIVE_TYPE_STORAGE_DESC = "hive_storage_desc";
    public static final String HIVE_TYPE_COLUMN = "hive_column";
    public static final String HIVE_TYPE_PROCESS = "hive_process";
    public static final String HIVE_TYPE_COLUMN_LINEAGE = "hive_column_lineage";
    public static final String HIVE_TYPE_SERDE = "hive_serde";
    public static final String HIVE_TYPE_ORDER = "hive_order";
    public static final String HDFS_TYPE_PATH = "hdfs_path";
    public static final String HBASE_TYPE_TABLE = "hbase_table";
    public static final String HBASE_TYPE_NAMESPACE = "hbase_namespace";

    public static final String ATTRIBUTE_QUALIFIED_NAME = "qualifiedName";
    public static final String ATTRIBUTE_NAME = "name";
    public static final String ATTRIBUTE_DESCRIPTION = "description";
    public static final String ATTRIBUTE_OWNER = "owner";
    public static final String ATTRIBUTE_CLUSTER_NAME = "clusterName";
    public static final String ATTRIBUTE_LOCATION = "location";
    public static final String ATTRIBUTE_PARAMETERS = "parameters";
    public static final String ATTRIBUTE_OWNER_TYPE = "ownerType";
    public static final String ATTRIBUTE_COMMENT = "comment";
    public static final String ATTRIBUTE_CREATE_TIME = "createTime";
    public static final String ATTRIBUTE_LAST_ACCESS_TIME = "lastAccessTime";
    public static final String ATTRIBUTE_VIEW_ORIGINAL_TEXT = "viewOriginalText";
    public static final String ATTRIBUTE_VIEW_EXPANDED_TEXT = "viewExpandedText";
    public static final String ATTRIBUTE_TABLE_TYPE = "tableType";
    public static final String ATTRIBUTE_TEMPORARY = "temporary";
    public static final String ATTRIBUTE_RETENTION = "retention";
    public static final String ATTRIBUTE_DB = "db";
    public static final String ATTRIBUTE_STORAGE_DESC = "sd";
    public static final String ATTRIBUTE_PARTITION = "partition";
    public static final String ATTRIBUTE_PARTITION_PATHS = "partitionPaths";
    public static final String ATTRIBUTE_PARTITION_VALUES = "partitionValues";
    public static final String ATTRIBUTE_PARTITION_KEYS = "partitionKeys";
    public static final String ATTRIBUTE_COLUMNS = "columns";
    public static final String ATTRIBUTE_INPUT_FORMAT = "inputFormat";
    public static final String ATTRIBUTE_OUTPUT_FORMAT = "outputFormat";
    public static final String ATTRIBUTE_COMPRESSED = "compressed";
    public static final String ATTRIBUTE_BUCKET_COLS = "bucketCols";
    public static final String ATTRIBUTE_NUM_BUCKETS = "numBuckets";
    public static final String ATTRIBUTE_STORED_AS_SUB_DIRECTORIES = "storedAsSubDirectories";
    public static final String ATTRIBUTE_TABLE = "table";
    public static final String ATTRIBUTE_SERDE_INFO = "serdeInfo";
    public static final String ATTRIBUTE_SERIALIZATION_LIB = "serializationLib";
    public static final String ATTRIBUTE_SORT_COLS = "sortCols";
    public static final String ATTRIBUTE_COL_TYPE = "type";
    public static final String ATTRIBUTE_COL_POSITION = "position";
    public static final String ATTRIBUTE_PATH = "path";
    public static final String ATTRIBUTE_NAME_SERVICE_ID = "nameServiceId";
    public static final String ATTRIBUTE_INPUTS = "inputs";
    public static final String ATTRIBUTE_OUTPUTS = "outputs";
    public static final String ATTRIBUTE_OPERATION_TYPE = "operationType";
    public static final String ATTRIBUTE_START_TIME = "startTime";
    public static final String ATTRIBUTE_USER_NAME = "userName";
    public static final String ATTRIBUTE_QUERY_TEXT = "queryText";
    public static final String ATTRIBUTE_QUERY_ID = "queryId";
    public static final String ATTRIBUTE_QUERY_PLAN = "queryPlan";
    public static final String ATTRIBUTE_END_TIME = "endTime";
    public static final String ATTRIBUTE_RECENT_QUERIES = "recentQueries";
    public static final String ATTRIBUTE_QUERY = "query";
    public static final String ATTRIBUTE_DEPENDENCY_TYPE = "dependencyType";
    public static final String ATTRIBUTE_EXPRESSION = "expression";
    public static final String ATTRIBUTE_ALIASES = "aliases";
    public static final String ATTRIBUTE_URI = "uri";
    public static final String ATTRIBUTE_STORAGE_HANDLER = "storage_handler";
    public static final String ATTRIBUTE_NAMESPACE = "namespace";
    public static final String ATTRIBUTE_COLUMN_LINEAGE = "columnLineage";

    public static final String HBASE_STORAGE_HANDLER_CLASS = "org.apache.hadoop.hive.hbase.HBaseStorageHandler";
    public static final String HBASE_DEFAULT_NAMESPACE = "default";
    public static final String HBASE_NAMESPACE_TABLE_DELIMITER = ":";
    public static final String HBASE_PARAM_TABLE_NAME = "hbase.table.name";
    public static final long MILLIS_CONVERT_FACTOR = 1000;

    protected static final Map<Integer, String> OWNER_TYPE_TO_ENUM_VALUE = new HashMap<>();

    static {
        OWNER_TYPE_TO_ENUM_VALUE.put(1, "USER");
        OWNER_TYPE_TO_ENUM_VALUE.put(2, "ROLE");
        OWNER_TYPE_TO_ENUM_VALUE.put(3, "GROUP");
    }

    protected final HiveHookContext context;


    protected BaseHiveEvent(HiveHookContext context) {
        this.context = context;
    }

    public HiveHookContext getContext() {
        return context;
    }

    public String getNotificationMessages() throws HiveException {
        return null;
    }

    public static long getTableCreateTime(Table table) {
        return table.getTTable() != null ? (table.getTTable().getCreateTime() * MILLIS_CONVERT_FACTOR) :
                System.currentTimeMillis();
    }

    public static String getTableOwner(Table table) {
        return table.getTTable() != null ? (table.getOwner()) : "";
    }

    protected Map<String, Object> getInputOutputEntity(Entity entity) throws HiveException {
        Map<String, Object> ret = null;
        switch (entity.getType()) {
            case TABLE:
            case PARTITION:
            case DFS_DIR:
                ret = toHiveEntity(entity);
                break;
            default:
        }
        return ret;
    }

    protected Map<String, Object> toHiveEntity(Entity entity) throws HiveException {
        Map<String, Object> ret = null;

        switch (entity.getType()) {
            case DATABASE:
                Database db = getHive().getDatabase(entity.getDatabase().getName());
                ret = toDbEntity(db);
                break;
            case TABLE:
            case PARTITION:
                ret = toTableEntity(entity);
                break;
            case DFS_DIR:
                try {
                    URI location = entity.getLocation();
                    if (location != null) {
                        ret = getHdfsPathEntity(new Path(entity.getLocation()));
                    }
                } catch (Exception e) {
                    throw new HiveException(e);
                }
                break;
            default:
                break;
        }
        return ret;
    }

    protected Map<String, Object> toDbEntity(Database db) {
        String dbQualifiedName = getQualifiedName(db);

        Map<String, Object> ret = context.getEntity(dbQualifiedName);

        if (ret == null) {
            ret = new HashMap<>();
            ret.put(HIVE_ENTITY_TYPE, HIVE_TYPE_DB);
            ret.put(ATTRIBUTE_QUALIFIED_NAME, dbQualifiedName);
            ret.put(ATTRIBUTE_NAME, db.getName().toLowerCase());
            ret.put(ATTRIBUTE_DESCRIPTION, db.getDescription());
            ret.put(ATTRIBUTE_OWNER, db.getOwnerName());
            ret.put(ATTRIBUTE_LOCATION, db.getLocationUri());
            ret.put(ATTRIBUTE_PARAMETERS, db.getParameters());

            if (db.getOwnerType() != null) {
                ret.put(ATTRIBUTE_OWNER_TYPE, OWNER_TYPE_TO_ENUM_VALUE.get(db.getOwnerType().getValue()));
            }

            context.putEntity(dbQualifiedName, ret);
        }

        return ret;
    }

    protected Map<String, Object> toTableEntity(Entity entity) throws HiveException {
        Table table = getHive().getTable(entity.getTable().getDbName(), entity.getTable().getTableName());

        Map<String, Object> ret = toTableEntity(table);
        // add partition
        addTablePartition(ret, entity.getPartition());

        return ret;
    }

    protected void addTablePartition(Map<String, Object> objectMap, Partition partition) {
        if (partition != null) {
            objectMap.put(ATTRIBUTE_PARTITION, toPartitionEntity(partition));
        }
    }

    protected Map<String, Object> toTableEntity(Table table) {
        // HIVE_TYPE_TABLE
        String tblQualifiedName = getQualifiedName(table);

        Map<String, Object> ret = context.getEntity(tblQualifiedName);

        if (ret == null) {
            ret = new HashMap<>();

            long createTime = getTableCreateTime(table);
            long lastAccessTime = table.getLastAccessTime() > 0 ? (table.getLastAccessTime() * MILLIS_CONVERT_FACTOR) : createTime;
            ret.put(HIVE_ENTITY_TYPE, HIVE_TYPE_TABLE);
            ret.put(ATTRIBUTE_DB, table.getDbName());
            ret.put(ATTRIBUTE_QUALIFIED_NAME, tblQualifiedName);
            ret.put(ATTRIBUTE_NAME, table.getTableName().toLowerCase());
            ret.put(ATTRIBUTE_OWNER, table.getOwner());
            ret.put(ATTRIBUTE_CREATE_TIME, createTime);
            ret.put(ATTRIBUTE_LAST_ACCESS_TIME, lastAccessTime);
            ret.put(ATTRIBUTE_RETENTION, table.getRetention());
            ret.put(ATTRIBUTE_PARAMETERS, table.getParameters());
            // ret.put(ATTRIBUTE_COMMENT, table.getParameters().get(ATTRIBUTE_COMMENT));
            ret.put(ATTRIBUTE_TABLE_TYPE, table.getTableType().name());
            ret.put(ATTRIBUTE_TEMPORARY, table.isTemporary());

            if (table.getViewOriginalText() != null) {
                ret.put(ATTRIBUTE_VIEW_ORIGINAL_TEXT, table.getViewOriginalText());
            }

            if (table.getViewExpandedText() != null) {
                ret.put(ATTRIBUTE_VIEW_EXPANDED_TEXT, table.getViewExpandedText());
            }

            // set partition keys list
            List<Map<String, Object>> partitionKeys = getColumnEntities(table, table.getPartitionKeys());
            if ((partitionKeys != null) && !partitionKeys.isEmpty()) {
                ret.put(ATTRIBUTE_PARTITION_KEYS, partitionKeys);
            }

            // set column list
            List<Map<String, Object>> columns = getColumnEntities(table, table.getCols());
            if ((columns != null) && !columns.isEmpty()) {
                ret.put(ATTRIBUTE_COLUMNS, columns);
            }

            // set storage desc
            Map<String, Object> sd = getStorageDescEntity(table);
            ret.put(ATTRIBUTE_STORAGE_DESC, sd);

            context.putEntity(tblQualifiedName, ret);
        }

        return ret;
    }

    protected Map<String, Object> toPartitionEntity(Partition partition) {
        Map<String, Object> ret = null;
        if (partition != null) {
            ret = new HashMap<>();
            ret.put(ATTRIBUTE_NAME, partition.getName());
            ret.put(ATTRIBUTE_LOCATION, partition.getLocation());
            ret.put(ATTRIBUTE_PARAMETERS, partition.getParameters());
            ret.put(ATTRIBUTE_PARTITION_VALUES, partition.getValues());
            Path[] paths = partition.getPath();
            if ((paths != null) && (paths.length > 0)) {
                List<String> pathList = new ArrayList<>();
                for (Path path : paths) {
                    if (path != null) {
                        pathList.add(path.toString());
                    }
                }
                ret.put(ATTRIBUTE_PARTITION_PATHS, pathList);
            }
        }
        return ret;
    }

    protected Map<String, Object> getStorageDescEntity(Table table) {
        String sdQualifiedName = getQualifiedName(table, table.getSd());
        Map<String, Object> ret = new HashMap<>();

        StorageDescriptor sd = table.getSd();

        ret.put(HIVE_ENTITY_TYPE, HIVE_TYPE_STORAGE_DESC);
        ret.put(ATTRIBUTE_TABLE, table.getTableName());
        ret.put(ATTRIBUTE_QUALIFIED_NAME, sdQualifiedName);
        ret.put(ATTRIBUTE_PARAMETERS, sd.getParameters());
        ret.put(ATTRIBUTE_LOCATION, sd.getLocation());
        ret.put(ATTRIBUTE_INPUT_FORMAT, sd.getInputFormat());
        ret.put(ATTRIBUTE_OUTPUT_FORMAT, sd.getOutputFormat());
        ret.put(ATTRIBUTE_COMPRESSED, sd.isCompressed());
        ret.put(ATTRIBUTE_NUM_BUCKETS, sd.getNumBuckets());
        ret.put(ATTRIBUTE_STORED_AS_SUB_DIRECTORIES, sd.isStoredAsSubDirectories());

        if (sd.getBucketCols() != null && sd.getBucketCols().isEmpty()) {
            ret.put(ATTRIBUTE_BUCKET_COLS, sd.getBucketCols());
        }

        if (sd.getSerdeInfo() != null) {
            SerDeInfo sdSerDeInfo = sd.getSerdeInfo();

            Map<String, Object> serdeInfo = new HashMap<>();
            serdeInfo.put(ATTRIBUTE_NAME, sdSerDeInfo.getName());
            serdeInfo.put(ATTRIBUTE_SERIALIZATION_LIB, sdSerDeInfo.getSerializationLib());
            serdeInfo.put(ATTRIBUTE_PARAMETERS, sdSerDeInfo.getParameters());

            ret.put(ATTRIBUTE_SERDE_INFO, serdeInfo);
        }

        if (CollectionUtils.isNotEmpty(sd.getSortCols())) {
            List<Map<String, Object>> sortCols = new ArrayList<>(sd.getSortCols().size());

            for (Order sdSortCol : sd.getSortCols()) {
                Map<String, Object> sortCol = new HashMap<>();
                sortCol.put("col", sdSortCol.getCol());
                sortCol.put("order", sdSortCol.getOrder());
                sortCols.add(sortCol);
            }

            ret.put(ATTRIBUTE_SORT_COLS, sortCols);
        }
        return ret;
    }

    protected List<Map<String, Object>> getColumnEntities(Table table, List<FieldSchema> fieldSchemas) {
        List<Map<String, Object>> ret = new ArrayList<>();

        // int columnPosition = 0;
        for (FieldSchema fieldSchema : fieldSchemas) {
            String colQualifiedName = getQualifiedName(table, fieldSchema);
            Map<String, Object> columnInfo = context.getEntity(colQualifiedName);

            if (columnInfo == null) {
                columnInfo = new HashMap<>();
                // columnInfo.put(ATTRIBUTE_TABLE, table.getTableName());
                // columnInfo.put(ATTRIBUTE_QUALIFIED_NAME, colQualifiedName);
                columnInfo.put(ATTRIBUTE_NAME, fieldSchema.getName());
                // columnInfo.put(ATTRIBUTE_OWNER, table.getOwner());
                columnInfo.put(ATTRIBUTE_COL_TYPE, fieldSchema.getType());
                // columnInfo.put(ATTRIBUTE_COL_POSITION, columnPosition++);
                columnInfo.put(ATTRIBUTE_COMMENT, fieldSchema.getComment());

                context.putEntity(colQualifiedName, columnInfo);
            }
            ret.add(columnInfo);
        }

        return ret;
    }

    protected Map<String, Object> getHdfsPathEntity(Path path) {
        // HDFS_TYPE_PATH
        String strPath = path.toString().toLowerCase();

        Map<String, Object> ret = new HashMap<>();

        ret.put(HIVE_ENTITY_TYPE, HDFS_TYPE_PATH);
        ret.put(ATTRIBUTE_PATH, strPath);
        ret.put(ATTRIBUTE_NAME, Path.getPathWithoutSchemeAndAuthority(path).toString().toLowerCase());

        return ret;
    }

    protected Hive getHive() throws HiveException {
        return context.getHive();
    }

    protected HookContext getHiveContext() {
        return context.getHiveContext();
    }

    protected String getQualifiedName(Entity entity) throws HiveException {
        switch (entity.getType()) {
            case DATABASE:
                return getQualifiedName(entity.getDatabase());
            case TABLE:
            case PARTITION:
                return getQualifiedName(entity.getTable());
            case DFS_DIR:
                try {
                    return getQualifiedName(entity.getLocation());
                } catch (Exception e) {
                    throw new HiveException(e);
                }
            default:
                return null;
        }
    }

    protected String getQualifiedName(Database db) {
        return context.getQualifiedName(db);
    }

    protected String getQualifiedName(Table table) {
        return context.getQualifiedName(table);
    }

    protected String getQualifiedName(Table table, StorageDescriptor sd) {
        return getQualifiedName(table) + "_storage";
    }

    protected String getQualifiedName(Table table, FieldSchema column) {
        String tblQualifiedName = getQualifiedName(table);
        return tblQualifiedName + HiveHookContext.QNAME_SEP_ENTITY_NAME + column.getName().toLowerCase();
    }

    protected String getQualifiedName(DependencyKey column) {
        String dbName = column.getDataContainer().getTable().getDbName();
        String tableName = column.getDataContainer().getTable().getTableName();
        String colName = column.getFieldSchema().getName();

        return getQualifiedName(dbName, tableName, colName);
    }

    protected String getQualifiedName(BaseColumnInfo column) {
        String dbName = column.getTabAlias().getTable().getDbName();
        String tableName = column.getTabAlias().getTable().getTableName();
        String colName = column.getColumn() != null ? column.getColumn().getName() : null;

        if (colName == null) {
            return (dbName + HiveHookContext.QNAME_SEP_ENTITY_NAME + tableName).toLowerCase();
        } else {
            return (dbName + HiveHookContext.QNAME_SEP_ENTITY_NAME + tableName + HiveHookContext.QNAME_SEP_ENTITY_NAME + colName).toLowerCase();
        }
    }

    protected String getQualifiedName(String dbName, String tableName, String colName) {
        return (dbName + HiveHookContext.QNAME_SEP_ENTITY_NAME + tableName + HiveHookContext.QNAME_SEP_ENTITY_NAME + colName).toLowerCase();
    }

    protected String getQualifiedName(URI location) {
        return new Path(location).toString().toLowerCase();
    }

    protected String getColumnQualifiedName(String tblQualifiedName, String columnName) {
        return tblQualifiedName + HiveHookContext.QNAME_SEP_ENTITY_NAME + columnName.toLowerCase();
    }

    protected Map<String, Object> toReferencedHBaseTable(Table table) {
        // HBASE_TYPE_TABLE
        Map<String, Object> ret = null;
        HBaseTableInfo hBaseTableInfo = new HBaseTableInfo(table);
        String hbaseNameSpace = hBaseTableInfo.getHbaseNameSpace();
        String hbaseTableName = hBaseTableInfo.getHbaseTableName();

        if (hbaseTableName != null) {
            ret = new HashMap<>();
            // set hbase namespace
            Map<String, Object> hbaseTypeNamespace = new HashMap<>();
            hbaseTypeNamespace.put(ATTRIBUTE_NAME, hbaseNameSpace);
            hbaseTypeNamespace.put(ATTRIBUTE_QUALIFIED_NAME, hbaseNameSpace.toLowerCase());

            ret.put(HIVE_ENTITY_TYPE, HBASE_TYPE_TABLE);
            ret.put(HBASE_TYPE_NAMESPACE, hbaseTypeNamespace);
            ret.put(ATTRIBUTE_NAME, hbaseTableName);
            ret.put(ATTRIBUTE_URI, hbaseTableName);
            // ret.put(ATTRIBUTE_NAMESPACE, getObjectId(nsEntity));
            ret.put(ATTRIBUTE_QUALIFIED_NAME, getHBaseTableQualifiedName(hbaseNameSpace, hbaseTableName));
        }

        return ret;
    }

    protected boolean isHBaseStore(Table table) {
        boolean ret = false;
        Map<String, String> parameters = table.getParameters();
        if (MapUtils.isNotEmpty(parameters)) {
            String storageHandler = parameters.get(ATTRIBUTE_STORAGE_HANDLER);
            ret = (storageHandler != null && storageHandler.equals(HBASE_STORAGE_HANDLER_CLASS));
        }
        return ret;
    }

    private static String getHBaseTableQualifiedName(String nameSpace, String tableName) {
        return String.format("%s:%s", nameSpace.toLowerCase(), tableName.toLowerCase());
    }

    static final class EntityComparator implements Comparator<Entity> {
        @Override
        public int compare(Entity entity1, Entity entity2) {
            String name1 = entity1.getName();
            String name2 = entity2.getName();
            if (name1 == null || name2 == null) {
                name1 = entity1.getD().toString();
                name2 = entity2.getD().toString();
            }
            return name1.toLowerCase().compareTo(name2.toLowerCase());
        }
    }

    static final Comparator<Entity> entityComparator = new EntityComparator();

    static final class HBaseTableInfo {
        String hbaseNameSpace = null;
        String hbaseTableName = null;

        HBaseTableInfo(Table table) {
            Map<String, String> parameters = table.getParameters();

            if (MapUtils.isNotEmpty(parameters)) {
                hbaseNameSpace = HBASE_DEFAULT_NAMESPACE;
                hbaseTableName = parameters.get(HBASE_PARAM_TABLE_NAME);

                if (hbaseTableName != null && (hbaseTableName.contains(HBASE_NAMESPACE_TABLE_DELIMITER))) {
                    String[] hbaseTableInfo = hbaseTableName.split(HBASE_NAMESPACE_TABLE_DELIMITER);
                    if (hbaseTableInfo.length > 1) {
                        hbaseNameSpace = hbaseTableInfo[0];
                        hbaseTableName = hbaseTableInfo[1];
                    }
                }
            }
        }

        public String getHbaseNameSpace() {
            return hbaseNameSpace;
        }

        public String getHbaseTableName() {
            return hbaseTableName;
        }
    }
}
