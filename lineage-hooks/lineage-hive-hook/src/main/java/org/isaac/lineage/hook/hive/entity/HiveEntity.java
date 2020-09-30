package org.isaac.lineage.hook.hive.entity;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.isaac.lineage.hook.hive.exceptions.HiveHookException;
import org.isaac.lineage.hook.hive.utils.PathUtils;

/**
 * <p>
 * An instance of an entity - like hive_table, hive_database
 * </p>
 *
 * @author isaac 2020/9/7 16:33
 * @since 1.0.0
 */
public class HiveEntity implements Serializable {

    private static final long serialVersionUID = 5426400334795203657L;

    public static final String KEY_HOOK_TYPE = "hookType";
    public static final String KEY_TYPENAME = "typeName";
    public static final String KEY_CREATED_BY = "createdBy";
    public static final String KEY_CREATE_TIME = "createTime";
    public static final String KEY_OPERATION_NAME = "operationName";
    public static final String KEY_IP_ADDRESS = "ipAddress";
    public static final String KEY_ATTRIBUTES = "attributes";
    public static final String KEY_EXECUTOR_ADDRESS = "executorAddress";
    /**
     * query info
     */
    public static final String KEY_QUERY_INFO = "queryInfo";
    public static final String KEY_QUERY_ID = "queryId";
    public static final String KEY_QUERY_STR = "queryStr";
    public static final String KEY_QUERY_COUNTERS = "queryCounters";
    public static final String KEY_QUERY_FETCH_TASK_ID = "queryFetchTaskId";
    public static final String KEY_QUERY_FETCH_TASK_JOB_ID = "queryFetchTaskJobId";
    public static final String KEY_QUERY_START_TIME = "queryStartTime";
    public static final String KEY_QUERY_MR_TASKS = "mrTasks";
    /**
     * work info
     */
    public static final String KEY_WORK_AVG_ROW_SIZE = "avgRowSize";
    public static final String KEY_WORK_DATA_SIZE = "dataSize";
    public static final String KEY_WORK_NUM_ROWS = "numRows";
    public static final String KEY_WORK_NUM_TASKS = "numTasks";
    /**
     * map task info
     */
    public static final String KEY_MAP_WORK_INFO = "mapWork";
    /**
     * reduce task info
     */
    public static final String KEY_REDUCE_WORK_INFO = "reduceWork";
    /**
     * table
     */
    public static final String KEY_TABLE_OLD = "oldTable";
    public static final String KEY_TABLE_RENAMED = "renamedTable";
    public static final String KEY_TABLE_COLUMN_OLD = "oldColumn";
    public static final String KEY_TABLE_COLUMN_NEW = "newColumn";
    /**
     * drop
     */
    public static final String KEY_DROP_TABLE = "dropTable";
    public static final String KEY_DROP_DATABASE = "dropDatabase";

    private final transient Map<String, Object> entity;
    private transient Map<String, Object> attributes;

    private static volatile Properties hiveHookProperties;

    public static void get() {
        if (hiveHookProperties == null) {
            synchronized (HiveEntity.class) {
                if (hiveHookProperties == null) {
                    hiveHookProperties = readHiveHookProperties();
                }
            }
        }
    }

    public HiveEntity() {
        this.entity = new HashMap<>();
        // kafka里有多个来源 这里标识下是从hive hook发送的 方便后面使用此sourceType进行解析
        entity.put("sourceType", "HIVE-HOOK");
        // 读取文件放入额外信息到hook中 主要是做系统以及集群血缘
        get();
        Set<Map.Entry<Object, Object>> entries = hiveHookProperties.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            entity.put(String.valueOf(entry.getKey()), entry.getValue());
        }
    }

    /**
     * hook.properties
     * clusterName=DEFAULT
     * platformName=DEFAULT
     */
    private static Properties readHiveHookProperties() {
        Properties properties = new Properties();
        try {
            String confLocation = PathUtils.getProjectPath();
            File file = new File(confLocation, "hook.properties");
            FileInputStream fileInputStream = new FileInputStream(file);
            properties.load(fileInputStream);
            return properties;
        } catch (IOException e) {
            throw new HiveHookException(e);
        }
    }

    public Map<String, Object> getResult() {
        if ((attributes != null) && !entity.containsKey(KEY_ATTRIBUTES)) {
            entity.put(KEY_ATTRIBUTES, attributes);
        }
        return entity;
    }

    public void setHookType(String hookType) {
        entity.put(KEY_HOOK_TYPE, hookType);
    }

    public String getTypeName() {
        return (String) entity.get(KEY_TYPENAME);
    }

    public void setTypeName(String typeName) {
        entity.put(KEY_TYPENAME, typeName);
    }

    public String getCreatedBy() {
        return (String) entity.get(KEY_CREATED_BY);
    }

    public void setCreatedBy(String createdBy) {
        entity.put(KEY_CREATED_BY, createdBy);
    }

    public long getCreateTime() {
        return (long) entity.get(KEY_CREATE_TIME);
    }

    public void setCreateTime(long createTime) {
        entity.put(KEY_CREATE_TIME, createTime);
    }

    public String getOperationName() {
        return (String) entity.get(KEY_OPERATION_NAME);
    }

    public void setOperationName(String operationName) {
        entity.put(KEY_OPERATION_NAME, operationName);
    }

    public String getIpAddress() {
        return (String) entity.get(KEY_IP_ADDRESS);
    }

    public void setIpAddress(String ipAddress) {
        entity.put(KEY_IP_ADDRESS, ipAddress);
    }

    public String getExecutorAddress() {
        return (String) entity.get(KEY_EXECUTOR_ADDRESS);
    }

    public void setExecutorAddress(String executorAddress) {
        entity.put(KEY_EXECUTOR_ADDRESS, executorAddress);
    }

    public void setQueryId(String queryId) {
        entity.put(KEY_QUERY_ID, queryId);
    }

    public String getQueryId() {
        return (String) entity.get(KEY_QUERY_ID);
    }

    public void setQueryStr(String queryStr) {
        entity.put(KEY_QUERY_STR, queryStr);
    }

    public String getQueryStr() {
        return (String) entity.get(KEY_QUERY_STR);
    }

    public void setQueryStartTime(Long queryStartTime) {
        entity.put(KEY_QUERY_START_TIME, queryStartTime);
    }

    public Long getQueryStartTime() {
        return (Long) entity.get(KEY_QUERY_START_TIME);
    }

    public void setQueryInfo(Map<String, Object> queryInfo) {
        entity.put(KEY_QUERY_INFO, queryInfo);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public boolean hasAttribute(String name) {
        Map<String, Object> a = this.attributes;
        return a != null && a.containsKey(name);
    }

    public Object getAttribute(String name) {
        Map<String, Object> a = this.attributes;
        return a != null ? a.get(name) : null;
    }

    public void setAttribute(String name, Object value) {
        Map<String, Object> a = this.attributes;
        if (a != null) {
            a.put(name, value);
        } else {
            a = new HashMap<>();
            a.put(name, value);
            this.attributes = a;
        }
    }

    public void removeAttribute(String name) {
        Map<String, Object> a = this.attributes;
        if (a != null) {
            a.remove(name);
        }
    }
}