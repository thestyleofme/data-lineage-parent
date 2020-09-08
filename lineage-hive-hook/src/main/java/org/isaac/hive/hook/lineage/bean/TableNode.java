package org.isaac.hive.hook.lineage.bean;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:59
 * @since 1.0.0
 */
public class TableNode {
    /**
     * table node id
     */
    private long id;
    /**
     * table name
     */
    private String table;
    /**
     * database name
     */
    private String db;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
