package org.isaac.lineage.hook.hive.lineage.bean;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * relationship model
 * </p>
 *
 * @author isaac 2020/9/7 16:59
 * @since 1.0.0
 */
public class Realationship {

    private long node1Id;
    private long node2Id;
    /**
     * from,hive
     */
    private String label;
    private Map<String, List<String>> propertyMap;

    public long getNode1Id() {
        return node1Id;
    }

    public void setNode1Id(long node1Id) {
        this.node1Id = node1Id;
    }

    public long getNode2Id() {
        return node2Id;
    }

    public void setNode2Id(long node2Id) {
        this.node2Id = node2Id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Map<String, List<String>> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, List<String>> propertyMap) {
        this.propertyMap = propertyMap;
    }
}
