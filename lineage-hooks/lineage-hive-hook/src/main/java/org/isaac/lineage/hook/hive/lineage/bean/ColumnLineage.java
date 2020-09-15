package org.isaac.lineage.hook.hive.lineage.bean;

import java.util.List;
import java.util.Set;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:57
 * @since 1.0.0
 */
public class ColumnLineage {

    /**
     * column name from sql
     */
    private String toNameParse;
    /**
     * Source field with condition list
     */
    private List<String> colConditions;
    /**
     * Source field list
     */
    private Set<String> fromNameSet;
    /**
     * Column condition
     */
    private Set<String> conditionSet;

    /**
     * output table
     */
    private String toTable;
    /**
     * Column name from metadata
     */
    private String toName;

    public ColumnLineage(String toNameParse, List<String> colConditions, Set<String> fromNameSet, Set<String> conditionSet,
                         String toTable, String toName) {
        this.toNameParse = toNameParse;
        this.colConditions = colConditions;
        this.fromNameSet = fromNameSet;
        this.conditionSet = conditionSet;
        this.toTable = toTable;
        this.toName = toName;
    }

    public String getToNameParse() {
        return toNameParse;
    }

    public void setToNameParse(String toNameParse) {
        this.toNameParse = toNameParse;
    }

    public List<String> getColConditions() {
        return colConditions;
    }

    public void setColConditions(List<String> colConditions) {
        this.colConditions = colConditions;
    }

    public void addColCondition(String condition) {
        this.colConditions.add(condition);
    }

    public void addColCondition(List<String> conditions) {
        if (conditions != null) {
            this.colConditions.addAll(conditions);
        }
    }

    public Set<String> getFromNameSet() {
        return fromNameSet;
    }

    public void setFromNameSet(Set<String> fromNameSet) {
        this.fromNameSet = fromNameSet;
    }

    public Set<String> getConditionSet() {
        return conditionSet;
    }

    public void setConditionSet(Set<String> conditionSet) {
        this.conditionSet = conditionSet;
    }

    public String getToTable() {
        return toTable;
    }

    public void setToTable(String toTable) {
        this.toTable = toTable;
    }

    public String getToName() {
        return toName;
    }

    public void setToName(String toName) {
        this.toName = toName;
    }

    public String getToColumnQualifiedName() {
        return getToTable() + "." + getToNameParse();
    }
}