package org.isaac.lineage.hook.hive.lineage.bean;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:57
 * @since 1.0.0
 */
public class ColumnBlock {
    /**
     * column condition
     */
    private String condition;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnBlock that = (ColumnBlock) o;
        if (!Objects.equals(condition, that.condition)) {
            return false;
        }
        return columnSet != null ? columnSet.equals(that.columnSet) : that.columnSet == null;
    }

    @Override
    public int hashCode() {
        int result = condition != null ? condition.hashCode() : 0;
        result = 31 * result + (columnSet != null ? columnSet.hashCode() : 0);
        return result;
    }

    /**
     * column list
     */
    private Set<String> columnSet = new LinkedHashSet<>();

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public Set<String> getColumnSet() {
        return columnSet;
    }

    public void setColumnSet(Set<String> columnSet) {
        this.columnSet = columnSet;
    }
}