package org.isaac.hive.hook.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.isaac.hive.hook.lineage.bean.ColumnLineage;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 17:21
 * @since 1.0.0
 */
public final class ListCloneUtil {

    public static ColumnLineage cloneColLine(ColumnLineage col) {
        return new ColumnLineage(
                col.getToNameParse(),
                col.getColConditions(),
                cloneSet(col.getFromNameSet()),
                cloneSet(col.getConditionSet()),
                col.getToTable(),
                col.getToName());
    }


    public static Set<String> cloneSet(Set<String> set) {
        Set<String> set2 = new HashSet<>(set.size());
        set2.addAll(set);
        return set2;
    }

    public static List<ColumnLineage> cloneList(List<ColumnLineage> list) {
        List<ColumnLineage> list2 = new ArrayList<>(list.size());
        for (ColumnLineage col : list) {
            list2.add(cloneColLine(col));
        }
        return list2;
    }
}
