package org.isaac.hive.hook.events;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.entity.HiveEntity;

/**
 * <p>
 * AlterTableRenameCol
 * </p>
 *
 * @author isaac 2020/9/7 16:45
 * @since 1.0.0
 */
public class AlterTableRenameCol extends AlterTable {
    public AlterTableRenameCol(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    @Override
    public HiveEntity getEntity() throws HiveException {
        if (CollectionUtils.isEmpty(getHiveContext().getInputs())) {
            return null;
        }

        if (CollectionUtils.isEmpty(getHiveContext().getOutputs())) {
            return null;
        }

        HiveEntity entity = context.createHiveEntity();
        entity.setTypeName(HIVE_TYPE_COLUMN);

        Table oldTable = getHiveContext().getInputs().iterator().next().getTable();
        Table newTable = getHiveContext().getOutputs().iterator().next().getTable();

        newTable = getHive().getTable(newTable.getDbName(), newTable.getTableName());

        List<FieldSchema> oldColumns = oldTable.getCols();
        List<FieldSchema> newColumns = newTable.getCols();
        FieldSchema changedColumnOld = null;
        FieldSchema changedColumnNew = null;

        for (FieldSchema oldColumn : oldColumns) {
            if (!newColumns.contains(oldColumn)) {
                changedColumnOld = oldColumn;
                break;
            }
        }

        for (FieldSchema newColumn : newColumns) {
            if (!oldColumns.contains(newColumn)) {
                changedColumnNew = newColumn;
                break;
            }
        }

        if (changedColumnOld != null && changedColumnNew != null) {
            String oldColumn = getQualifiedName(oldTable, changedColumnOld);
            String newColumn = getQualifiedName(newTable, changedColumnNew);

            Map<String, Object> column = new HashMap<>();
            column.put(HiveEntity.KEY_TABLE_COLUMN_OLD, oldColumn);
            column.put(HiveEntity.KEY_TABLE_COLUMN_NEW, newColumn);

            entity.setAttribute(ATTRIBUTE_COLUMNS, column);
        }
        return entity;
    }
}
