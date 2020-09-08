package org.isaac.hive.hook.events;

import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.entity.HiveEntity;

/**
 * <p>
 * AlterTableRename
 * </p>
 *
 * @author isaac 2020/9/7 16:42
 * @since 1.0.0
 */
public class AlterTableRename extends BaseHiveEvent {

    public AlterTableRename(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() throws HiveException {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_TABLE);

        if (CollectionUtils.isEmpty(getHiveContext().getInputs())) {
            return ret;
        }

        Table oldTable = getHiveContext().getInputs().iterator().next().getTable();
        Table newTable = null;

        if (CollectionUtils.isNotEmpty(getHiveContext().getOutputs())) {
            for (WriteEntity entity : getHiveContext().getOutputs()) {
                if (entity.getType() == Entity.Type.TABLE) {
                    newTable = entity.getTable();

                    // Hive sends with both old and new table names in the outputs which is weird.
                    // So skipping that with the below check
                    if (StringUtils.equalsIgnoreCase(newTable.getDbName(), oldTable.getDbName()) &&
                            StringUtils.equalsIgnoreCase(newTable.getTableName(), oldTable.getTableName())) {
                        newTable = null;
                    } else {
                        newTable = getHive().getTable(newTable.getDbName(), newTable.getTableName());
                    }
                    break;
                }
            }
        }

        if (newTable == null) {
            return ret;
        }

        Map<String, Object> oldTableEntity = toTableEntity(oldTable);
        Map<String, Object> renamedTableEntity = toTableEntity(newTable);

        // set previous name as the alias
        renamedTableEntity.put(ATTRIBUTE_ALIASES, oldTable.getTableName());

        ret.setAttribute(HiveEntity.KEY_TABLE_OLD, oldTableEntity);
        ret.setAttribute(HiveEntity.KEY_TABLE_RENAMED, renamedTableEntity);

        return ret;
    }
}
