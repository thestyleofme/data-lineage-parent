package org.isaac.lineage.hook.hive.events;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.isaac.lineage.hook.hive.HiveHookContext;
import org.isaac.lineage.hook.hive.entity.HiveEntity;

/**
 * <p>
 * AlterTable
 * </p>
 *
 * @author isaac 2020/9/7 16:42
 * @since 1.0.0
 */
public class AlterTable extends CreateTable {

    public AlterTable(HiveHookContext context) {
        super(context, true);
    }

    @Override
    public String getNotificationMessages() throws HiveException {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
}
