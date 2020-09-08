package org.isaac.hive.hook.events;

import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.entity.HiveEntity;

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
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
}
