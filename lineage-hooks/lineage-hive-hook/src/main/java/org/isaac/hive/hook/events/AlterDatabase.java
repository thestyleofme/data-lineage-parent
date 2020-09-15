package org.isaac.hive.hook.events;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.entity.HiveEntity;

/**
 * <p>
 * AlterDatabase
 * </p>
 *
 * @author isaac 2020/9/7 16:41
 * @since 1.0.0
 */
public class AlterDatabase extends CreateDatabase {

    public AlterDatabase(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws HiveException {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }
}
