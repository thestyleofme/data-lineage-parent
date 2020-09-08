package org.isaac.hive.hook.events;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.hooks.Entity;
import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.entity.HiveEntity;

/**
 * <p>
 * DropTable
 * </p>
 *
 * @author isaac 2020/9/7 16:55
 * @since 1.0.0
 */
public class DropTable extends BaseHiveEvent {

    public DropTable(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws Exception {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_TABLE);

        List<String> dropTables = null;
        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.TABLE) {
                String tblName = getQualifiedName(entity.getTable());
                if (dropTables == null) {
                    dropTables = new ArrayList<>();
                    ret.setAttribute(HiveEntity.KEY_DROP_TABLE, dropTables);
                }
                dropTables.add(tblName);
            }
        }
        return ret;
    }
}
