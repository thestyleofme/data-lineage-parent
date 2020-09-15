package org.isaac.lineage.hook.hive.events;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.hooks.Entity;
import org.isaac.lineage.hook.hive.HiveHookContext;
import org.isaac.lineage.hook.hive.entity.HiveEntity;

/**
 * <p>
 * DropDatabase
 * </p>
 *
 * @author isaac 2020/9/7 16:55
 * @since 1.0.0
 */
public class DropDatabase extends BaseHiveEvent {

    public DropDatabase(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_DB);

        List<String> dropTables = null;
        List<String> dropDbs = null;
        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.DATABASE) {
                String dbName = getQualifiedName(entity.getDatabase());
                if (dropDbs == null) {
                    dropDbs = new ArrayList<>();
                    ret.setAttribute(HiveEntity.KEY_DROP_DATABASE, dropDbs);
                }
                dropDbs.add(dbName);
            } else if (entity.getType() == Entity.Type.TABLE) {
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
