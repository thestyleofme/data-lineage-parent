package org.isaac.lineage.hook.hive;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.isaac.lineage.hook.hive.events.*;
import org.isaac.lineage.hook.hive.notification.NotificationInterface;
import org.isaac.lineage.hook.hive.notification.NotificationProvider;
import org.isaac.lineage.hook.hive.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Post-execution hooks
 * </p>
 *
 * @author isaac 2020/9/7 16:15
 * @since 1.0.0
 */
public class LineageHook implements ExecuteWithHookContext {

    private static final Logger LOG = LoggerFactory.getLogger(LineageHook.class);

    protected static NotificationInterface notificationInterface;
    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<>();
    private final JsonMapper jsonMapper;

    static {
        notificationInterface = NotificationProvider.get();
        for (HiveOperation hiveOperation : HiveOperation.values()) {
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }
    }

    public LineageHook() {
        this.jsonMapper = new JsonMapper();
    }

    @Override
    public void run(HookContext hookContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("LineageHook run() {}", hookContext.getOperationName());
        }
        try {
            HiveOperation hiveOperation = OPERATION_MAP.get(hookContext.getOperationName());
            HiveHookContext context = new HiveHookContext(hiveOperation, hookContext, jsonMapper);
            BaseHiveEvent event = genEvent(hiveOperation, context, hookContext);
            if (event != null) {
                String message = event.getNotificationMessages();
                if (StringUtils.isNotBlank(message)) {
                    notificationInterface.send(message);
                }
            }
        } catch (Exception e) {
            LOG.error("LineageHook run(), failed to process operation {}", hookContext.getOperationName(), e);
        }
    }

    private BaseHiveEvent genEvent(HiveOperation hiveOperation,
                                   HiveHookContext context,
                                   HookContext hookContext) {
        BaseHiveEvent event = null;
        switch (hiveOperation) {
            case CREATEDATABASE:
                event = new CreateDatabase(context);
                break;
            case DROPDATABASE:
                event = new DropDatabase(context);
                break;
            case ALTERDATABASE:
            case ALTERDATABASE_OWNER:
                event = new AlterDatabase(context);
                break;
            case CREATETABLE:
                event = new CreateTable(context, true);
                break;
            case DROPTABLE:
            case DROPVIEW:
                event = new DropTable(context);
                break;
            case ALTERTABLE_ADDPARTS:
            case CREATETABLE_AS_SELECT:
            case CREATEVIEW:
            case ALTERVIEW_AS:
            case LOAD:
            case EXPORT:
            case IMPORT:
            case QUERY:
            case TRUNCATETABLE:
                event = new CreateHiveProcess(context);
                break;
            case ALTERTABLE_DROPPARTS:
            case ALTERTABLE_FILEFORMAT:
            case ALTERTABLE_CLUSTER_SORT:
            case ALTERTABLE_BUCKETNUM:
            case ALTERTABLE_PROPERTIES:
            case ALTERVIEW_PROPERTIES:
            case ALTERTABLE_SERDEPROPERTIES:
            case ALTERTABLE_SERIALIZER:
            case ALTERTABLE_ADDCOLS:
            case ALTERTABLE_REPLACECOLS:
            case ALTERTABLE_PARTCOLTYPE:
            case ALTERTABLE_LOCATION:
                event = new AlterTable(context);
                break;
            case ALTERTABLE_RENAME:
            case ALTERVIEW_RENAME:
                event = new AlterTableRename(context);
                break;
            case ALTERTABLE_RENAMECOL:
                event = new AlterTableRenameCol(context);
                break;
            case SWITCHDATABASE:
            case SHOWDATABASES:
            case SHOWTABLES:
            case SHOW_CREATETABLE:
            case SHOWCOLUMNS:
            case SHOWPARTITIONS:
            case SHOWFUNCTIONS:
            case SHOW_TABLESTATUS:
            case SHOW_TBLPROPERTIES:
            case SHOWLOCKS:
            case DESCDATABASE:
            case DESCTABLE:
            case DESCFUNCTION:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("HiveHook run(), process operation {}", hookContext.getOperationName());
                }
                break;
            default:
                event = new DefaultEvent(context);
                break;
        }
        return event;
    }
}
