package org.isaac.hive.hook.events;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.entity.HiveEntity;
import org.isaac.hive.hook.lineage.LineageParser;
import org.isaac.hive.hook.lineage.bean.ColumnLineage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * CreateHiveProcess
 * </p>
 *
 * @author isaac 2020/9/7 16:52
 * @since 1.0.0
 */
public class CreateHiveProcess extends BaseHiveEvent {

    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveProcess.class);

    public CreateHiveProcess(HiveHookContext context) {
        super(context);
    }

    @Override
    public String getNotificationMessages() throws HiveException {
        HiveEntity entity = getEntity();
        return context.toJson(entity.getResult());
    }

    public HiveEntity getEntity() throws HiveException {
        HiveEntity ret = context.createHiveEntity();
        ret.setTypeName(HIVE_TYPE_PROCESS);

        if (!skipProcess()) {
            List<Map<String, Object>> inputs = new ArrayList<>();
            List<Map<String, Object>> outputs = new ArrayList<>();
            HookContext hiveContext = getHiveContext();

            if (hiveContext.getInputs() != null) {
                for (ReadEntity input : hiveContext.getInputs()) {
                    String qualifiedName = getQualifiedName(input);

                    if (qualifiedName == null) {
                        continue;
                    }

                    Map<String, Object> entity = getInputOutputEntity(input);

                    if (entity != null) {
                        inputs.add(entity);
                    }
                }
            }

            if (hiveContext.getOutputs() != null) {
                for (WriteEntity output : hiveContext.getOutputs()) {
                    String qualifiedName = getQualifiedName(output);

                    if (qualifiedName == null) {
                        continue;
                    }

                    Map<String, Object> entity = getInputOutputEntity(output);

                    if (entity != null) {
                        outputs.add(entity);
                    }
                }
            }

            if (!inputs.isEmpty() || !outputs.isEmpty()) {
                ret.setAttribute(ATTRIBUTE_INPUTS, inputs);
                ret.setAttribute(ATTRIBUTE_OUTPUTS, outputs);
                List<Map<String, Object>> columnLineage = processColumnLineageWithParser();
                if (!columnLineage.isEmpty()) {
                    ret.setAttribute(ATTRIBUTE_COLUMN_LINEAGE, columnLineage);
                }
            }
        }

        return ret;
    }

    private List<Map<String, Object>> processColumnLineageWithParser() {
        List<Map<String, Object>> ret = new ArrayList<>();
        try {
            LineageParser parser = new LineageParser(context);
            parser.getLineageInfo(context.getQueryStr(false));

            List<ColumnLineage> colLines = parser.getColumnLineages();
            if (colLines != null) {
                for (ColumnLineage col : colLines) {
                    String outputColName = col.getToColumnQualifiedName();

                    List<Map<String, Object>> inputColumns = new ArrayList<>();
                    for (String fromColumnName : col.getFromNameSet()) {
                        Map<String, Object> inputColumn = new HashMap<>();
                        inputColumn.put(ATTRIBUTE_QUALIFIED_NAME, fromColumnName);
                        inputColumns.add(inputColumn);
                    }

                    if (inputColumns.isEmpty()) {
                        continue;
                    }

                    Map<String, Object> columnLineageProcess = new HashMap<>();
                    columnLineageProcess.put(ATTRIBUTE_NAME, col.getToNameParse());
                    columnLineageProcess.put(ATTRIBUTE_QUALIFIED_NAME, outputColName);
                    columnLineageProcess.put(ATTRIBUTE_INPUTS, inputColumns);
                    columnLineageProcess.put(ATTRIBUTE_EXPRESSION, col.getColConditions());

                    ret.add(columnLineageProcess);
                }
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(e.getMessage(), e);
            }
        }
        return ret;
    }

    /*
    private List<Map<String, Object>> processColumnLineage() {
    	List<Map<String, Object>> ret = new ArrayList<>();
        LineageInfo lineageInfo = getHiveContext().getLinfo();

        if ((lineageInfo == null) || CollectionUtils.isEmpty(lineageInfo.entrySet())) {
            return ret;
        }

        for (Map.Entry<DependencyKey, Dependency> entry : lineageInfo.entrySet()) {
            String outputColName = getQualifiedName(entry.getKey());
            //Map<String, Object> outputColumn = context.getEntity(outputColName);

            //if (outputColumn == null) {
            //    LOG.warn("column-lineage: non-existing output-column {}", outputColName);
            //    continue;
            //}

            List<Map<String, Object>> inputColumns = new ArrayList<>();
            
            for (BaseColumnInfo baseColumn : getBaseCols(entry.getValue())) {
                String inputColName = getQualifiedName(baseColumn);
                //Map<String, Object> inputColumn = context.getEntity(inputColName);

                //if (inputColumn == null) {
                //    LOG.warn("column-lineage: non-existing input-column {} for output-column={}", inputColName, outputColName);
                //    continue;
                //}
                
                Map<String, Object> inputColumn = new HashMap<>();
                inputColumn.put(ATTRIBUTE_TABLE, baseColumn.getColumn().getName());
                inputColumn.put(ATTRIBUTE_QUALIFIED_NAME, inputColName);
                //inputColumn.put(ATTRIBUTE_EXPRESSION, entry.getValue().getExpr());
                //inputColumn.put(ATTRIBUTE_DEPENDENCY_TYPE, entry.getValue().getType());

                inputColumns.add(inputColumn);
            }

            if (inputColumns.isEmpty()) {
                continue;
            }
            
            //HIVE_TYPE_COLUMN_LINEAGE
            Map<String, Object> columnLineageProcess = new HashMap<>();

            columnLineageProcess.put(ATTRIBUTE_NAME, entry.getKey().getFieldSchema().getName());
            columnLineageProcess.put(ATTRIBUTE_QUALIFIED_NAME, outputColName);
            columnLineageProcess.put(ATTRIBUTE_INPUTS, inputColumns);
            //columnLineageProcess.put(ATTRIBUTE_OUTPUTS, Collections.singletonList(outputColName));
            //columnLineageProcess.put(ATTRIBUTE_QUERY, getObjectId(hiveProcess));
            columnLineageProcess.put(ATTRIBUTE_DEPENDENCY_TYPE, entry.getValue().getType());
            columnLineageProcess.put(ATTRIBUTE_EXPRESSION, entry.getValue().getExpr());

            ret.add(columnLineageProcess);
        }
        return ret;
    }

    private Collection<BaseColumnInfo> getBaseCols(Dependency lInfoDep) {
        Collection<BaseColumnInfo> ret = Collections.emptyList();

        if (lInfoDep != null) {
            try {
                Method getBaseColsMethod = lInfoDep.getClass().getMethod("getBaseCols");

                Object retGetBaseCols = getBaseColsMethod.invoke(lInfoDep);

                if (retGetBaseCols != null) {
                    if (retGetBaseCols instanceof Collection) {
                        ret = (Collection) retGetBaseCols;
                    } else {
                        LOG.warn("{}: unexpected return type from LineageInfo.Dependency.getBaseCols(), expected type {}",
                                retGetBaseCols.getClass().getName(), "Collection");
                    }
                }
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                LOG.warn("getBaseCols()", ex);
            }
        }

        return ret;
    }
	*/

    private boolean skipProcess() {
        Set<ReadEntity> inputs = getHiveContext().getInputs();
        Set<WriteEntity> outputs = getHiveContext().getOutputs();

        boolean ret = CollectionUtils.isEmpty(inputs) && CollectionUtils.isEmpty(outputs);

        if (!ret && getContext().getHiveOperation() == HiveOperation.QUERY && outputs.size() == 1) {
            // Select query has only one output
            WriteEntity output = outputs.iterator().next();
            if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR) {
                boolean temp = output.getWriteType() == WriteEntity.WriteType.PATH_WRITE && output.isTempURI();
                if (temp) {
                    ret = true;
                }
            }
        }
        return ret;
    }
}
