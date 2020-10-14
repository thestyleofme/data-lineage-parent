package org.isaac.lineage.neo4j.kafka.handler.hive.event;

import java.util.ArrayList;

import org.isaac.lineage.neo4j.domain.LineageMapping;
import org.isaac.lineage.neo4j.domain.node.FieldNode;
import org.isaac.lineage.neo4j.domain.node.SchemaNode;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/10/14 11:32
 * @since 1.0.0
 */
public class HiveTableProcessUtil {

    private HiveTableProcessUtil(){
        throw new IllegalStateException();
    }

    public static void genSchemaNode(LineageMapping lineageMapping,
                                      HiveTableProcessEvent hiveTableProcessEvent) {
        ArrayList<SchemaNode> list = new ArrayList<>();
        hiveTableProcessEvent.getInputs().forEach(inputsDTO ->
                list.add(SchemaNode.builder().schemaName(inputsDTO.getDb()).build())
        );
        hiveTableProcessEvent.getOutputs().forEach(outputsDTO ->
                list.add(SchemaNode.builder().schemaName(outputsDTO.getDb()).build())
        );
        lineageMapping.getSchemaNodeList().addAll(list);
    }

    public static void genFieldNode(LineageMapping lineageMapping,
                                     HiveTableProcessEvent hiveTableProcessEvent) {
        ArrayList<FieldNode> list = new ArrayList<>();
        hiveTableProcessEvent.getOutputs().forEach(outputsDTO ->
                outputsDTO.getColumns().forEach(columnsDTO -> {
                    FieldNode fieldNode = genColumn(columnsDTO);
                    fieldNode.setSchemaName(outputsDTO.getDb());
                    fieldNode.setTableName(outputsDTO.getName());
                    list.add(fieldNode);
                }));
        hiveTableProcessEvent.getInputs().forEach(inputsDTO ->
                inputsDTO.getColumns().forEach(columnsDTO -> {
                    FieldNode fieldNode = genColumn(columnsDTO);
                    fieldNode.setSchemaName(inputsDTO.getDb());
                    fieldNode.setTableName(inputsDTO.getName());
                    list.add(fieldNode);
                }));
        lineageMapping.getFieldNodeList().addAll(list);
    }

    public static FieldNode genColumn(BaseAttribute.ColumnsDTO columnsDTO) {
        return FieldNode.builder()
                .fieldName(columnsDTO.getName())
                .fieldType(columnsDTO.getType())
                .build();
    }
}
