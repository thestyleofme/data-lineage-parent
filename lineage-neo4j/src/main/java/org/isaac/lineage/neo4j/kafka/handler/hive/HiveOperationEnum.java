package org.isaac.lineage.neo4j.kafka.handler.hive;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/09/23 11:43
 * @since 1.0.0
 */
public enum HiveOperationEnum {

    /**
     * CreateTable event
     */
    CREATETABLE("CREATETABLE"),
    CREATETABLE_AS_SELECT("CREATETABLE_AS_SELECT"),
    DROPTABLE("DROPTABLE"),
    QUERY("QUERY")
    ;

    private final String name;

    HiveOperationEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
