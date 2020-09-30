package org.isaac.lineage.neo4j.contants;

/**
 * <p>
 * Constant class
 * </p>
 *
 * @author JupiterMouse 2020/09/27
 * @since 1.0
 */
public class NeoConstant {

    private NeoConstant() {
    }

    public static final String ENTITY_EXTRA_PREFIX = "ext";
    public static final String PK_FORMAT = "ext";

    public static class RelationShip {
        private RelationShip() {
        }

        public static final String REL_CLUSTER_FROM_PLATFORM = "CLUSTER_FROM_PLATFORM";
        public static final String REL_SCHEMA_FROM_CLUSTER = "SCHEMA_FROM_CLUSTER";
        public static final String REL_TABLE_FROM_SCHEMA = "TABLE_FROM_SCHEMA";
        public static final String REL_CREATE_TABLE_AS_SELECT = "CREATE_TABLE_AS_SELECT";
        public static final String REL_INSERT_OVERWRITE_TABLE_SELECT = "INSERT_OVERWRITE_TABLE_SELECT";
        public static final String REL_FIELD_FROM_TABLE = "FIELD_FROM_TABLE";
    }

    public static class Properties {
        private Properties() {
        }

        public static final String ATTR = "attr";
    }

    public static class Type {
        private Type() {
        }

        public static final String NODE_PLATFORM = "Platform";
        public static final String NODE_CLUSTER = "Cluster";
        public static final String NODE_SCHEMA = "Schema";
        public static final String NODE_TABLE = "Table";
        public static final String NODE_FIELD = "Field";
        public static final String NODE_TAG = "Tag";
    }


}
