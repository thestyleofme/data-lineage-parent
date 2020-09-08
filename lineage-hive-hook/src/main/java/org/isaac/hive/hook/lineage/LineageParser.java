package org.isaac.hive.hook.lineage;

import java.util.*;
import java.util.Map.Entry;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.isaac.hive.hook.HiveHookContext;
import org.isaac.hive.hook.exceptions.SqlParseException;
import org.isaac.hive.hook.exceptions.UnSupportedException;
import org.isaac.hive.hook.lineage.bean.ColumnBlock;
import org.isaac.hive.hook.lineage.bean.ColumnLineage;
import org.isaac.hive.hook.lineage.bean.QueryTree;
import org.isaac.hive.hook.utils.ListCloneUtil;

/**
 * <p>
 * HiveQL parser, obtain input table, output table and column lineage information
 * </p>
 *
 * @author isaac 2020/9/7 16:59
 * @since 1.0.0
 */
public class LineageParser {

    private static final String SPLIT_DOT = ".";
    private static final String SPLIT_COMMA = ",";
    private static final String SPLIT_AND = "&";
    private static final String CONDITION_WHERE = "WHERE:";
    private static final String TOK_TMP_FILE = "TOK_TMP_FILE";

    /**
     * The table columns map,
     * Key is table name
     * Value is column list of the table
     */
    private final Map<String, List<String>> tableColumnsMap = new HashMap<>();

    /**
     * Sub query relationship tree storage
     */
    private final List<QueryTree> queryTreeList = new ArrayList<>();

    /**
     * query tree map
     */
    private final Map<String, QueryTree> queryTreeMap = new HashMap<>();

    /**
     * stack of column generated condition
     */
    private final Stack<Set<String>> conditionsStack = new Stack<>();

    /**
     * stack of column
     */
    private final Stack<List<ColumnLineage>> columnsStack = new Stack<>();

    /**
     * Cache list of 'Where' or 'join'
     */
    private final Set<String> conditions = new HashSet<>();

    /**
     * Cache of sub query column lineage
     */
    private final List<ColumnLineage> cols = new ArrayList<>();

    /**
     * table name stack list
     */
    private final Stack<String> tableNameStack = new Stack<>();

    /**
     * join node
     */
    private final Stack<Boolean> joinStack = new Stack<>();
    private final Stack<ASTNode> joinClauseStack = new Stack<>();

    /**
     * whether has join clause
     */
    private boolean isJoinClause = false;
    private ASTNode joinClause = null;

    /**
     * Hive default database name
     */
    private String currentDB = "default";

    private boolean isCreateTable = false;

    /**
     * Result list
     */
    private final Set<String> outputTables;
    private final Set<String> inputTables;
    private final List<ColumnLineage> columnLineages;

    private final HiveHookContext context;

    /**
     * Constructor
     *
     * @param context HiveHookContext
     */
    public LineageParser(HiveHookContext context) {
        this.context = context;
        this.inputTables = new HashSet<>();
        this.outputTables = new HashSet<>();
        this.columnLineages = new ArrayList<>();
    }

    public Set<String> getOutputTables() {
        return outputTables;
    }

    public Set<String> getInputTables() {
        return inputTables;
    }

    public List<ColumnLineage> getColumnLineages() {
        return columnLineages;
    }

    /**
     * prepare parse current node
     *
     * @param ast ASTNode
     */
    private void preParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                // hive SQL: switch database ...
                case HiveParser.TOK_SWITCHDATABASE:
                    currentDB = ast.getChild(0).getText();
                    break;
                case HiveParser.TOK_TRANSFORM:
                    throw new UnSupportedException("Not support transform clause");
                    // table join clause
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                    // case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN:
                    joinStack.push(isJoinClause);
                    isJoinClause = true;
                    joinClauseStack.push(joinClause);
                    joinClause = ast;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * parse all child ASTNode
     *
     * @param ast ASTNode
     */
    private void parseChildNodes(ASTNode ast) {
        int count = ast.getChildCount();
        if (count > 0) {
            for (int index = 0; index < count; index++) {
                ASTNode child = (ASTNode) ast.getChild(index);
                iterationNode(child);
            }
        }
    }

    /**
     * parse ASTNode
     *
     * @param ast ASTNode
     */
    private void parseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_CREATETABLE: {
                    // create table ... (output table)
                    isCreateTable = true;
                    String outputTable = getQualityName(getUnescapedName((ASTNode) ast.getChild(0)));
                    outputTables.add(outputTable);
                    break;
                }
                case HiveParser.TOK_TAB: {
                    // get output table
                    String outputTable = getQualityName(getUnescapedName((ASTNode) ast.getChild(0)));
                    outputTables.add(outputTable);
                    break;
                }
                case HiveParser.TOK_TABREF:
                    // from table ... (input table)
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableNames = tabTree.getChildCount() == 1
                            ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) // table
                            : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) // database
                            + SPLIT_DOT
                            + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(1)); // table
                    // get full table name
                    String tableQualityName = getQualityName(tableNames);
                    // add to input table list
                    inputTables.add(tableQualityName);
                    // clear query map
                    queryTreeMap.clear();
                    // Get table alias name
                    // from table_name as table_alias
                    if (ast.getChild(1) != null) {
                        String tableAlias = ast.getChild(1).getText().toLowerCase();
                        // save query tree
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(tableAlias);
                        qt.addTableSet(tableQualityName);
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        queryTreeList.add(qt);
                        // TOK_SUBQUERY join TOK_TABREF
                        if (isJoinClause && ast.getParent() == joinClause) {
                            for (QueryTree entry : queryTreeList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryTreeMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {
                            queryTreeMap.put(qt.getCurrent(), qt);
                        }
                    } else {
                        String tableName = getTableName(tableQualityName);
                        String tableAlias = tableName.toLowerCase();
                        // save query tree
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(tableAlias);
                        qt.addTableSet(tableQualityName);
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        queryTreeList.add(qt);
                        if (isJoinClause && ast.getParent() == joinClause) {
                            for (QueryTree entry : queryTreeList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryTreeMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {
                            queryTreeMap.put(qt.getCurrent(), qt);
                            // check query schema likes select app.t1.c1,t1.c1 from t1
                            queryTreeMap.put(tableQualityName.toLowerCase(), qt);
                        }
                    }
                    break;
                case HiveParser.TOK_SUBQUERY:
                    // parse sub query, likes “from ( select * from tbl1 ) as a
                    if (ast.getChildCount() == 2) {
                        // get name 'a'
                        String tableAlias = getUnescapeIdentifier(ast.getChild(1).getText());
                        // save query tree
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(tableAlias.toLowerCase());
                        // get column lineage
                        qt.setColLineageList(generateColLineageList(cols, conditions));
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setId(generateTreeId(ast));
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        qt.setChildList(getSubQueryChildren(qt.getId()));
                        if (CollectionUtils.isNotEmpty(qt.getChildList())) {
                            for (QueryTree cqt : qt.getChildList()) {
                                qt.getTableSet().addAll(cqt.getTableSet());
                                // remove sub node
                                queryTreeList.remove(cqt);
                            }
                        }
                        queryTreeList.add(qt);
                        cols.clear();

                        queryTreeMap.clear();
                        for (QueryTree _qt : queryTreeList) {
                            // cache next sub query
                            if (qt.getParent().equals(_qt.getParent())) {
                                queryTreeMap.put(_qt.getCurrent(), _qt);
                            }
                        }
                    }
                    break;
                case HiveParser.TOK_SELEXPR: // get input/output column
                    // get insert semantic
                    Tree tokInsert = ast.getParent().getParent();
                    // get insert table
                    Tree child = tokInsert.getChild(0).getChild(0);
                    // insert table name
                    String tempName = getUnescapedName((ASTNode) child.getChild(0));
                    String destTable = TOK_TMP_FILE.equals(tempName) ? TOK_TMP_FILE : getQualityName(tempName);
                    // check column '*' likes 'select * from tbl1'
                    if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                        String tableOrAlias = "";
                        if (ast.getChild(0).getChild(0) != null) {
                            tableOrAlias = ast.getChild(0).getChild(0).getChild(0).getText();
                        }
                        // get table and alias
                        // likes "select * from (select c1, c2 from db1.tbl1) as a1"
                        // result = ['db1.tbl1', 'a1']
                        String[] result = getTableAndAlias(tableOrAlias);
                        String alias = result[1];

                        // parse nest sql likes select *
                        boolean isSub = false;
                        if (StringUtils.isNotBlank(alias)) {
                            for (String s : alias.split(SPLIT_AND)) {
                                QueryTree qt = queryTreeMap.get(s.toLowerCase());
                                if (null != qt) {
                                    List<ColumnLineage> colLineList = qt.getColLineageList();
                                    if (CollectionUtils.isNotEmpty(colLineList)) {
                                        isSub = true;
                                        cols.addAll(colLineList);
                                    }
                                }
                            }
                        }
                        // fill '*' of column
                        if (!isSub) {
                            String nowTable = result[0];
                            String[] tableArr = nowTable.split(SPLIT_AND); // fact.test&test2
                            for (String tables : tableArr) {
                                String[] split = tables.split("\\.");
                                if (split.length > 2) {
                                    throw new SqlParseException("parse table:" + nowTable);
                                }
                                List<String> colByTab = context.getColumnByTableName(tables);
                                for (String column : colByTab) {
                                    Set<String> fromNameSet = new LinkedHashSet<>();
                                    String colQualityName = tables + SPLIT_DOT + column;
                                    fromNameSet.add(colQualityName);
                                    ColumnLineage cl = new ColumnLineage(
                                            column,
                                            Collections.singletonList(colQualityName),
                                            fromNameSet,
                                            new LinkedHashSet<>(),
                                            destTable,
                                            column);
                                    cols.add(cl);
                                }
                            }
                        }
                    } else {
                        ColumnBlock cb = getBlockIteral((ASTNode) ast.getChild(0));
                        String toNameParse = getToNameParse(ast, cb);
                        Set<String> fromNameSet = filterData(cb.getColumnSet());
                        ColumnLineage cl = new ColumnLineage(
                                toNameParse,
                                Collections.singletonList(cb.getCondition()),
                                fromNameSet,
                                new LinkedHashSet<>(),
                                destTable,
                                "");
                        cols.add(cl);
                    }
                    break;
                case HiveParser.TOK_WHERE:
                    // parse where clause
                    conditions.add(CONDITION_WHERE + getBlockIteral((ASTNode) ast.getChild(0)).getCondition());
                    break;
                default:
                    // parse join on clause
                    if (joinClause != null && joinClause.getTokenStartIndex() == ast.getTokenStartIndex()
                            && joinClause.getTokenStopIndex() == ast.getTokenStopIndex()) {
                        ASTNode astCon = (ASTNode) ast.getChild(2);
                        conditions.add(ast.getText().substring(4) + ":" + getBlockIteral(astCon).getCondition());
                        break;
                    }
            }
        }
    }

    /**
     * parsed current node
     *
     * @param ast ASTNode
     */
    private void endParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            Tree parent = ast.getParent();
            // jump out from join clause
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                    // case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN:
                    isJoinClause = joinStack.pop();
                    joinClause = joinClauseStack.pop();
                    break;

                case HiveParser.TOK_QUERY:
                    // sub node of union
                    processUnionStack(ast, parent);
                    break;
                case HiveParser.TOK_INSERT:
                case HiveParser.TOK_SELECT:
                    break;
                // case HiveParser.TOK_UNION:
                case HiveParser.TOK_UNIONALL:
                    // merge union column info
                    mergeUnionCols();
                    // sub node of union
                    processUnionStack(ast, parent);
                    break;
            }
        }
    }

    private String getUnescapedName(ASTNode node) {
        return BaseSemanticAnalyzer.getUnescapedName(node);
    }

    /**
     * Remove the encapsulating "`" pair from the identifier.
     * We allow users touse "`" to escape identifier for table names,
     * column names and aliases, incase that coincide with Hive language keywords.
     *
     * @param value String
     * @return String
     */
    private String getUnescapeIdentifier(String value) {
        return BaseSemanticAnalyzer.unescapeIdentifier(value);
    }

    /**
     * Get parent tree of sub query
     *
     * @param ast Tree
     */
    private QueryTree getSubQueryParent(Tree ast) {
        Tree temp = ast;
        QueryTree qt = new QueryTree();
        while (!(temp = temp.getParent()).isNil()) {
            if (temp.getType() == HiveParser.TOK_SUBQUERY) {
                qt.setpId(generateTreeId(temp));
                qt.setParent(BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1)));
                return qt;
            }
        }
        qt.setpId(-1);
        qt.setParent("NIL");
        return qt;
    }

    private int generateTreeId(Tree tree) {
        return tree.getTokenStartIndex() + tree.getTokenStopIndex();
    }

    /**
     * Get query child
     *
     * @param id int
     */
    private List<QueryTree> getSubQueryChildren(int id) {
        List<QueryTree> list = new ArrayList<>();
        for (QueryTree qt : queryTreeList) {
            if (id == qt.getpId()) {
                list.add(qt);
            }
        }
        return list;
    }

    /**
     * Get qualified name of column
     *
     * @param ast ASTNode
     * @param bk  ColumnBlock
     * @return String
     */
    private String getToNameParse(ASTNode ast, ColumnBlock bk) {
        String alia = "";
        Tree child = ast.getChild(0);
        if (ast.getChild(1) != null) {
            // has alias likes ip as alias
            alia = ast.getChild(1).getText();
        } else if (child.getType() == HiveParser.DOT
                // no alias likes a.ip
                && child.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL && child.getChild(0).getChildCount() == 1
                && child.getChild(1).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(1).getText());
        } else if (child.getType() == HiveParser.TOK_TABLE_OR_COL // no alias
                && child.getChildCount() == 1 && child.getChild(0).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
        }
        return alia;
    }

    /**
     * Fetch block apply for WHERE, JOIN and SELECT
     * <p>
     * where a=1
     * <p>
     * t1 join t2 on t1.col1=t2.col1 and t1.col2=123
     * <p>
     * select count(distinct col1) from t1
     *
     * @param ast ASTNode
     * @return ColumnBlock
     */
    private ColumnBlock getBlockIteral(ASTNode ast) {
        if (ast.getType() == HiveParser.KW_OR || ast.getType() == HiveParser.KW_AND) {
            ColumnBlock bk1 = getBlockIteral((ASTNode) ast.getChild(0));
            ColumnBlock bk2 = getBlockIteral((ASTNode) ast.getChild(1));
            bk1.getColumnSet().addAll(bk2.getColumnSet());
            bk1.setCondition("(" + bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition() + ")");
            return bk1;
        } else if (ast.getType() == HiveParser.NOTEQUAL // condition likes > < like in
                || ast.getType() == HiveParser.EQUAL || ast.getType() == HiveParser.LESSTHAN
                || ast.getType() == HiveParser.LESSTHANOREQUALTO || ast.getType() == HiveParser.GREATERTHAN
                || ast.getType() == HiveParser.GREATERTHANOREQUALTO || ast.getType() == HiveParser.KW_LIKE
                || ast.getType() == HiveParser.DIVIDE || ast.getType() == HiveParser.PLUS
                || ast.getType() == HiveParser.MINUS || ast.getType() == HiveParser.STAR
                || ast.getType() == HiveParser.MOD || ast.getType() == HiveParser.AMPERSAND
                || ast.getType() == HiveParser.TILDE || ast.getType() == HiveParser.BITWISEOR
                || ast.getType() == HiveParser.BITWISEXOR) {
            ColumnBlock bk1 = getBlockIteral((ASTNode) ast.getChild(0));
            if (ast.getChild(1) == null) { // -1
                bk1.setCondition(ast.getText() + bk1.getCondition());
            } else {
                ColumnBlock bk2 = getBlockIteral((ASTNode) ast.getChild(1));
                bk1.getColumnSet().addAll(bk2.getColumnSet());
                bk1.setCondition(bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition());
            }
            return bk1;
        } else if (ast.getType() == HiveParser.TOK_FUNCTIONDI) {
            ColumnBlock col = getBlockIteral((ASTNode) ast.getChild(1));
            String condition = ast.getChild(0).getText();
            col.setCondition(condition + "(distinct (" + col.getCondition() + "))");
            return col;
        } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
            String fun = ast.getChild(0).getText();
            ColumnBlock col = ast.getChild(1) == null ? new ColumnBlock() : getBlockIteral((ASTNode) ast.getChild(1));
            if ("when".equalsIgnoreCase(fun)) {
                col.setCondition(getWhenCondition(ast));
                Set<ColumnBlock> processChildren = processChildren(ast, 1);
                col.getColumnSet().addAll(bkToCols(col, processChildren));
                return col;
            } else if ("IN".equalsIgnoreCase(fun)) {
                col.setCondition(col.getCondition() + " in (" + blockCondToString(processChildren(ast, 2)) + ")");
                return col;
            } else if ("TOK_ISNOTNULL".equalsIgnoreCase(fun) // isnull isnotnull
                    || "TOK_ISNULL".equalsIgnoreCase(fun)) {
                col.setCondition(col.getCondition() + " " + fun.toLowerCase().substring(4));
                return col;
            } else if ("BETWEEN".equalsIgnoreCase(fun)) {
                col.setCondition(getBlockIteral((ASTNode) ast.getChild(2)).getCondition() + " between "
                        + getBlockIteral((ASTNode) ast.getChild(3)).getCondition() + " and "
                        + getBlockIteral((ASTNode) ast.getChild(4)).getCondition());
                return col;
            }
            Set<ColumnBlock> processChildren = processChildren(ast, 1);
            col.getColumnSet().addAll(bkToCols(col, processChildren));
            col.setCondition(fun + "(" + blockCondToString(processChildren) + ")");
            return col;
        } else if (ast.getType() == HiveParser.LSQUARE) { // map,array
            ColumnBlock column = getBlockIteral((ASTNode) ast.getChild(0));
            ColumnBlock key = getBlockIteral((ASTNode) ast.getChild(1));
            column.setCondition(column.getCondition() + "[" + key.getCondition() + "]");
            return column;
        } else {
            return parseBlock(ast);
        }
    }

    private Set<String> bkToCols(ColumnBlock col, Set<ColumnBlock> processChilds) {
        Set<String> set = new LinkedHashSet<>(processChilds.size());
        for (ColumnBlock colLine : processChilds) {
            if (CollectionUtils.isNotEmpty(colLine.getColumnSet())) {
                set.addAll(colLine.getColumnSet());
            }
        }
        return set;
    }

    private String blockCondToString(Set<ColumnBlock> processChilds) {
        StringBuilder sb = new StringBuilder();
        for (ColumnBlock colLine : processChilds) {
            sb.append(colLine.getCondition()).append(SPLIT_COMMA);
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * parse condition of when
     *
     * @param ast ASTNode
     * @return case when c1>100 then col1 when c1>0 col2 else col3 end
     */
    private String getWhenCondition(ASTNode ast) {
        int cnt = ast.getChildCount();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < cnt; i++) {
            String condition = getBlockIteral((ASTNode) ast.getChild(i)).getCondition();
            if (i == 1) {
                sb.append("(case when ").append(condition);
            } else if (i == cnt - 1) {
                // else
                sb.append(" else ").append(condition).append(" end)");
            } else if (i % 2 == 0) {
                // then
                sb.append(" then ").append(condition);
            } else {
                sb.append(" when ").append(condition);
            }
        }
        return sb.toString();
    }

    private List<ColumnLineage> generateColLineageList(List<ColumnLineage> cols, Set<String> conditions) {
        List<ColumnLineage> list = new ArrayList<>();
        for (ColumnLineage entry : cols) {
            entry.getConditionSet().addAll(conditions);
            list.add(ListCloneUtil.cloneColLine(entry));
        }
        return list;
    }

    /**
     * check column type, normal: "a as column, a", exception: "1, 'a'"
     */
    private boolean notNormalCol(String column) {
        return StringUtils.isBlank(column) || NumberUtils.isNumber(column)
                || (column.startsWith("\"") && column.endsWith("\""))
                || (column.startsWith("'") && column.endsWith("'"));
    }

    /**
     * parse sub tree with index
     *
     * @param ast        ASTNode
     * @param startIndex position of index
     * @return Set<ColumnBlock>
     */
    private Set<ColumnBlock> processChildren(ASTNode ast, int startIndex) {
        int cnt = ast.getChildCount();
        Set<ColumnBlock> set = new LinkedHashSet<>();
        for (int i = startIndex; i < cnt; i++) {
            ColumnBlock bk = getBlockIteral((ASTNode) ast.getChild(i));
            if (StringUtils.isNotBlank(bk.getCondition()) || CollectionUtils.isNotEmpty(bk.getColumnSet())) {
                set.add(bk);
            }
        }
        return set;
    }

    /**
     * fetch column name
     *
     * @param ast ASTNode
     * @return ColumnBlock
     */
    private ColumnBlock parseBlock(ASTNode ast) {
        if (ast.getType() == HiveParser.DOT && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1 && ast.getChild(1).getType() == HiveParser.Identifier) {
            String column = getUnescapeIdentifier(ast.getChild(1).getText());
            String alia = getUnescapeIdentifier(ast.getChild(0).getChild(0).getText());
            return getBlock(column, alia);
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            String column = ast.getChild(0).getText();
            return getBlock(column, null);
        } else if (ast.getType() == HiveParser.Number || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            ColumnBlock bk = new ColumnBlock();
            bk.setCondition(ast.getText());
            bk.getColumnSet().add(ast.getText());
            return bk;
        }
        return new ColumnBlock();
    }

    /**
     * get block by column name and alias
     *
     * @param column String
     * @param alias  String
     * @return ColumnBlock
     */
    private ColumnBlock getBlock(String column, String alias) {
        String[] result = getTableAndAlias(alias);
        String tableArray = result[0];
        String aliaTemp = result[1];

        for (String string : aliaTemp.split(SPLIT_AND)) {
            QueryTree qt = queryTreeMap.get(string.toLowerCase());
            if (StringUtils.isNotBlank(column)) {
                for (ColumnLineage colLine : qt.getColLineageList()) {
                    if (column.equalsIgnoreCase(colLine.getToNameParse())) {
                        ColumnBlock bk = new ColumnBlock();
                        bk.setCondition(StringUtils.join(colLine.getColConditions(), "@&@"));
                        bk.setColumnSet(ListCloneUtil.cloneSet(colLine.getFromNameSet()));
                        return bk;
                    }
                }
            }
        }

        String realTableTemp = tableArray;
        // there has ambiguity sql if column matches more from with metadata
        int cnt = 0;
        for (String tables : tableArray.split(SPLIT_AND)) {
            String[] split = tables.split("\\.");
            if (split.length > 2) {
                throw new SqlParseException("parse table:" + tables);
            }
            List<String> colByTab = context.getColumnByTableName(tables);
            for (String col : colByTab) {
                if (column.equalsIgnoreCase(col)) {
                    realTableTemp = tables;
                    cnt++;
                }
            }
        }

        // check ambiguity sql
        if (cnt > 1) {
            throw new SqlParseException("SQL is ambiguity, column: " + column + " tables:" + tableArray);
        }

        ColumnBlock bk = new ColumnBlock();
        bk.setCondition(realTableTemp + SPLIT_DOT + column);
        bk.getColumnSet().add(realTableTemp + SPLIT_DOT + column);
        return bk;
    }

    /**
     * filter unused column, likes col1,123,'2013',col2 ==>> col1,col2
     *
     * @param colSet Set<String>
     * @return Set<String>
     */
    private Set<String> filterData(Set<String> colSet) {
        Set<String> set = new LinkedHashSet<>();
        for (String string : colSet) {
            if (!notNormalCol(string)) {
                set.add(string);
            }
        }
        return set;
    }

    private void mergeUnionCols() {
        validateUnion(cols);
        int size = cols.size();
        int colNum = size / 2;
        List<ColumnLineage> list = new ArrayList<>(colNum);
        // merge
        for (int i = 0; i < colNum; i++) {
            ColumnLineage col = cols.get(i);
            for (int j = i + colNum; j < size; j = j + colNum) {
                ColumnLineage col2 = cols.get(j);
                list.add(col2);
                if (notNormalCol(col.getToNameParse()) && !notNormalCol(col2.getToNameParse())) {
                    col.setToNameParse(col2.getToNameParse());
                }
                col.getFromNameSet().addAll(col2.getFromNameSet());

                col.addColCondition(col2.getColConditions());

                Set<String> conditionSet = ListCloneUtil.cloneSet(col.getConditionSet());
                conditionSet.addAll(col2.getConditionSet());
                conditionSet.addAll(conditions);
                col.getConditionSet().addAll(conditionSet);
            }
        }
        // remove that already merged col
        cols.removeAll(list);
    }

    private void processUnionStack(ASTNode ast, Tree parent) {
        boolean isNeedAdd = parent.getType() == HiveParser.TOK_UNIONALL;
        if (isNeedAdd) {
            if (parent.getChild(0) == ast && parent.getChild(1) != null) {
                // pop down
                conditionsStack.push(ListCloneUtil.cloneSet(conditions));
                conditions.clear();
                columnsStack.push(ListCloneUtil.cloneList(cols));
                cols.clear();
            } else {
                // pop up
                if (!conditionsStack.isEmpty()) {
                    conditions.addAll(conditionsStack.pop());
                }
                if (!columnsStack.isEmpty()) {
                    cols.addAll(0, columnsStack.pop());
                }
            }
        }
    }

    /**
     * Start parse ASTNode
     *
     * @param ast ASTNode
     */
    private void iterationNode(ASTNode ast) {
        // preParse current node
        preParseCurrentNode(ast);
        // parse next child node
        parseChildNodes(ast);
        // parse current node
        parseCurrentNode(ast);
        // end parse
        endParseCurrentNode(ast);
    }

    /**
     * Get lineage information from input sql
     *
     * @param sql sql
     * @throws Exception Exception
     */
    public void getLineageInfo(String sql) throws Exception {
        if (StringUtils.isNotBlank(sql)) {
            String trim = sql.toLowerCase().trim();
            if (isNotSql(trim)) {
                return;
            }
            ParseDriver pd = new ParseDriver();
            ASTNode ast = pd.parse(sql);
            // initialize
            initialize();
            // start parse
            iterationNode(ast);
            // fetch result
            fetchResult();
        }
    }

    private void initialize() {
        isCreateTable = false;
        tableColumnsMap.clear();

        columnLineages.clear();
        outputTables.clear();
        inputTables.clear();

        queryTreeMap.clear();
        queryTreeList.clear();

        conditionsStack.clear();
        columnsStack.clear();

        conditions.clear();
        cols.clear();

        tableNameStack.clear();
        joinStack.clear();
        joinClauseStack.clear();

        isJoinClause = false;
        joinClause = null;
    }

    /**
     * SQL format check
     *
     * @param sql sql
     * @return boolean
     */
    private boolean isNotSql(String sql) {
        boolean ret = false;
        if (sql.startsWith("set") || sql.startsWith("add")) {
            ret = true;
        }
        return ret;
    }

    /**
     * end parse
     */
    private void fetchResult() {
        fetchColumnsByTableMetadata();
        fetchColomnLineageList();
    }

    /**
     * fetch output table column lineage
     */
    private void fetchColomnLineageList() {
        Map<String, List<ColumnLineage>> map = new HashMap<>();
        List<ColumnLineage> value = generateColLineageList(cols, conditions);
        for (ColumnLineage colLine : value) {
            List<ColumnLineage> list = map.get(colLine.getToTable());
            if (CollectionUtils.isEmpty(list)) {
                list = new ArrayList<>();
                map.put(colLine.getToTable(), list);
            }
            list.add(colLine);
        }

        for (Entry<String, List<ColumnLineage>> entry : map.entrySet()) {
            String table = entry.getKey();
            List<ColumnLineage> pList = entry.getValue();
            List<String> dList = tableColumnsMap.get(table);
            int metaSize = CollectionUtils.isEmpty(dList) ? 0 : dList.size();
            // insert into column
            for (int i = 0; i < pList.size(); i++) {
                ColumnLineage clp = pList.get(i);
                String colName = null;
                if (i < metaSize) {
                    colName = table + SPLIT_DOT + dList.get(i);
                }
                if (isCreateTable && TOK_TMP_FILE.equals(table)) {
                    for (String string : outputTables) {
                        table = string;
                    }
                }
                ColumnLineage colLine = new ColumnLineage(
                        clp.getToNameParse(),
                        clp.getColConditions(),
                        clp.getFromNameSet(),
                        clp.getConditionSet(),
                        table,
                        colName);
                columnLineages.add(colLine);
            }
        }
    }

    private void fetchColumnsByTableMetadata() {
        for (String table : outputTables) {
            List<String> list = context.getColumnByTableName(table);
            tableColumnsMap.put(table, list);
        }
    }

    /**
     * Complete database info likes
     * table1 ==>> db1.table1 db1.table1 ==>> db1.table1 db2.t1&t2 ==>> db2.t1&db1.t2
     *
     * @param tableNames
     */
    private String getQualityName(String tableNames) {
        if (StringUtils.isEmpty(tableNames)) {
            return tableNames;
        }
        StringBuilder sb = new StringBuilder();
        // default.tbl1&tbl2&tbl3
        String[] tableArr = tableNames.split(SPLIT_AND);
        for (String tables : tableArr) {
            String[] split = tables.split("\\" + SPLIT_DOT);
            if (split.length > 2) {
                throw new SqlParseException("parse table:" + tableNames);
            }
            if (split.length == 2) {
                sb.append(split[0]).append(SPLIT_DOT).append(split[1]).append(SPLIT_AND);
            } else {
                sb.append(currentDB).append(SPLIT_DOT).append(split[0]).append(SPLIT_AND);
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private String getTableName(String qualityName) {
        if (StringUtils.isEmpty(qualityName)) {
            return qualityName;
        }
        String[] split = qualityName.split("\\" + SPLIT_DOT);
        if (split.length == 2) {
            return split[1];
        } else {
            return split[0];
        }
    }

    /**
     * Get table name by alias
     *
     * @param alias String
     * @return String[]
     */
    private String[] getTableAndAlias(String alias) {
        String aliasTemp = StringUtils.isNotBlank(alias) ? alias : StringUtils.join(queryTreeMap.keySet(), SPLIT_AND);
        String[] result = {"", aliasTemp};
        Set<String> tableSet = new HashSet<>();
        if (StringUtils.isNotBlank(aliasTemp)) {
            String[] split = aliasTemp.split(SPLIT_AND);
            for (String s : split) {
                // alias type likes select a.col,table_name.col from table_name a
                if (inputTables.contains(s) || inputTables.contains(getQualityName(s))) {
                    tableSet.add(getQualityName(s));
                } else if (queryTreeMap.containsKey(s.toLowerCase())) {
                    tableSet.addAll(queryTreeMap.get(s.toLowerCase()).getTableSet());
                }
            }
            result[0] = StringUtils.join(tableSet, SPLIT_AND);
            result[1] = aliasTemp;
        }
        return result;
    }

    /**
     * validate union
     *
     * @param list List<ColumnLineage>
     */
    private void validateUnion(List<ColumnLineage> list) {
        int size = list.size();
        if (size % 2 == 1) {
            throw new SqlParseException("union column number are different, size=" + size);
        }
        int colNum = size / 2;
        checkUnion(list, 0, colNum);
        checkUnion(list, colNum, size);
    }

    /**
     * Check union column type and number
     *
     * @param list  List<ColumnLineage>
     * @param start start
     * @param end   end
     */
    private void checkUnion(List<ColumnLineage> list, int start, int end) {
        String tmp = null;
        for (int i = start; i < end; i++) { // merge column
            ColumnLineage col = list.get(i);
            if (StringUtils.isBlank(tmp)) {
                tmp = col.getToTable();
            } else if (!tmp.equals(col.getToTable())) {
                throw new SqlParseException(
                        "union column number/types are different,table1=" + tmp + ",table2=" + col.getToTable());
            }
        }
    }

}
