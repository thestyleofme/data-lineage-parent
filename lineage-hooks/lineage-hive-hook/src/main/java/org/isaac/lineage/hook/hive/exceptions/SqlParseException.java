package org.isaac.lineage.hook.hive.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:57
 * @since 1.0.0
 */
public class SqlParseException extends RuntimeException {

    private static final long serialVersionUID = 7580524884309975467L;

    public SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlParseException(String message) {
        super(message);
    }

    public SqlParseException(Throwable cause) {
        super(cause);
    }
}
