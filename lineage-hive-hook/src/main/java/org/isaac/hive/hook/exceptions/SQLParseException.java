package org.isaac.hive.hook.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:57
 * @since 1.0.0
 */
public class SQLParseException extends RuntimeException {

    private static final long serialVersionUID = 7580524884309975467L;

    public SQLParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLParseException(String message) {
        super(message);
    }

    public SQLParseException(Throwable cause) {
        super(cause);
    }
}
