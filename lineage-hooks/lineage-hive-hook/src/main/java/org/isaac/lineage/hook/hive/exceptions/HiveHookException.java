package org.isaac.lineage.hook.hive.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:56
 * @since 1.0.0
 */
public class HiveHookException extends RuntimeException {

    private static final long serialVersionUID = 4209504057102032847L;

    public HiveHookException() {
    }

    public HiveHookException(String message) {
        super(message);
    }

    public HiveHookException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveHookException(Throwable cause) {
        super(cause);
    }

    public HiveHookException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
