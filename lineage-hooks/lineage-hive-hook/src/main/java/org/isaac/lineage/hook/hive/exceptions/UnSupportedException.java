package org.isaac.lineage.hook.hive.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:57
 * @since 1.0.0
 */
public class UnSupportedException extends RuntimeException {

    private static final long serialVersionUID = -6917876792327031646L;

    public UnSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportedException(String message) {
        super(message);
    }

    public UnSupportedException(Throwable cause) {
        super(cause);
    }
}
