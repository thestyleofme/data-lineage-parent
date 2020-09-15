package org.isaac.lineage.neo4j.exceptions;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 15:33
 * @since 1.0.0
 */
public class LineageException extends RuntimeException {

    private static final long serialVersionUID = 3283437446607525448L;

    public LineageException(String message) {
        super(message);
    }

    public LineageException(String message, Throwable cause) {
        super(message, cause);
    }

    public LineageException(Throwable cause) {
        super(cause);
    }
}
