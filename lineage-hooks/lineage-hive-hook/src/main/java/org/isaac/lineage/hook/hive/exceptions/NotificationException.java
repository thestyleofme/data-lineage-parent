package org.isaac.lineage.hook.hive.exceptions;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:56
 * @since 1.0.0
 */
public class NotificationException extends RuntimeException {

    private static final long serialVersionUID = -3342248364615189630L;

    private final List<String> failedMessages;

    public NotificationException(Exception e) {
        super(e);
        failedMessages = Collections.emptyList();
    }

    public NotificationException(Exception e, List<String> failedMessages) {
        super(e);
        this.failedMessages = failedMessages;
    }

    public List<String> getFailedMessages() {
        return failedMessages;
    }
}
