package org.isaac.hive.hook.notification;

import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration2.Configuration;
import org.isaac.hive.hook.exceptions.NotificationException;

/**
 * <p>
 * Abstract notification interface implementation
 * </p>
 *
 * @author isaac 2020/9/7 16:14
 * @since 1.0.0
 */
public abstract class AbstractNotification implements NotificationInterface {

    /**
     * each char can encode upto 4 bytes in UTF-8
     */
    public static final int MAX_BYTES_PER_CHAR = 4;

    public AbstractNotification(Configuration applicationProperties) {
    }

    @VisibleForTesting
    protected AbstractNotification() {
    }

    @Override
    public void send(List<String> messages) {
        sendInternal(messages);
    }

    @Override
    public void send(String... messages) {
        send(Arrays.asList(messages));
    }

    /**
     * Send the given messages.
     *
     * @param messages the array of messages to send
     * @throws NotificationException if an error occurs while sending
     */
    protected abstract void sendInternal(List<String> messages);
}
