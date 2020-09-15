package org.isaac.lineage.hook.hive.notification;

import java.util.List;

import org.isaac.lineage.hook.hive.exceptions.NotificationException;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 16:13
 * @since 1.0.0
 */
public interface NotificationInterface {

    /**
     * Send the given messages.
     *
     * @param messages the messages to send
     * @throws NotificationException if an error occurs while sending
     */
    void send(String... messages);

    /**
     * Send the given messages.
     *
     * @param messages the list of messages to send
     * @throws NotificationException if an error occurs while sending
     */
    void send(List<String> messages);

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();
}
