package org.isaac.hive.hook.notification;

import org.apache.commons.configuration2.Configuration;
import org.isaac.hive.hook.ApplicationProperties;

/**
 * <p>
 * To create notification instance
 * </p>
 *
 * @author isaac 2020/9/7 16:13
 * @since 1.0.0
 */
public class NotificationProvider {

    private static KafkaNotification kafkaNotification;

    public static KafkaNotification get() {
        if (kafkaNotification == null) {
            Configuration applicationProperties = ApplicationProperties.get();
            kafkaNotification = new KafkaNotification(applicationProperties);
        }
        return kafkaNotification;
    }
    
}
