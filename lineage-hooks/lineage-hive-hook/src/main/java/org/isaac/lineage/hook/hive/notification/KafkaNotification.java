package org.isaac.lineage.hook.hive.notification;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import javax.inject.Inject;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.isaac.lineage.hook.hive.exceptions.NotificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Kafka specific access point to the notification framework
 * </p>
 *
 * @author isaac 2020/9/7 16:12
 * @since 1.0.0
 */
public class KafkaNotification extends AbstractNotification {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaNotification.class);

    public static final String HOOK_TOPIC = "TOPIC_METADATA_LINEAGE";
    private final Properties properties;
    private KafkaProducer<String, String> producer;

    /**
     * Construct a KafkaNotification.
     *
     * @param applicationProperties the application properties used to configure Kafka
     */
    @Inject
    public KafkaNotification(Configuration applicationProperties) {
        super(applicationProperties);

        properties = ConfigurationConverter.getProperties(applicationProperties);

        // Override default configs
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("enable.auto.commit", applicationProperties.getBoolean("enable.auto.commit", false));
        properties.put("session.timeout.ms", applicationProperties.getString("session.timeout.ms", "30000"));
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }

    @Override
    public void sendInternal(List<String> messages) {
        if (producer == null) {
            createProducer();
        }
        sendInternalToProducer(producer, messages);
    }

    void sendInternalToProducer(Producer<String, String> p, List<String> messages) {
        List<MessageContext> messageContexts = new ArrayList<>();

        for (String message : messages) {
            if (StringUtils.isNotBlank(message)) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(HOOK_TOPIC, message);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sending message for topic {}: {}", HOOK_TOPIC, message);
                }

                Future<RecordMetadata> future = p.send(record);

                messageContexts.add(new MessageContext(future, message));
            }
        }

        List<String> failedMessages = new ArrayList<>();
        Exception lastFailureException = null;

        for (MessageContext context : messageContexts) {
            try {
                RecordMetadata response = context.getFuture().get();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sent message for topic - {}, partition - {}, offset - {}",
                            response.topic(), response.partition(), response.offset());
                }
            } catch (Exception e) {
                lastFailureException = e;

                failedMessages.add(context.getMessage());
            }
        }

        if (lastFailureException != null) {
            throw new NotificationException(lastFailureException, failedMessages);
        }
    }

    private synchronized void createProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(properties);
        }
    }

    private static class MessageContext {
        private final Future<RecordMetadata> future;
        private final String message;

        public MessageContext(Future<RecordMetadata> future, String message) {
            this.future = future;
            this.message = message;
        }

        public Future<RecordMetadata> getFuture() {
            return future;
        }

        public String getMessage() {
            return message;
        }
    }
}
