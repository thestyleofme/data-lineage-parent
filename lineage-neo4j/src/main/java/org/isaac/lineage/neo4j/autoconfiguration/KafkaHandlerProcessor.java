package org.isaac.lineage.neo4j.autoconfiguration;

import org.isaac.lineage.neo4j.annotation.SourceType;
import org.isaac.lineage.neo4j.context.KafkaHandlerContext;
import org.isaac.lineage.neo4j.kafka.handler.BaseKafkaHandler;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 15:23
 * @since 1.0.0
 */
@Component
public class KafkaHandlerProcessor implements BeanPostProcessor {

    private final KafkaHandlerContext kafkaHandlerContext;

    public KafkaHandlerProcessor(KafkaHandlerContext kafkaHandlerContext) {
        this.kafkaHandlerContext = kafkaHandlerContext;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) {
        Class<?> clazz = bean.getClass();
        SourceType sourceType = clazz.getAnnotation(SourceType.class);
        if (sourceType == null || !(bean instanceof BaseKafkaHandler)) {
            return bean;
        }
        String value = sourceType.value();
        kafkaHandlerContext.register(value.toUpperCase(), (BaseKafkaHandler) bean);
        return bean;
    }
}
