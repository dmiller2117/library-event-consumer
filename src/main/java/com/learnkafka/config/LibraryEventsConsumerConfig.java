package com.learnkafka.config;

import com.learnkafka.entity.enums.Status;
import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka // needed on older versions, fyi only.
@Slf4j
public class LibraryEventsConsumerConfig {

    private final String retryTopic;
    private final String deadLetterTopic;
    private final KafkaProperties properties;
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private FailureService failureService;

    @Autowired
    public LibraryEventsConsumerConfig(
            KafkaTemplate<Integer, String> kafkaTemplate,
            @Value("${topics.retry:library-events.RETRY}") String retryTopic,
            @Value("${topics.dlt:library-events.DLT}") String deadLetterTopic,
            KafkaProperties properties,
            FailureService failureService) {
        this.kafkaTemplate = kafkaTemplate;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.properties = properties;
        this.failureService = failureService;
    }


    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                }
        );
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
        var record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            // recovery logic
            log.info("Inside recovery");
            failureService.saveFailedRecord(record, e, Status.RETRY);
        } else {
            // non recovery logic
            log.info("Inside non recovery");
            failureService.saveFailedRecord(record, e, Status.DEAD);
        }
    };

    public DefaultErrorHandler errorHandler() {

        List<Class<IllegalArgumentException>> exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
        List<Class<RecoverableDataAccessException>> exceptionsToRetryList = List.of(RecoverableDataAccessException.class);

        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2L);

        ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1_000L);
        exponentialBackOff.setMultiplier(2D);
        exponentialBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(
                consumerRecordRecoverer,
                //publishingRecoverer(),
                //fixedBackOff
                exponentialBackOff
        );

        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed record in retry listener, Exception : {} , deliverAttempt : {} ",
                    ex.getMessage(),
                    deliveryAttempt);
        }));
        errorHandler.addNotRetryableExceptions();
        // to ignore...
        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        // or retry, the choice is yours
        //exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);

        return errorHandler;
    }

    @Bean
        //@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        //kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }


}