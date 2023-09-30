package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;
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
@EnableKafka //:: needed on older versions, fyi only.
@Slf4j
public class LibraryEventsConsumerConfig {

    private final KafkaProperties properties;
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;

    public LibraryEventsConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                }
        );
        return recoverer;
    }

    public DefaultErrorHandler errorHandler() {

        List<Class<IllegalArgumentException>> exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
        List<Class<RecoverableDataAccessException>> exceptionsToRetryList = List.of(RecoverableDataAccessException.class);

        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2L);

        ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1_000L);
        exponentialBackOff.setMultiplier(2D);
        exponentialBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(
                publishingRecoverer(),
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
        //exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        // or retry, the choice is yours
        exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);

        return errorHandler;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            //failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);

        }
    };

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