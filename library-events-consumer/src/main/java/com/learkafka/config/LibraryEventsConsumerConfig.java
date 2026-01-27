package com.learkafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.Objects;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer(){
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r,e) -> {
                    if(e instanceof RecoverableDataAccessException){
                        return new TopicPartition(retryTopic,r.partition());
                    }else{
                        return new TopicPartition(deadLetterTopic,r.partition());
                    }
                }
        );

        return recoverer;
    }

    public DefaultErrorHandler errorHandler(){
        var exceptionsToIgnoreList = List.of(
                IllegalArgumentException.class
        );

        FixedBackOff fixedBackOff = new FixedBackOff(1000L,2);

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2_0);
        expBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(
                publishingRecoverer(),
                //fixedBackOff
                expBackOff
        );

        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        errorHandler.setRetryListeners((record,exception,deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {}, deliveryAttempt: {}, record: {}", exception.getMessage(),deliveryAttempt,record);
        });

        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
