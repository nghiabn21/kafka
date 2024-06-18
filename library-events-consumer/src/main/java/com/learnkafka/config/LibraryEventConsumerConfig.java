package com.learnkafka.config;


import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String SUCCESS = "SUCCESS";
    public static final String DEAD = "DEAD";
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureService failureService;
    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
                , (r, e) -> {
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

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            // recovery logic
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            // non-recovery-logic
            log.info("Inside the non recoverable logic and skipping the record : {}", record);
            failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, DEAD);
        }
    };

    public DefaultErrorHandler errorHandler() {

        var exceptiopnToIgnorelist = List.of(
                IllegalArgumentException.class
        );

        var exceptiopnToRetryIgnorelist = List.of(
                RecoverableDataAccessException.class
        );

        var fixBackOff = new FixedBackOff(1000L, 2);

        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L); // lần gọi đầu là 1s
        expBackOff.setMultiplier(2.0);    // các lần gọi từ lần t2 sẽ là 2s ???
        expBackOff.setMaxInterval(2_000L);

        // thử lại 2 lần vs độ trễ 1s
        var errorHanle = new DefaultErrorHandler(
                consumerRecordRecoverer,  // cách 2: lưu message chưa gửi dc vào db r setup giờ r gửi lại
//                publishingRecoverer(), // cách 1: khi gửi message lỗi thì retry lại nó nhiều lần gửi lại
                expBackOff
//                fixBackOff
        );

        // bỏ qua lỗi khi đã gặp phải
        exceptiopnToIgnorelist.forEach(errorHanle::addNotRetryableExceptions);
//        exceptiopnToRetryIgnorelist.forEach(errorHanle::addNotRetryableExceptions);

        // theo dõi mỗi lần thử thất bại
        errorHanle.setRetryListeners(
                (record, ex, deliveryAttempt) ->
                        log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} "
                                , ex.getMessage(), deliveryAttempt)
        );

        return errorHanle;
    }

    @Bean
//    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // chạy song song từ 3 phân vùng
        factory.setConcurrency(3);
        // thử lại bản ghi không thành công 2 lần độ trễ 1s
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}