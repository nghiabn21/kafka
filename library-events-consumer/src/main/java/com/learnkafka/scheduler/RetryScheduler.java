package com.learnkafka.scheduler;

import com.learnkafka.config.LibraryEventConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {
    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    // chạy sau 10s
    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying Failed Records Started!");
        String status = LibraryEventConsumerConfig.RETRY;
        failureRecordRepository.findAllByStatus(status)
                .forEach(failureRecord -> {
                    try {
                        //libraryEventsService.processLibraryEvent();
                        log.info("Message lỗi!: {}", failureRecord);
                        ConsumerRecord<Integer, String> consumerRecord = buildConsumerRecord(failureRecord);
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        // libraryEventsConsumer.onMessage(consumerRecord); // This does not involve the recovery code for in the consumerConfig
                        failureRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
                    } catch (Exception e) {
                        log.error("Gọi lại không thành công !!! Exception in retry FailedRecords : ", e);
                    }
                });
        log.info("Retrying Failed Records Complete!");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey_value(),
                failureRecord.getErrorRecord());

    }
}
