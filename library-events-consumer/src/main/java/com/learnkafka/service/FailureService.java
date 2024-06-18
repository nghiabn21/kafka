package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FailureService {

    @Autowired
    private FailureRecordRepository failureRecordRepository;

//    public FailureService(FailureRecordRepository failureRecordRepository) {
//        this.failureRecordRepository = failureRecordRepository;
//    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus) {
        var failureRecord = new FailureRecord(
                 record.topic()
                , record.key()
                , record.value()
                , record.partition()
                , record.offset()
                , exception.getCause().getMessage()
                , recordStatus);

        failureRecordRepository.save(failureRecord);

    }
}
