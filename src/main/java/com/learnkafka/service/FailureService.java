package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.entity.enums.Status;
import com.learnkafka.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;

    @Autowired
    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, Status status) {
        failureRecordRepository.save(FailureRecord.builder()
                .topic(consumerRecord.topic())
                .key_value(consumerRecord.key())
                .partition(consumerRecord.partition())
                .offset_value(consumerRecord.offset())
                .exception(e.getCause().getMessage())
                .status(status)
                .build());
    }
}