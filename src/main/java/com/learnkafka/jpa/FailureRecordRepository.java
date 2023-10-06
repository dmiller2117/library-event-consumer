package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.entity.enums.Status;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {
    List<FailureRecord> findAllByStatus(Status status);
}