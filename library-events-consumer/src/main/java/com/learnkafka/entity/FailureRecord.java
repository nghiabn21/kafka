package com.learnkafka.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class FailureRecord {
    @Id
    @GeneratedValue
    private Integer bookId;
    private String topic;
    private Integer key_value;
    private String errorRecord;
    private Integer partition;
    private Long offset_value;
    private String exception;
    private String status;

    public FailureRecord(String topic, Integer key, String value, int partition, long offset, String message,
                         String recordStatus) {
    this.topic = topic;
    this.key_value = key;
    this.errorRecord = value;
    this.partition = partition;
    this.offset_value =offset;
    this.exception = message;
    this.status = recordStatus;
    }
}
