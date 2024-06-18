package com.example.controller;

import com.example.domain.LibraryEvent;
import com.example.domain.LibraryEventType;
import com.example.producer.LibraryEventsProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventsProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<?> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
            throws JsonProcessingException {

//        if (LibraryEventType.NEW != libraryEvent.libraryEventType()) {
//            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
//        }
        //invoke kafka producer
//        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
//        libraryEventProducer.sendLibraryEvent(libraryEvent);
        libraryEventProducer.sendLibraryEvent_approach3(libraryEvent);
        log.info("sau khi truy van xong");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("libraryEvent: {}", libraryEvent);
        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventProducer.sendLibraryEvent_approach3(libraryEvent);
        log.info("after produce call");
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (!LibraryEventType.UPDATE.equals(libraryEvent.getLibraryEventType()))  {
            log.info("Inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }


}
