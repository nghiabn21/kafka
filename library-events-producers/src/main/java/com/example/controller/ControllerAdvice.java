package com.example.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class ControllerAdvice {


    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<?> handleException(MethodArgumentNotValidException ex) {
      var message =  ex.getBindingResult()
                .getFieldErrors()
                .stream().map(fieldError -> fieldError.getField() + " " + " " + fieldError.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining(", "));
      log.info("Error message: {} " ,message);
      return  new ResponseEntity<>(message, HttpStatus.BAD_REQUEST);
    }

}
