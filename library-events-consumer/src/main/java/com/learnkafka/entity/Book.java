package com.learnkafka.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class Book {

    @Id
    private Integer bookId;

    private String bookName;

    private String bookAuthor;

    @OneToOne
    @JoinColumn(name = "libraryEventId")
    private LibraryEvent libraryEvent;
}
