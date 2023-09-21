package com.learnkafka.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import lombok.*;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

    @Id
    private Integer bookId;
    private String bookName;
    private String bookAuthor;
    @OneToOne
    @JoinColumn(name = "libraryEventId")
    private LibraryEvent libraryEvent;

}