package com.iit.kafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iit.kafka.domain.Book;
import com.iit.kafka.domain.LibraryEvent;
import com.iit.kafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.concurrent.SettableListenableFuture;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventsControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private LibraryEventProducer libraryEventProducer;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void postLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String requestBody = objectMapper.writeValueAsString(libraryEvent);
        SettableListenableFuture future = new SettableListenableFuture();
        when(libraryEventProducer.sendLibraryEvent_GenericApproach(isA(LibraryEvent.class))).thenReturn(future);

        mockMvc.perform(post("/v1/libraryEvent")
        .content(requestBody)
        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }
}
