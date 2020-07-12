package com.iit.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iit.kafka.domain.Book;
import com.iit.kafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    private KafkaTemplate<Integer,String> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper;

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    @BeforeEach
    public void init(){
        ReflectionTestUtils.setField(libraryEventProducer, "avroTopic", "library-events");
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void sendLibraryEvent_GenericApproach_FailureTest() {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Calling Kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, ()-> libraryEventProducer.sendLibraryEvent_GenericApproach(libraryEvent).get());
    }

    @Test
    public void sendLibraryEvent_GenericApproach_SuccessTest() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String record = objectMapper.writeValueAsString(libraryEvent);
        SettableListenableFuture future = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(),record );
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  libraryEventProducer.sendLibraryEvent_GenericApproach(libraryEvent);

        SendResult<Integer,String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition()==1;

    }

    @Test
    public void sendLibraryEventSynchronousTest() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String record = objectMapper.writeValueAsString(libraryEvent);
        SettableListenableFuture future = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(),record );
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);
        future.set(sendResult);
        when(kafkaTemplate.send(Mockito.any(String.class),Mockito.any(),Mockito.any())).thenReturn(future);

        SendResult<Integer, String> sendResultActual = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        assert sendResultActual.getRecordMetadata().partition()==1;
    }

    @Test
    public void sendLibraryEventSynchronousExceptionTest() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Calling Kafka"));

        when(kafkaTemplate.send(Mockito.any(String.class),Mockito.any(),Mockito.any())).thenReturn(future);

        assertThrows(Exception.class, ()-> libraryEventProducer.sendLibraryEventSynchronous(libraryEvent));
    }

    @Test
    public void sendLibraryEvent_FailureTest() {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Calling Kafka"));

        when(kafkaTemplate.send(Mockito.any(String.class),Mockito.any(),Mockito.any())).thenReturn(future);

        assertThrows(Exception.class, ()-> libraryEventProducer.sendLibraryEvent(libraryEvent).get());
    }

    @Test
    public void sendLibraryEvent_SuccessTest() throws JsonProcessingException, ExecutionException, InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Root")
                .bookName("AI")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String record = objectMapper.writeValueAsString(libraryEvent);
        SettableListenableFuture future = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(),record );
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);
        future.set(sendResult);

        when(kafkaTemplate.send(Mockito.any(String.class),Mockito.any(),Mockito.any())).thenReturn(future);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  libraryEventProducer.sendLibraryEvent(libraryEvent);

        SendResult<Integer,String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition()==1;
    }
}
