package com.example.controller;

import com.example.controller.LibraryEventController;
import com.example.domain.LibraryEvent;
import com.example.producer.LibraryEventsProducer;
import com.example.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
public class LibraryEventControllerTestUtil {

    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    LibraryEventsProducer libraryEventProducer;

    @Test
    void postLibraryEvent() throws Exception {
        //given
        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void postLibraryEvent_4xx() throws Exception {
        //given

        LibraryEvent libraryEvent = TestUtil.libraryEventRecordWithInvalidBook();

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);
        //expect
        String expectedErrorMessage = "book.bookId  must not be null, book.bookName  must not be blank";
        mockMvc.perform(post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));

    }

//    @Test
//    void updateLibraryEvent() throws Exception {
//
//        //given
//
//
//        String json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordUpdate());
//        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
//
//        //expect
//        mockMvc.perform(
//                        put("/v1/libraryevent")
//                                .content(json)
//                                .contentType(MediaType.APPLICATION_JSON))
//                .andExpect(status().isOk());
//
//    }
//
//    @Test
//    void updateLibraryEvent_withNullLibraryEventId() throws Exception {
//
//        //given
//
//        String json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordUpdateWithNullLibraryEventId());
//        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
//
//        //expect
//        mockMvc.perform(
//                        put("/v1/libraryevent")
//                                .content(json)
//                                .contentType(MediaType.APPLICATION_JSON))
//                .andExpect(status().is4xxClientError())
//                .andExpect(content().string("Please pass the LibraryEventId"));
//
//    }

//    @Test
//    void updateLibraryEvent_withNullInvalidEventType() throws Exception {
//
//        //given
//
//        String json = objectMapper.writeValueAsString(TestUtil.newLibraryEventRecordWithLibraryEventId());
//        //when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);
//
//        //expect
//        mockMvc.perform(
//                        put("/v1/libraryevent")
//                                .content(json)
//                                .contentType(MediaType.APPLICATION_JSON))
//                .andExpect(status().is4xxClientError())
//                .andExpect(content().string("Only UPDATE event type is supported"));
//
//    }
}
