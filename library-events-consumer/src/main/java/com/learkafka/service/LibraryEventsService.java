package com.learkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learkafka.entity.LibraryEvent;
import com.learkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent =objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        log.info("LibraryEvent: {}",libraryEvent);

        switch(libraryEvent.getLibraryEventType()){
            case NEW:
                //Save operation
                save(libraryEvent);
                break;
            case UPDATE:
                //Update operation
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid event type");
        }

    }
    public void validate(LibraryEvent libraryEvent){
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Library Event Id is not valid");
        }

        log.info("Validation is successful for the libraryEvent: {}", libraryEvent);
    }
    public void save(LibraryEvent libraryEvent){
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("LibraryEvent saved successfully: {}", libraryEvent);
    }
}
