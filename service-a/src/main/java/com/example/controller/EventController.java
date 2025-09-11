package com.example.controller;


import com.example.model.CommandMessage;
import com.example.producer.EventPublisher;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class EventController {

    @Autowired
    private EventPublisher publisher;

    @PostMapping("/command")
    public String sendRawEvent(@RequestBody CommandMessage customer) throws PulsarClientException {
        publisher.publishCommandMessage(customer);
        return "Command object published !";
    }
}
