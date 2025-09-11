package com.example.producer;

import com.example.model.CommandMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventPublisher {

    @Value("${app.pulsar.command-topic}")
    private String topicName;
    @Autowired
    private PulsarTemplate<Object> template;

    public void publishCommandMessage(CommandMessage commandMessage) throws PulsarClientException {
        template.send(topicName, commandMessage);
        log.info("EventPublisher::publishRawMessage publish the event {}", commandMessage.getPayload());
    }
}
