package com.example.publisher;

import com.example.model.OrchestrationResponse;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventPublisher {

    Logger log = LoggerFactory.getLogger(EventPublisher.class);

    @Value("${spring.pulsar.producer.topic-name1}")
    private String topicName1;

    @Value("${spring.pulsar.producer.topic-name2}")
    private String topicName2;
    @Autowired
    private PulsarTemplate<Object> template;

    public void publishPlainMessage(String message) throws PulsarClientException {
        template.send(topicName1, message);
        log.info("EventPublisher::publishPlainMessage publish the event {}", message);
    }

    public void publishRawMessage(OrchestrationResponse orchestrationResponse) throws PulsarClientException {
        template.sendAsync(topicName2, orchestrationResponse)
                .whenComplete((messageId, ex) -> {
                    if (ex != null) {
                        log.error("Async publish failed for {}", topicName2, ex);
                    } else {
                        log.info("Async publish succeeded on topic={}, messageId={}, message={}", topicName2, messageId, orchestrationResponse.toString());
                    }
                });
    }

}
