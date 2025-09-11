package com.example.messaging;

import com.example.model.CommandMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.stereotype.Service;

//@Service
@Slf4j
@RequiredArgsConstructor
public class EventConsumer {

    private final ObjectMapper mapper; // reuse a singleton mapper

/*    @PulsarListener(
            topics = "${app.pulsar.command-topic}",
            subscriptionName = "my-subscription",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared,
            ackMode = AckMode.MANUAL   // <-- enable manual ack for this listener
    )
    public void consumeRawEvent(CommandMessage msg, Acknowledgement ack) {
        try {
            log.info("EventConsumer::consumeRawEvent payload={}", mapper.writeValueAsString(msg));
            // business logic here...

            ack.acknowledge();      // <-- ACK on success
        } catch (Exception e) {
            log.error("EventConsumer::consumeRawEvent failed", e);
            ack.nack();
        }
    }*/
}
