package com.example.messaging;

import com.example.model.CommandMessage;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@Component
public class OrchestratorPulsarProducer {

    private final PulsarTemplate<CommandMessage> commandTemplate;

    public OrchestratorPulsarProducer(PulsarTemplate<CommandMessage> commandTemplate) {
        this.commandTemplate = commandTemplate;
    }

    public CompletableFuture<MessageId> sendAsync(String topic, CommandMessage cmd, String correlationId) throws PulsarClientException {
        Map<String, String> props = new HashMap<>();
        props.put("X-Correlation-Id", correlationId);
        return commandTemplate.newMessage(cmd)
                .withTopic(topic)
                .withMessageCustomizer(mb -> {
                    mb.key(correlationId);
                    mb.properties(props);
                })
                .sendAsync();
    }
}