package com.example.messaging;


import com.example.model.CommandMessage;
import com.example.model.ReplyMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ServiceAPulsarHandler {

    private final PulsarTemplate<ReplyMessage> replyTemplate;
    private final ObjectMapper mapper;

    @Value("${app.service.name:A}")
    private String serviceName;

    @Value("${app.pulsar.response-topic}")
    private String responseTopic;

    @PulsarListener(
            topics = "${app.pulsar.command-topic}",
            subscriptionName = "serviceA",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Shared,
            ackMode = AckMode.MANUAL
    )
    public void onCommand(Message<CommandMessage> msg, Acknowledgement ack) {
        String cid = null;
        try {
            if (msg == null || msg.getValue() == null) {
                ack.acknowledge();
                return;
            }

            CommandMessage cmd = msg.getValue();

            // Only process commands that target this service
            if (cmd.getTargetService() == null || !serviceName.equalsIgnoreCase(cmd.getTargetService())) {
                ack.acknowledge();
                return;
            }

            // Extract metadata from incoming Pulsar message
            Map<String, String> inProps = Optional.ofNullable(msg.getProperties()).orElse(Map.of());
            String incomingCid = inProps.get("X-Correlation-Id");
            cid = Optional.ofNullable(incomingCid).orElse(cmd.getCorrelationId());
            String key = Optional.ofNullable(msg.getKey()).orElse(cid);

            // Build reply payload
            ReplyMessage reply = new ReplyMessage();
            reply.setCorrelationId(cid);
            reply.setService(serviceName);
            reply.setMessage("Processed A:" + (cmd.getPayload() != null ? cmd.getPayload().getTag() : "n/a"));
            reply.setStatus(200);

            // Send reply with key + properties (copy all incoming, enforce X-Correlation-Id)
            String finalCid = cid;
            replyTemplate
                    .newMessage(reply)
                    .withTopic(responseTopic)
                    .withMessageCustomizer(mb -> {
                        if (key != null) {
                            mb.key(key);
                        }
                        // copy all existing properties from the incoming command
                        inProps.forEach(mb::property);

                        // ensure X-Correlation-Id is present and correct
                        if (finalCid != null) {
                            mb.property("X-Correlation-Id", finalCid);
                        }
                    })
                    .send();

            log.info("ServiceAPulsarHandler::onCommand key={}, cid={}, props={}, reply={}",
                    key, cid, inProps, mapper.writeValueAsString(reply));

            ack.acknowledge();

        } catch (Exception ex) {
            try {
                // Best-effort error reply (no properties needed here, orchestrator may still parse JSON)
                ReplyMessage error = new ReplyMessage();
                error.setCorrelationId(cid);
                error.setService(serviceName);
                error.setStatus(500);
                error.setError(ex.getMessage());
                replyTemplate.send(responseTopic, error);
            } catch (Exception ignore) {
                // swallow secondary send errors
            }
            try { ack.acknowledge(); } catch (Exception ignore) {}
            log.error("ServiceAPulsarHandler::onCommand failed", ex);
        }
    }




/*@org.springframework.pulsar.annotation.PulsarListener(
    topics = "${app.pulsar.compensate-topic}",
    subscriptionName = "svcComp",
    subscriptionType = org.apache.pulsar.client.api.SubscriptionType.Shared
)
public void onCompensate(com.example.model.CommandMessage cmd,
                         org.springframework.pulsar.listener.Acknowledgement ack) {
    try {
        if (cmd == null || cmd.getAction() == null || !"COMPENSATE".equalsIgnoreCase(cmd.getAction())) {
            ack.acknowledge();
            return;
        }
        String tag = cmd.getPayload() != null ? cmd.getPayload().getTag() : "";
        boolean mine = tag != null && tag.startsWith("compensateA");
        if (mine) {
            log.info("Service A: compensation done for tag {}", tag);
            ack.acknowledge();
        } else {
            // Not mine -> requeue so other service can pick it up
            ack.nack();
        }
    } catch (Exception ex) {
        log.error("Service A: error in compensation listener", ex);
        ack.nack();
    }
}*/
}
