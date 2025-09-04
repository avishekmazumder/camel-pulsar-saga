package com.example.messaging;

import com.example.model.CommandMessage;
import com.example.model.ReplyMessage;
import com.example.model.TagRequest;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.pulsar.annotation.PulsarListener;

import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.stereotype.Component;

@Component
public class ServiceAPulsarHandler {
    private static final Logger log = LoggerFactory.getLogger(ServiceAPulsarHandler.class);

    private final PulsarTemplate<ReplyMessage> replyTemplate;
    private final String responseTopic;
    private final String serviceName = "A";

    @Autowired
    public ServiceAPulsarHandler(PulsarTemplate<ReplyMessage> replyTemplate,
                                 @Value("${app.pulsar.response-topic}") String responseTopic) {
        this.replyTemplate = replyTemplate;
        this.responseTopic = responseTopic;
    }

    @PulsarListener(
            topics = "${app.pulsar.command-topic}",
            subscriptionName = "svcA",
            schemaType = SchemaType.JSON,
            subscriptionType = SubscriptionType.Exclusive)
    public void onCommand(CommandMessage cmd, Acknowledgement ack) throws PulsarClientException {
        try {
            if (cmd == null) {
                ack.acknowledge();
                return;
            }

            if (cmd.getTargetService() == null || !serviceName.equalsIgnoreCase(cmd.getTargetService())) {
                ack.acknowledge();
                return;
            }

            ReplyMessage reply = new ReplyMessage();
            reply.setCorrelationId(cmd.getCorrelationId());
            reply.setService(serviceName);

            if ("COMPENSATE".equalsIgnoreCase(cmd.getAction())) {
                reply.setStatus(200);
                reply.setMessage("Compensated A for: " + (cmd.getPayload() != null ? cmd.getPayload().getTag() : "n/a"));
                replyTemplate.send(responseTopic, reply);
                ack.acknowledge();
                return;
            }

            TagRequest req = cmd.getPayload();
            String tag = req != null ? req.getTag() : "null";

            if ("failA0".equals(tag)) {
                reply.setStatus(500);
                reply.setError("Simulated failure in service A for tag " + tag);
            } else {
                reply.setStatus("failA1".equals(tag) ? 201 : 200);
                reply.setMessage("Processed A: " + tag);
            }

            replyTemplate.send(responseTopic, reply);
            ack.acknowledge();

        } catch (Exception ex) {
            ReplyMessage reply = new ReplyMessage();
            reply.setCorrelationId(cmd != null ? cmd.getCorrelationId() : "n/a");
            reply.setService(serviceName);
            reply.setStatus(500);
            reply.setError(ex.getMessage());
            replyTemplate.send(responseTopic, reply);
            ack.acknowledge();
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
