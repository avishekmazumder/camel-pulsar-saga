package com.example.route;

import com.example.model.CommandMessage;
import com.example.model.ReplyMessage;
import com.example.model.TagRequest;
import com.example.messaging.OrchestratorPulsarProducer;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class ServiceAChannelRoutes extends RouteBuilder {

    @Value("${app.pulsar.service-a.command-topic}")
    private String aCommandTopic;

    @Value("${app.pulsar.service-a.response-topic}")
    private String aResponseTopic;

    @Value("${app.pulsar.service-a.reply-subscription:orchestrator-a-replies}")
    private String aReplySub;

    private final OrchestratorPulsarProducer producer;

    public ServiceAChannelRoutes(OrchestratorPulsarProducer producer) {
        this.producer = producer;
    }

    @Override
    public void configure() {

        errorHandler(defaultErrorHandler()
            .maximumRedeliveries(0)
            .retryAttemptedLogLevel(LoggingLevel.WARN));

        // ---- Producer entry (orchestrator -> A command topic) ----
        from("direct:sendACommand")
            .routeId("send-a-command")
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                String tag = ex.getProperty("tagA", String.class);
                CommandMessage cmd = new CommandMessage();
                cmd.setCorrelationId(cid);
                cmd.setTargetService("A");
                cmd.setAction("PROCESS");
                TagRequest t = new TagRequest();
                t.setTag(tag);
                cmd.setPayload(t);
                ex.setProperty("cmdA", cmd);
            })
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                CommandMessage cmd = ex.getProperty("cmdA", CommandMessage.class);
                CompletableFuture<org.apache.pulsar.client.api.MessageId> fut =
                        producer.sendAsync(aCommandTopic, cmd, cid);
                ex.setProperty("ackA", fut);
            })
            .log("Sent async command for A with cid=${exchangeProperty.cid}");

        // ---- Response consumer (A -> orchestrator) ----
        final String consumerParams =
                "?subscriptionName={{app.pulsar.service-a.reply-subscription}}"
              + "&subscriptionType=Key_Shared"
              + "&subscriptionInitialPosition=Latest"
              + "&consumerQueueSize=10";

        from("pulsar:{{app.pulsar.service-a.response-topic}}" + consumerParams)
            .routeId("consume-a-replies")
            .process(ex -> {
                // Extract correlation from Pulsar properties
                @SuppressWarnings("unchecked")
                Map<String, String> props = ex.getIn().getHeader("CamelPulsarMessageProperties", Map.class);
                if (props != null && props.containsKey("X-Correlation-Id")) {
                    ex.getIn().setHeader("X-Correlation-Id", props.get("X-Correlation-Id"));
                }
            })
            .unmarshal().json(ReplyMessage.class)
            // fan-in to per-correlation SEDA channel
            .toD("seda:replyA-${header.X-Correlation-Id}");
    }
}