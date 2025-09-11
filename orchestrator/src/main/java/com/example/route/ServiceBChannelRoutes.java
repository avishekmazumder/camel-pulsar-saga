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
public class ServiceBChannelRoutes extends RouteBuilder {

    @Value("${app.pulsar.service-b.command-topic}")
    private String bCommandTopic;

    @Value("${app.pulsar.service-b.response-topic}")
    private String bResponseTopic;

    @Value("${app.pulsar.service-b.reply-subscription:orchestrator-b-replies}")
    private String bReplySub;

    private final OrchestratorPulsarProducer producer;

    public ServiceBChannelRoutes(OrchestratorPulsarProducer producer) {
        this.producer = producer;
    }

    @Override
    public void configure() {

        errorHandler(defaultErrorHandler()
            .maximumRedeliveries(0)
            .retryAttemptedLogLevel(LoggingLevel.WARN));

        // ---- Producer entry (orchestrator -> B command topic) ----
        from("direct:sendBCommand")
            .routeId("send-b-command")
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                String tag = ex.getProperty("tagB", String.class);
                CommandMessage cmd = new CommandMessage();
                cmd.setCorrelationId(cid);
                cmd.setTargetService("B");
                cmd.setAction("PROCESS");
                TagRequest t = new TagRequest();
                t.setTag(tag);
                cmd.setPayload(t);
                ex.setProperty("cmdB", cmd);
            })
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                CommandMessage cmd = ex.getProperty("cmdB", CommandMessage.class);
                java.util.concurrent.CompletableFuture<org.apache.pulsar.client.api.MessageId> fut =
                        producer.sendAsync(bCommandTopic, cmd, cid);
                ex.setProperty("ackB", fut);
            })
            .log("Sent async command for B with cid=${exchangeProperty.cid}");

        // ---- Response consumer (B -> orchestrator) ----
        final String consumerParams =
                "?subscriptionName={{app.pulsar.service-b.reply-subscription}}"
              + "&subscriptionType=Key_Shared"
              + "&subscriptionInitialPosition=Latest"
              + "&consumerQueueSize=10";

        from("pulsar:{{app.pulsar.service-b.response-topic}}" + consumerParams)
            .routeId("consume-b-replies")
            .process(ex -> {
                @SuppressWarnings("unchecked")
                java.util.Map<String,String> props = ex.getIn().getHeader("CamelPulsarMessageProperties", java.util.Map.class);
                if (props != null && props.containsKey("X-Correlation-Id")) {
                    ex.getIn().setHeader("X-Correlation-Id", props.get("X-Correlation-Id"));
                }
            })
            .unmarshal().json(ReplyMessage.class)
            .toD("seda:replyB-${header.X-Correlation-Id}");
    }
}