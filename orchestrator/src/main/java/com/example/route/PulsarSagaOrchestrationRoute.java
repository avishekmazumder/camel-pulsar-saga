package com.example.route;

import com.example.model.*;
import com.example.ResponseAggregator;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.saga.InMemorySagaService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class PulsarSagaOrchestrationRoute extends RouteBuilder {

    private static final int REPLY_TIMEOUT_MS = 30000;
    private final ResponseAggregator aggregator;

    public PulsarSagaOrchestrationRoute(ResponseAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public void configure() throws Exception {

        getContext().addService(new InMemorySagaService(), true);

        errorHandler(defaultErrorHandler()
            .maximumRedeliveries(1)
            .redeliveryDelay(2000)
            .retryAttemptedLogLevel(LoggingLevel.WARN));

        from("direct:startSagaMq")
            .routeId("pulsar-saga-orchestrator")
            .saga().propagation(SagaPropagation.REQUIRED)
            .log("Starting Pulsar Saga")
            .setProperty("request", body())
            .process(ex -> {
                String cid = UUID.randomUUID().toString();
                ex.setProperty("cid", cid);
                OrchestrationRequest req = ex.getProperty("request", OrchestrationRequest.class);
                ex.setProperty("tagA", req.getServiceATag());
                ex.setProperty("tagB", req.getServiceBTag());
            })

            // Send both commands asynchronously (store ack futures)
            .to("direct:sendACommand")
            .to("direct:sendBCommand")
            .log("Producer messages sent")

            // Derive ack flags without blocking saga on network I/O
            .process(ex -> {
                CompletableFuture<?> ackA = ex.getProperty("ackA", CompletableFuture.class);
                CompletableFuture<?> ackB = ex.getProperty("ackB", CompletableFuture.class);
                boolean aOk = false, bOk = false;
                try { aOk = ackA != null && ackA.get(5, TimeUnit.SECONDS) != null; } catch (Exception ignore) {}
                try { bOk = ackB != null && ackB.get(5, TimeUnit.SECONDS) != null; } catch (Exception ignore) {}
                ex.setProperty("ackAOk", aOk);
                ex.setProperty("ackBOk", bOk);
            })


            // Wait for responses from per-correlation SEDA channels populated by consumer routes

                .doTry()
                // A: wait for reply (throw if timeout)
                .pollEnrich("seda:replyA-${exchangeProperty.cid}", REPLY_TIMEOUT_MS)
                .process((org.apache.camel.Exchange ex) -> {
                    Object body = ex.getIn().getBody();
                    if (body == null) {
                        throw new RuntimeException("Timed out waiting for Service A reply");
                    }
                    ex.setProperty("respA", ex.getIn().getBody(com.example.model.ReplyMessage.class));
                })

                // B: wait for reply (throw if timeout)
                .pollEnrich("seda:replyB-${exchangeProperty.cid}", REPLY_TIMEOUT_MS)
                .process((org.apache.camel.Exchange ex) -> {
                    Object body = ex.getIn().getBody();
                    if (body == null) {
                        throw new RuntimeException("Timed out waiting for Service B reply");
                    }
                    ex.setProperty("respB", ex.getIn().getBody(com.example.model.ReplyMessage.class));
                })

                // Build final orchestration response from each service’s message
                .process((org.apache.camel.Exchange ex) -> {
                    com.example.model.ReplyMessage a = ex.getProperty("respA", com.example.model.ReplyMessage.class);
                    com.example.model.ReplyMessage b = ex.getProperty("respB", com.example.model.ReplyMessage.class);

                    String msgA = (a != null) ? a.getMessage() : null;
                    String msgB = (b != null) ? b.getMessage() : null;

                    com.example.model.OrchestrationResponse resp =
                            new com.example.model.OrchestrationResponse(msgA, msgB);

                    ex.getMessage().setBody(resp);
                    ex.getMessage().setHeader(org.apache.camel.Exchange.HTTP_RESPONSE_CODE, 200);
                })
                .doCatch(Exception.class)
                .log(org.apache.camel.LoggingLevel.ERROR,
                        "Saga failed or timed out: ${exception.message}. Triggering compensation where applicable.")
                .process((org.apache.camel.Exchange exchange) -> {
                    String cid = exchange.getProperty("cid", String.class);
                    boolean aOk = Boolean.TRUE.equals(exchange.getProperty("ackAOk", Boolean.class));
                    boolean bOk = Boolean.TRUE.equals(exchange.getProperty("ackBOk", Boolean.class));

                    if (aOk) {
                        exchange.getContext().createProducerTemplate()
                                .sendBodyAndHeader("direct:sendACompensate", null, "cid", cid);
                    }
                    if (bOk) {
                        exchange.getContext().createProducerTemplate()
                                .sendBodyAndHeader("direct:sendBCompensate", null, "cid", cid);
                    }
                })
                .setHeader(org.apache.camel.Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(org.apache.camel.Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"message\":\"Saga failed. Compensation triggered.\","
                        + "\"error\":\"${exception.message}\"}"))
                .end();



        // Lightweight compensation sender routes
        from("direct:sendACompensate")
            .routeId("send-a-compensate")
            .process(ex -> {
                String cid = (String) ex.getIn().getHeader("cid");
                CommandMessage cmd = new CommandMessage();
                cmd.setCorrelationId(cid);
                cmd.setTargetService("A");
                cmd.setAction("COMPENSATE");
                TagRequest t = new TagRequest();
                t.setTag("compensateA");
                cmd.setPayload(t);
                ex.getIn().setBody(cmd);
                Map<String,String> props = new HashMap<>();
                props.put("X-Correlation-Id", cid);
                ex.getIn().setHeader("CamelPulsarMessageProperties", props);
                ex.getIn().setHeader("CamelPulsarMessageKey", cid);
            })
            .marshal().json()
            .toD("pulsar:{{app.pulsar.service-a.compensate-topic}}?producerName=orchestrator-comp&sendTimeoutMs=10000");

        from("direct:sendBCompensate")
            .routeId("send-b-compensate")
            .process(ex -> {
                String cid = (String) ex.getIn().getHeader("cid");
                CommandMessage cmd = new CommandMessage();
                cmd.setCorrelationId(cid);
                cmd.setTargetService("B");
                cmd.setAction("COMPENSATE");
                TagRequest t = new TagRequest();
                t.setTag("compensateB");
                cmd.setPayload(t);
                ex.getIn().setBody(cmd);
                Map<String,String> props = new HashMap<>();
                props.put("X-Correlation-Id", cid);
                ex.getIn().setHeader("CamelPulsarMessageProperties", props);
                ex.getIn().setHeader("CamelPulsarMessageKey", cid);
            })
            .marshal().json()
            .toD("pulsar:{{app.pulsar.service-b.compensate-topic}}?producerName=orchestrator-comp&sendTimeoutMs=10000");
    }
}