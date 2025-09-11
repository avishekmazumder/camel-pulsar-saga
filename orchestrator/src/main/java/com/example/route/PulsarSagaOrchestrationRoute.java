package com.example.route;

import com.example.ResponseAggregator;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.saga.InMemorySagaService;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class PulsarSagaOrchestrationRoute extends RouteBuilder {

    private static final long REPLY_TIMEOUT_MS = 30000;
    private final ResponseAggregator aggregator;

    public PulsarSagaOrchestrationRoute(ResponseAggregator aggregator) {
        this.aggregator = aggregator;
    }

/*    @Override
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
            //.to("direct:sendBCommand")
            .log("Producer messages sent")

            // Derive ack flags without blocking saga on network I/O
            .process(ex -> {
                CompletableFuture<?> ackA = ex.getProperty("ackA", CompletableFuture.class);
                //CompletableFuture<?> ackB = ex.getProperty("ackB", CompletableFuture.class);
                boolean aOk = false, bOk = false;
                try { aOk = ackA != null && ackA.get(5, TimeUnit.SECONDS) != null; } catch (Exception ignore) {}
               // try { bOk = ackB != null && ackB.get(5, TimeUnit.SECONDS) != null; } catch (Exception ignore) {}
                ex.setProperty("ackAOk", aOk);
                //ex.setProperty("ackBOk", bOk);
            })


            // Wait for responses from per-correlation SEDA channels populated by consumer routes

            .doTry()
            // Precompute dynamic URI as a property
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                String uri = "seda:replyA-" + cid;
                ex.setProperty("replyAUri", uri);
            })
            .log("Waiting on SEDA queue seda:replyA-${exchangeProperty.replyAUri} (timeout: " + REPLY_TIMEOUT_MS + " ms)")

            // Use resolved string URI for pollEnrich (no DSL ambiguity, no need for .end())
            .pollEnrich().simple("${exchangeProperty.replyAUri}").timeout(REPLY_TIMEOUT_MS).end()

            .process(ex -> {
                Object body = ex.getIn().getBody();
                if (body == null) {
                    throw new RuntimeException("Timed out waiting for Service A reply");
                }
                ex.setProperty("respA", ex.getIn().getBody(com.example.model.ReplyMessage.class));
            })


            // B: wait for reply (throw if timeout)
//                .pollEnrich("seda:replyB-${exchangeProperty.cid}", REPLY_TIMEOUT_MS)
//                .process((org.apache.camel.Exchange ex) -> {
//                    Object body = ex.getIn().getBody();
//                    if (body == null) {
//                        throw new RuntimeException("Timed out waiting for Service B reply");
//                    }
//                    ex.setProperty("respB", ex.getIn().getBody(com.example.model.ReplyMessage.class));
//                })

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
    }*/

/*    @Override
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
                .to("direct:sendACommand")
                .log("Producer message for Service A sent")
                .process(ex -> {
                    CompletableFuture<?> ackA = ex.getProperty("ackA", CompletableFuture.class);
                    boolean aOk = false;
                    try {
                        aOk = ackA != null && ackA.get(5, TimeUnit.SECONDS) != null;
                    } catch (Exception ignore) {}
                    ex.setProperty("ackAOk", aOk);
                })

                .doTry()
                // Precompute full URI
                .process(ex -> {
                    String cid = ex.getProperty("cid", String.class);
                    ex.setProperty("replyAUri", "seda:replyA-" + cid);
                })
                .log("Polling SEDA for Service A: ${exchangeProperty.replyAUri}")
                .pollEnrich("${exchangeProperty.replyAUri}", REPLY_TIMEOUT_MS)
                .process(ex -> {
                    Object body = ex.getIn().getBody();
                    if (body == null) {
                        throw new RuntimeException("Timed out waiting for Service A reply");
                    }
                    ex.setProperty("respA", ex.getIn().getBody(ReplyMessage.class));
                })
                .process(ex -> {
                    ReplyMessage a = ex.getProperty("respA", ReplyMessage.class);
                    ReplyMessage b = ex.getProperty("respB", ReplyMessage.class);
                    String msgA = (a != null) ? a.getMessage() : null;
                    String msgB = (b != null) ? b.getMessage() : null;
                    OrchestrationResponse resp = new OrchestrationResponse(msgA, msgB);
                    ex.getMessage().setBody(resp);
                    ex.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
                })
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR,
                        "Saga failed or timed out: ${exception.message}. Triggering compensation.")
                .process(exchange -> {
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
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"message\":\"Saga failed. Compensation triggered.\","
                        + "\"error\":\"${exception.message}\"}"))
                .end();

        // Compensation routes (unchanged)
        from("direct:sendACompensate")
                .routeId("send-a-compensate")
                .process(ex -> {
                    String cid = ex.getIn().getHeader("cid", String.class);
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
                    String cid = ex.getIn().getHeader("cid", String.class);
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
    }*/

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
                    String cid = java.util.UUID.randomUUID().toString();
                    ex.setProperty("cid", cid);
                    com.example.model.OrchestrationRequest req =
                            ex.getProperty("request", com.example.model.OrchestrationRequest.class);
                    ex.setProperty("tagA", req.getServiceATag());
                    ex.setProperty("tagB", req.getServiceBTag());
                })

                // Send A and B commands (your direct:send*Command routes should set ack futures)
                .to("direct:sendACommand")
                .to("direct:sendBCommand")
                .log("Producer messages sent")

                // Resolve ack flags (non-blocking snapshot)
                .process(ex -> {
                    java.util.concurrent.CompletableFuture<?> ackA =
                            ex.getProperty("ackA", java.util.concurrent.CompletableFuture.class);
                    java.util.concurrent.CompletableFuture<?> ackB =
                            ex.getProperty("ackB", java.util.concurrent.CompletableFuture.class);

                    boolean aOk = ackA != null && ackA.isDone() && !ackA.isCompletedExceptionally();
                    boolean bOk = ackB != null && ackB.isDone() && !ackB.isCompletedExceptionally();

                    ex.setProperty("ackAOk", aOk);
                    ex.setProperty("ackBOk", bOk);
                })

                // ===== Try: blocking waits for A then B =====
                .doTry()
                // Precompute SEDA URIs
                .process(ex -> {
                    String cid = ex.getProperty("cid", String.class);
                    ex.setProperty("replyAUri", "seda:replyA-" + cid);
                    ex.setProperty("replyBUri", "seda:replyB-" + cid);
                })
                .log("Polling SEDA for Service A: ${exchangeProperty.replyAUri} (timeout: " + REPLY_TIMEOUT_MS + " ms)")
                .process(ex -> {
                    String uri = ex.getProperty("replyAUri", String.class);
                    org.apache.camel.ConsumerTemplate ct = ex.getContext().createConsumerTemplate();
                    try {
                        com.example.model.ReplyMessage r =
                                ct.receiveBody(uri, REPLY_TIMEOUT_MS, com.example.model.ReplyMessage.class);
                        if (r == null) {
                            throw new RuntimeException("Timed out waiting for Service A reply");
                        }
                        ex.setProperty("respA", r);
                    } finally {
                        // optional: ct.stop(); (context-managed templates can be reused)
                    }
                })

                .log("Polling SEDA for Service B: ${exchangeProperty.replyBUri} (timeout: " + REPLY_TIMEOUT_MS + " ms)")
                .process(ex -> {
                    String uri = ex.getProperty("replyBUri", String.class);
                    org.apache.camel.ConsumerTemplate ct = ex.getContext().createConsumerTemplate();
                    try {
                        com.example.model.ReplyMessage r =
                                ct.receiveBody(uri, REPLY_TIMEOUT_MS, com.example.model.ReplyMessage.class);
                        if (r == null) {
                            throw new RuntimeException("Timed out waiting for Service B reply");
                        }
                        ex.setProperty("respB", r);
                    } finally {
                        // optional: ct.stop();
                    }
                })

                // Build final HTTP response with both replies
                .process(ex -> {
                    com.example.model.ReplyMessage a = ex.getProperty("respA", com.example.model.ReplyMessage.class);
                    com.example.model.ReplyMessage b = ex.getProperty("respB", com.example.model.ReplyMessage.class);

                    // If your DTO has responseA/responseB fields:
                    com.example.model.OrchestrationResponse resp = new com.example.model.OrchestrationResponse(a.getMessage(), b.getMessage());

                    ex.getMessage().setBody(resp);
                    ex.getMessage().setHeader(org.apache.camel.Exchange.HTTP_RESPONSE_CODE, 200);
                })

                // ===== Catch: compensate on failure =====
                .doCatch(Exception.class)
                .log(org.apache.camel.LoggingLevel.ERROR,
                        "Saga failed or timed out: ${exception.message}. Triggering compensation where applicable.")
                .process(exchange -> {
                    // (Re-)snapshot acks at failure time (non-blocking)
                    java.util.concurrent.CompletableFuture<?> ackA =
                            exchange.getProperty("ackA", java.util.concurrent.CompletableFuture.class);
                    java.util.concurrent.CompletableFuture<?> ackB =
                            exchange.getProperty("ackB", java.util.concurrent.CompletableFuture.class);
                    boolean aOk = ackA != null && ackA.isDone() && !ackA.isCompletedExceptionally();
                    boolean bOk = ackB != null && ackB.isDone() && !ackB.isCompletedExceptionally();
                    exchange.setProperty("ackAOk", aOk);
                    exchange.setProperty("ackBOk", bOk);

                    String cid = exchange.getProperty("cid", String.class);

                    if (aOk) {
                        exchange.getContext().createProducerTemplate()
                                .sendBodyAndHeader("direct:sendACompensate", null, "cid", cid);
                    }
                    if (bOk) {
                        exchange.getContext().createProducerTemplate()
                                .sendBodyAndHeader("direct:sendBCompensate", null, "cid", cid);
                    }
                })
                .setHeader(org.apache.camel.Exchange.HTTP_RESPONSE_CODE, org.apache.camel.builder.Builder.constant(500))
                .setHeader(org.apache.camel.Exchange.CONTENT_TYPE, org.apache.camel.builder.Builder.constant("application/json"))
                .setBody(org.apache.camel.builder.Builder.simple(
                        "{\"message\":\"Saga failed. Compensation triggered.\"," +
                                "\"error\":\"${exception.message}\"}"
                ))
                .end();

        // ===== Compensation senders (unchanged) =====

        from("direct:sendACompensate")
                .routeId("send-a-compensate")
                .process(ex -> {
                    String cid = ex.getIn().getHeader("cid", String.class);
                    com.example.model.CommandMessage cmd = new com.example.model.CommandMessage();
                    cmd.setCorrelationId(cid);
                    cmd.setTargetService("A");
                    cmd.setAction("COMPENSATE");
                    com.example.model.TagRequest t = new com.example.model.TagRequest();
                    t.setTag("compensateA");
                    cmd.setPayload(t);
                    ex.getIn().setBody(cmd);
                    java.util.Map<String,String> props = new java.util.HashMap<>();
                    props.put("X-Correlation-Id", cid);
                    ex.getIn().setHeader("CamelPulsarMessageProperties", props);
                    ex.getIn().setHeader("CamelPulsarMessageKey", cid);
                })
                .marshal().json()
                .toD("pulsar:{{app.pulsar.service-a.compensate-topic}}?producerName=orchestrator-comp&sendTimeoutMs=10000");

        from("direct:sendBCompensate")
                .routeId("send-b-compensate")
                .process(ex -> {
                    String cid = ex.getIn().getHeader("cid", String.class);
                    com.example.model.CommandMessage cmd = new com.example.model.CommandMessage();
                    cmd.setCorrelationId(cid);
                    cmd.setTargetService("B");
                    cmd.setAction("COMPENSATE");
                    com.example.model.TagRequest t = new com.example.model.TagRequest();
                    t.setTag("compensateB");
                    cmd.setPayload(t);
                    ex.getIn().setBody(cmd);
                    java.util.Map<String,String> props = new java.util.HashMap<>();
                    props.put("X-Correlation-Id", cid);
                    ex.getIn().setHeader("CamelPulsarMessageProperties", props);
                    ex.getIn().setHeader("CamelPulsarMessageKey", cid);
                })
                .marshal().json()
                .toD("pulsar:{{app.pulsar.service-b.compensate-topic}}?producerName=orchestrator-comp&sendTimeoutMs=10000");
    }






}