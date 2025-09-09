
package com.example.route;

import com.example.model.CommandMessage;
import com.example.model.OrchestrationRequest;
import com.example.model.OrchestrationResponse;
import com.example.model.ReplyMessage;
import com.example.model.TagRequest;
import com.example.messaging.OrchestratorPulsarProducer;
import com.example.util.PendingSagaRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.saga.InMemorySagaService;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class PulsarSagaOrchestrationRoute extends RouteBuilder {

    private static final long REPLY_TIMEOUT_MS = 30000;

    private final PendingSagaRegistry pending;

    public PulsarSagaOrchestrationRoute(PendingSagaRegistry pending) {
        this.pending = pending;
    }

    @Override
    public void configure() {

        getContext().addService(new InMemorySagaService(), true);

        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(1)
                .redeliveryDelay(2000)
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        // === Start saga, send commands only (no blocking wait) ===
        from("direct:startSagaMq")
            .routeId("pulsar-saga-orchestrator-async")
            .saga().propagation(SagaPropagation.REQUIRED)
            .log("Starting Pulsar Saga (async)")
            .setProperty("request", body())
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                if (cid == null) {
                    cid = UUID.randomUUID().toString();
                    ex.setProperty("cid", cid);
                }
                OrchestrationRequest req = ex.getProperty("request", OrchestrationRequest.class);
                ex.setProperty("tagA", req.getServiceATag());
                ex.setProperty("tagB", req.getServiceBTag());
            })
            // send A + B
            .to("direct:sendACommand")
            .to("direct:sendBCommand")
            // snapshot ack futures into registry (non-blocking)
            .process(ex -> {
                String cid = ex.getProperty("cid", String.class);
                CompletableFuture<?> ackA = ex.getProperty("ackA", CompletableFuture.class);
                CompletableFuture<?> ackB = ex.getProperty("ackB", CompletableFuture.class);
                pending.attachAcks(cid, ackA, ackB);
            })
            .log("Commands dispatched for cid=${exchangeProperty.cid}");

        // === Aggregator: consume replies from both services and complete DeferredResult ===
        // Both ServiceAChannelRoutes and ServiceBChannelRoutes should route replies to direct:agg-in
        from("direct:agg-in")
            .routeId("pulsar-saga-aggregator")
            // group by correlation id
            .aggregate(header("corrId"))
                .aggregationStrategy((oldEx, newEx) -> {
                    OrchestrationResponse acc;
                    ReplyMessage r = newEx.getMessage().getBody(ReplyMessage.class);
                    if (oldEx == null) {
                        acc = new OrchestrationResponse(null, null);
                    } else {
                        acc = oldEx.getMessage().getBody(OrchestrationResponse.class);
                    }
                    if (r != null) {
                        if ("A".equalsIgnoreCase(r.getService())) {
                            acc.setResponseA(r.getMessage());
                        } else if ("B".equalsIgnoreCase(r.getService())) {
                            acc.setResponseB(r.getMessage());
                        } else {
                            // fallback: put into A if unknown
                            if (acc.getResponseA() == null) acc.setResponseA(r.getMessage());
                            else acc.setResponseB(r.getMessage());
                        }
                    }
                    newEx.getMessage().setBody(acc);
                    return newEx;
                })
                .completionTimeout(REPLY_TIMEOUT_MS)
                .completionSize(2)
                .eagerCheckCompletion()
                .log("Aggregator completed for cid=${header.corrId} by=${exchangeProperty.CamelAggregatedCompletedBy} size=${exchangeProperty.CamelAggregatedSize}")
                .process(ex -> {
                    String cid = ex.getMessage().getHeader("corrId", String.class);
                    OrchestrationResponse resp = ex.getMessage().getBody(OrchestrationResponse.class);
                    String by = ex.getProperty(Exchange.AGGREGATED_COMPLETED_BY, String.class);
                    if ("timeout".equalsIgnoreCase(by)) {
                        // timeout already compensates via PendingSagaRegistry timeout hook
                        pending.completeSuccess(cid, resp); // still return whatever we have
                    } else {
                        pending.completeSuccess(cid, resp);
                    }
                })
            .end();
    }
}
