package com.example.route;

import com.example.model.OrchestrationRequest;
import com.example.model.TagRequest;
import com.example.processor.StatusCodeProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.saga.InMemorySagaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

//@Component
public class MulticastSagaOrchestrationRoute extends RouteBuilder {

    @Value("${service.a.url}")
    private String serviceAUrl;

    @Value("${service.b.url}")
    private String serviceBUrl;

    @Autowired
    StatusCodeProcessor statusCodeProcessor;

    @Override
    public void configure() throws Exception {

        // Saga coordinator
        getContext().addService(new InMemorySagaService(), true);

        // Generic retry/logging
        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(1)
                .redeliveryDelay(2000)
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        // Global failure -> shape a JSON error but DON'T swallow; saga must see failure
        onException(Exception.class)
                .handled(false)
                .log("Saga failed. Exception: ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"message\":\"Saga failed. Compensation triggered.\",\"error\":\"${exception.message}\"}"));

        /* ===================== Orchestrator (single saga, now parallel) ===================== */
        from("direct:startSaga")
                .routeId("saga-orchestration-route")
                .saga().propagation(SagaPropagation.REQUIRED)
                .log("Starting Saga (parallel) with OrchestrationRequest")
                .setProperty("orchestrationRequest", body())
                // Fan-out to A and B concurrently; fail fast if any branch throws
                .multicast().parallelProcessing().stopOnException()
                .to("direct:callA", "direct:callB")
                .end()
                // Build final response from properties set by branches
                .bean("responseAggregator", "aggregate(${exchangeProperty.responseA}, ${exchangeProperty.responseB})")
                .log("Aggregated response: ${body}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
                .end();

        /* ===================== Service A (work only; register on success) ===================== */
        from("direct:callA")
                .routeId("call-service-a")
                .log("Calling Service A")
                .process(this::prepareARequest)
                .marshal().json()
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .doTry()
                .toD(serviceAUrl + "?bridgeEndpoint=true")
                .log("Service A response: ${body}")
                .process(statusCodeProcessor)                // Http* exceptions on non-2xx are caught below
                .setProperty("responseA", simple("${body}"))
                .to("direct:registerA")                      // register only after success
                .doCatch(org.springframework.web.client.HttpClientErrorException.class,
                        org.springframework.web.client.HttpServerErrorException.class,
                        org.apache.camel.http.base.HttpOperationFailedException.class)
                .log("Service A HTTP error: ${exception.statusCode} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, simple("${exception.statusCode}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"HTTP failure calling Service A\","
                        + "\"status\":${exception.statusCode},"
                        + "\"error\":\"${exception.message}\"}"))
                // rethrow to fail the multicast/saga
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
                .doCatch(Exception.class)
                .log("Service A unexpected error: ${exception.class} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"Unexpected error calling Service A\","
                        + "\"error\":\"${exception.message}\"}"))
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
                .end();

        /* ===================== Service B (work only; register on success) ===================== */
        from("direct:callB")
                .routeId("call-service-b")
                .log("Calling Service B")
                .process(this::prepareBRequest)
                .marshal().json()
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .doTry()
                .toD(serviceBUrl + "?bridgeEndpoint=true")
                .log("Service B response: ${body}")
                .process(statusCodeProcessor)
                .setProperty("responseB", simple("${body}"))
                .to("direct:registerB")                      // register only after success
                .doCatch(org.springframework.web.client.HttpClientErrorException.class,
                        org.springframework.web.client.HttpServerErrorException.class,
                        org.apache.camel.http.base.HttpOperationFailedException.class)
                .log("Service B HTTP error: ${exception.statusCode} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, simple("${exception.statusCode}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"B\",\"message\":\"HTTP failure calling Service B\","
                        + "\"status\":${exception.statusCode},"
                        + "\"error\":\"${exception.message}\"}"))
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
                .doCatch(Exception.class)
                .log("Service B unexpected error: ${exception.class} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"B\",\"message\":\"Unexpected error calling Service B\","
                        + "\"error\":\"${exception.message}\"}"))
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
                .end();

        /* ===================== Registration routes (mark completion for compensation) ===================== */
        registerParticipant("direct:registerA", "direct:compensateA", "Service A");
        registerParticipant("direct:registerB", "direct:compensateB", "Service B");

        /* ===================== Compensations ===================== */
        from("direct:compensateA")
                .process(this::prepareCompensateARequest)
                .log("Compensating A for: ${body.toString}")
                .marshal().json()
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .toD("http://localhost:8081/compensateA?bridgeEndpoint=true")
                .process(statusCodeProcessor);

        from("direct:compensateB")
                .process(this::prepareCompensateBRequest)
                .log("Compensating B for: ${body.toString}")
                .marshal().json()
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .toD("http://localhost:8082/compensateB?bridgeEndpoint=true")
                .process(statusCodeProcessor);
    }

    /* ===================== Helpers ===================== */

    // Small, reusable route constructor that *registers* a completed participant
    private void registerParticipant(String routeUri, String compensationRoute, String participantName) {
        from(routeUri)
                .saga()
                .propagation(SagaPropagation.MANDATORY)     // join the saga started in direct:startSaga
                .compensation(compensationRoute)            // what to call if the saga fails later
                .log("Registered participant: " + participantName);
    }

    // Build requests
    private void prepareARequest(Exchange ex) {
        OrchestrationRequest req = ex.getProperty("orchestrationRequest", OrchestrationRequest.class);
        TagRequest tag = new TagRequest();
        tag.setTag(req.getServiceATag());
        ex.getIn().setBody(tag);
    }

    private void prepareBRequest(Exchange ex) {
        OrchestrationRequest req = ex.getProperty("orchestrationRequest", OrchestrationRequest.class);
        TagRequest tag = new TagRequest();
        tag.setTag(req.getServiceBTag());
        ex.getIn().setBody(tag);
    }

    // Build compensation payloads
    private void prepareCompensateARequest(Exchange ex) {
        TagRequest tag = new TagRequest();
        tag.setTag("compensateA1");
        ex.getIn().setBody(tag);
    }

    private void prepareCompensateBRequest(Exchange ex) {
        TagRequest tag = new TagRequest();
        tag.setTag("compensateB1");
        ex.getIn().setBody(tag);
    }
}
