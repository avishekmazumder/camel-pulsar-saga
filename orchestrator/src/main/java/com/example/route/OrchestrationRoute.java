package com.example.route;

import com.example.model.OrchestrationRequest;
import com.example.model.OrchestrationResponse;
import com.example.model.TagRequest;
import com.example.processor.StatusCodeProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.SagaCompletionMode;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.saga.InMemorySagaService;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

@Component
public class OrchestrationRoute extends RouteBuilder {

    @Value("${service.a.url}")
    private String serviceAUrl;

    @Value("${service.a.compensate.url}")
    private String serviceACompensateUrl;

    @Value("${service.b.url}")
    private String serviceBUrl;

    @Value("${service.b.compensate.url}")
    private String serviceBCompensateUrl;

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

        /* ===================== Orchestrator (single saga) ===================== */
        from("direct:startSaga")
                .routeId("saga-orchestration-route")
                .saga().propagation(SagaPropagation.REQUIRED)    // start/join one saga spanning A→B
                .log("Starting Saga with OrchestrationRequest")
                .setProperty("orchestrationRequest", body())

                // ---- Call Service A (no saga here) ----
                .to("direct:callA")

                // If A had HTTP error, abort gracefully (no compensation for A)
                .choice()
                .when(exchangeProperty("abortOrchestration").isEqualTo(true))
                .log("Aborting after Service A HTTP error")
                .stop()
                .otherwise()
                // A succeeded → register A as completed participant
                //.to("direct:registerA")

                // ---- Call Service B (no saga here) ----
                .to("direct:callB")

                // B succeeded → register B as completed participant
                //.to("direct:registerB")

                // Aggregate both responses
                .bean("responseAggregator", "aggregate(${exchangeProperty.responseA}, ${exchangeProperty.responseB})")
                .log("Aggregated response: ${body}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
                .end();

        /* ===================== Service A (work only; no saga here) ===================== */
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
                    .to("direct:registerA")
                    .process(statusCodeProcessor)                // throws on non-2xx
                    .setProperty("responseA", simple("${body}"))
                .doCatch(org.springframework.web.client.HttpClientErrorException.class,
                        org.springframework.web.client.HttpServerErrorException.class,
                        org.apache.camel.http.base.HttpOperationFailedException.class)
                .log("Service A HTTP error: ${exception.statusCode} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, simple("${exception.statusCode}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"HTTP failure calling Service A\","
                        + "\"status\":${exception.statusCode},"
                        + "\"error\":\"${exception.message}\"}"))
                .setProperty("abortOrchestration", constant(true)) // tell the orchestrator to stop
                .stop()                                            // no saga failure => no compensateA
                .doCatch(Exception.class)
                .log("Service A unexpected error: ${exception.class} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"Unexpected error calling Service A\","
                        + "\"error\":\"${exception.message}\"}"))
                // fail the saga; A was not registered yet, so no compensateA
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
                .end();

        /* ===================== Service B (work only; no saga here) ===================== */
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
                .to("direct:registerB")
                .process(statusCodeProcessor)
                .setProperty("responseB", simple("${body}"))
                // B HTTP errors: rethrow to fail saga -> compensate A (B not registered)
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

        /* ===================== Registration routes (these mark completion) ===================== */
        registerParticipant("direct:registerA", "direct:compensateA", "Service A");
        registerParticipant("direct:registerB", "direct:compensateB", "Service B");

        /* ===================== Compensations ===================== */
        from("direct:compensateA")
                .process(this::prepareCompensateARequest)
                .log("Compensating A for: ${body.toString}")
                .marshal().json()
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .toD(serviceACompensateUrl + "?bridgeEndpoint=true")
                .process(statusCodeProcessor);

        from("direct:compensateB")
                .process(this::prepareCompensateBRequest)
                .log("Compensating B for: ${body.toString}")
                .marshal().json()
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .toD(serviceBCompensateUrl + "?bridgeEndpoint=true")
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

