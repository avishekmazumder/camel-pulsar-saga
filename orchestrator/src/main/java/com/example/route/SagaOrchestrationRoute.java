
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

//@Component
public class SagaOrchestrationRoute extends RouteBuilder {

    @Value("${service.a.url}")
    private String serviceAUrl;

    @Value("${service.b.url}")
    private String serviceBUrl;

    @Autowired
    StatusCodeProcessor statusCodeProcessor;

/*    @Override
    public void configure() throws Exception {
        getContext().addService(new InMemorySagaService(), true);

        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(1)
                .redeliveryDelay(2000)
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        // Let Camel throw HttpOperationFailedException to trigger saga rollback

        onException(Exception.class)
                .handled(false) // <- important
                .log("Saga failed. Exception: ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"message\":\"Saga failed. Compensation triggered.\",\"error\":\"${exception.message}\"}"));

        from("direct:startSaga")
            .saga()
                .propagation(SagaPropagation.REQUIRED)
                .compensation("direct:compensateA")
            .routeId("saga-orchestration-route")
            .log("Starting Saga with OrchestrationRequest")
            .setProperty("orchestrationRequest", body())

            .process(exchange -> {
                OrchestrationRequest req = exchange.getProperty("orchestrationRequest", OrchestrationRequest.class);
                TagRequest tag = new TagRequest();
                tag.setTag(req.getServiceATag());
                exchange.getIn().setBody(tag);
            })
            .marshal().json()
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .doTry()
                .toD(serviceAUrl + "?bridgeEndpoint=true")
                .log("Service A response: ${body}")
                .process(statusCodeProcessor) // throws on non-2xx
                .setProperty("responseA", simple("${body}"))
            // --- Specific catches for A ---
            .doCatch(org.springframework.web.client.HttpClientErrorException.class,
                    org.springframework.web.client.HttpServerErrorException.class,
                    org.apache.camel.http.base.HttpOperationFailedException.class)
                .log("Service A client error: ${exception.statusCode} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"Client error calling Service A\",\"error\":\"${exception.message}\"}"))
*//*                .process(e -> {
                    throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                })*//*
                .stop()

            .doCatch(Exception.class)
                .log("Service A unexpected error: ${exception.class} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"Unexpected error calling Service A\",\"error\":\"${exception.message}\"}"))
                .process(e -> {
                    throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                })
            .end()

                // Only reach B if A succeeded and stored responseA
            .to("direct:invokeB")

            .bean("responseAggregator", "aggregate(${exchangeProperty.responseA}, ${exchangeProperty.responseB})")
            .log("Aggregated response: ${body}")
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200));

        from("direct:invokeB")
            .saga()
                .propagation(SagaPropagation.REQUIRED)
                .compensation("direct:compensateB")
            .routeId("invoke-service-b")
            .process(exchange -> {
                OrchestrationRequest req = exchange.getProperty("orchestrationRequest", OrchestrationRequest.class);
                TagRequest tag = new TagRequest();
                tag.setTag(req.getServiceBTag());
                exchange.getIn().setBody(tag);
            })
            .marshal().json()
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .doTry()
                .toD(serviceBUrl + "?bridgeEndpoint=true")
                .log("Service B response: ${body}")
                .process(statusCodeProcessor)
                .setProperty("responseB", simple("${body}"))
            // --- B: HTTP errors => DO NOT compensate B, BUT fail saga so A rolls back
            .doCatch(org.springframework.web.client.HttpClientErrorException.class,
                    org.springframework.web.client.HttpServerErrorException.class,
                    org.apache.camel.http.base.HttpOperationFailedException.class)
                .log("Service B HTTP error: ${exception.statusCode} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, simple("${exception.statusCode}"))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"B\",\"message\":\"HTTP failure calling Service B\","
                        + "\"status\":${exception.statusCode},"
                        + "\"error\":\"${exception.message}\"}"))
                // rethrow -> exchange failed -> saga compensates A (B hasn't completed so no compensateB)
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })

            // --- B: other errors => rethrow so saga compensates B (and A) if completed
            .doCatch(Exception.class)
                .log("Service B unexpected error: ${exception.class} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"B\",\"message\":\"Unexpected error calling Service B\","
                        + "\"error\":\"${exception.message}\"}"))
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
            .end();

        from("direct:compensateA")
            .log("Compensating A for: ${body}")
            .process(exchange -> {
                TagRequest tag = new TagRequest();
                tag.setTag("compensateA1");
                exchange.getIn().setBody(tag);
            })
            .marshal().json()
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .toD("http://localhost:8081/compensateA?bridgeEndpoint=true")
            .process(statusCodeProcessor);

        from("direct:compensateB")
            .log("Compensating B for: ${body}")
            .process(exchange -> {
                TagRequest tag = new TagRequest();
                tag.setTag("compensateB1");
                exchange.getIn().setBody(tag);
            })
            .marshal().json()
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .toD("http://localhost:8082/compensateB?bridgeEndpoint=true");
    }*/

    @Override
    public void configure() throws Exception {

        getContext().addService(new InMemorySagaService(), true);

        errorHandler(defaultErrorHandler()
                .maximumRedeliveries(1)
                .redeliveryDelay(2000)
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        // Global: format error but DO NOT swallow so saga sees failure
        onException(Exception.class)
                .handled(false)
                .log("Saga failed. Exception: ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"message\":\"Saga failed. Compensation triggered.\",\"error\":\"${exception.message}\"}"));

        /* ===================== Orchestration (single saga) ===================== */
        from("direct:startSaga")
                .routeId("saga-orchestration-route")
                .saga().propagation(SagaPropagation.REQUIRED)   // start/join one saga spanning A->B
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
                // A succeeded; now register A as a completed participant
                .to("direct:registerA")

                // ---- Call Service B (no saga here) ----
                .to("direct:callB")
                // if B succeeded, register B as completed participant


                // Aggregate
                .bean("responseAggregator", "aggregate(${exchangeProperty.responseA}, ${exchangeProperty.responseB})")
                .log("Aggregated response: ${body}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
                .end();

        /* ===================== Service A (no saga here) ===================== */
        from("direct:callA")
                .routeId("call-service-a")
                .log("Calling Service A")
                .process(ex -> {
                    OrchestrationRequest req = ex.getProperty("orchestrationRequest", OrchestrationRequest.class);
                    TagRequest tag = new TagRequest();
                    tag.setTag(req.getServiceATag());
                    ex.getIn().setBody(tag);
                })
                .marshal().json()
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .doTry()
                .toD(serviceAUrl + "?bridgeEndpoint=true")
                .log("Service A response: ${body}")
                .process(statusCodeProcessor)                       // throws on non-2xx
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
                .setProperty("abortOrchestration", constant(true))  // orchestrator will stop
                .stop()
                .doCatch(Exception.class)
                .log("Service A unexpected error: ${exception.class} - ${exception.message}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setBody(simple("{\"service\":\"A\",\"message\":\"Unexpected error calling Service A\","
                        + "\"error\":\"${exception.message}\"}"))
                // fail saga; since A didn't register yet, there's no compensateA
                .process(e -> { throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class); })
                .end();

        /* ===================== Service B (no saga here) ===================== */
        from("direct:callB")
                .routeId("call-service-b")
                .log("Calling Service B")
                .process(ex -> {
                    OrchestrationRequest req = ex.getProperty("orchestrationRequest", OrchestrationRequest.class);
                    TagRequest tag = new TagRequest();
                    tag.setTag(req.getServiceBTag());
                    ex.getIn().setBody(tag);
                })
                .marshal().json()
                .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .doTry()
                .toD(serviceBUrl + "?bridgeEndpoint=true")
                .log("Service B response: ${body}")
                .to("direct:registerB")
                .process(statusCodeProcessor)
                .setProperty("responseB", simple("${body}"))
                // B HTTP errors: rethrow so the saga fails -> compensate A (B not yet registered)
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

        /* ===================== Registration routes (create completed participants) ===================== */
        from("direct:registerA")
                .routeId("register-a-as-participant")
                .saga()                                               // join existing saga
                .propagation(SagaPropagation.MANDATORY)
                .compensation("direct:compensateA")
                .log("Registered A as completed participant");        // route ends OK -> A is 'completed'

        from("direct:registerB")
                .routeId("register-b-as-participant")
                .saga()
                .propagation(SagaPropagation.MANDATORY)
                .compensation("direct:compensateB")
                .log("Registered B as completed participant");        // only reached after B success

        /* ===================== Compensations ===================== */
        from("direct:compensateA")
                .log("Compensating A for: ${body}")
                .process(ex -> {
                    TagRequest tag = new TagRequest();
                    tag.setTag("compensateA1");
                    ex.getIn().setBody(tag);
                })
                .marshal().json()
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .toD("http://localhost:8081/compensateA?bridgeEndpoint=true")
                .process(statusCodeProcessor);

        from("direct:compensateB")
                .log("Compensating B for: ${body}")
                .process(ex -> {
                    TagRequest tag = new TagRequest();
                    tag.setTag("compensateB1");
                    ex.getIn().setBody(tag);
                })
                .marshal().json()
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .toD("http://localhost:8082/compensateB?bridgeEndpoint=true")
                .process(statusCodeProcessor);
    }



}
