package com.example.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("statusCodeProcessor")
public class StatusCodeProcessor implements Processor {

    Logger logger = LoggerFactory.getLogger(StatusCodeProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        Integer status = exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        if (status != null) {
            if (status == 201) {
                throw new RuntimeException("Simulating failure from StatusCodeProcessor");
            }
            exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, status);
            logger.info("Received HTTP status code: {}", status);
        } else {
            logger.warn("No HTTP status code found in response.");
        }
    }
}
