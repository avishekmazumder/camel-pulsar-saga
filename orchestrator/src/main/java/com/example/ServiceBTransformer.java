
package com.example;

import com.example.model.OrchestrationRequest;
import org.springframework.stereotype.Component;

@Component("serviceBTransformer")
public class ServiceBTransformer {
    public String transform(OrchestrationRequest input) {
        return "{\"tag\":\"" + input.getServiceBTag() + "\"}";
    }
}
