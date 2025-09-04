
package com.example;

import com.example.model.OrchestrationRequest;
import org.springframework.stereotype.Component;

@Component("serviceATransformer")
public class ServiceATransformer {
    public String transform(OrchestrationRequest input) {
        return "{\"tag\":\"" + input.getServiceATag() + "\"}";
    }
}
