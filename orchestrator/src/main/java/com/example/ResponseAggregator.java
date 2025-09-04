
package com.example;

import com.example.model.OrchestrationResponse;
import org.springframework.stereotype.Component;

@Component("responseAggregator")
public class ResponseAggregator {
    public OrchestrationResponse aggregate(String a, String b) {
        if (b.contains("tagB1")) {
            throw new RuntimeException("Simulate Saga failure from response aggregator");
        }
        return new OrchestrationResponse(a, b);
    }
}
