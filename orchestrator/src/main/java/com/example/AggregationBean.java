
package com.example;

import org.springframework.stereotype.Component;

@Component("aggregator")
public class AggregationBean {
    public String aggregate(String responseA, String responseB) {
        if (responseB.contains("B1")) {
            throw new RuntimeException("Simulate Saga failure");
        }
        return "Aggregated Response -> [" + responseA + "] & [" + responseB + "]";
    }
}
