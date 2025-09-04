
package com.example;

import org.springframework.stereotype.Component;

@Component("transformer")
public class TransformationBean {
    public String transform(String input) {
        return "Transformed: " + input;
    }
}
