
package com.example;

import com.example.model.TagRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.web.bind.annotation.*;

@SpringBootApplication
@RestController
@EnablePulsar
public class ServiceBApplication {

    Logger logger = LoggerFactory.getLogger(ServiceBApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ServiceBApplication.class, args);
    }

    @PostMapping("/processB")
    public ResponseEntity<String> processB(@RequestBody TagRequest request) {
        logger.info("Processing B with request: " + request.toString());

        if (request.getTag().equals("failB")) {
            throw new RuntimeException("Simulated failure in Service B");
        }

        if (request.getTag().equals("failB1")) {
            return new ResponseEntity<>("Processed B: " + request.getTag(), HttpStatusCode.valueOf(201));
        }
        return new ResponseEntity<>("Processed B: " + request.getTag(), HttpStatusCode.valueOf(200));
    }

    @PostMapping("/compensateB")
    public String compensateB(@RequestBody(required = false) TagRequest request) {
        logger.info("Compensating B with request: " + request.toString());
        return "Compensated B for: " + request.getTag();
    }
}
