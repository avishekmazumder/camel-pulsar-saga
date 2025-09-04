
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
public class ServiceAApplication {

    Logger logger = LoggerFactory.getLogger(ServiceAApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ServiceAApplication.class, args);
    }

    @PostMapping("/processA")
    public ResponseEntity<String> processA(@RequestBody TagRequest request) {
        logger.info("Processing A with request: " + request.toString());
        if (request.getTag().equals("failA")) {
            throw new RuntimeException("Simulated failure in Service A");
        }

        if (request.getTag().equals("failA1")) {
            return new ResponseEntity<>("Processed A: " + request.getTag(), HttpStatusCode.valueOf(201));
        }
        return new ResponseEntity<>("Processed A: " + request.getTag(), HttpStatusCode.valueOf(200));
    }

    @PostMapping("/compensateA")
    public String compensateA(@RequestBody(required = false) TagRequest request) {
        logger.info("Compensating A with request: " + request.toString());
        return "Compensated A for: " + request.getTag();
    }
}
