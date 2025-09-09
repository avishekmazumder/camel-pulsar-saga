
package com.example;

import com.example.model.OrchestrationRequest;
import com.example.model.OrchestrationResponse;
import com.example.util.PendingSagaRegistry;
import org.apache.camel.ProducerTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.UUID;

@RestController
@RequestMapping("/orchestrate")
public class OrchestrationController {

    private final ProducerTemplate producerTemplate;
    private final PendingSagaRegistry pending;

    public OrchestrationController(ProducerTemplate producerTemplate, PendingSagaRegistry pending) {
        this.producerTemplate = producerTemplate;
        this.pending = pending;
    }

    @PostMapping("/mq")
    public DeferredResult<OrchestrationResponse> orchestrateMq(@RequestBody OrchestrationRequest request) {
        String cid = UUID.randomUUID().toString();

        // 1) register async holder
        DeferredResult<OrchestrationResponse> dr =
                pending.register(cid, 30000); // 30s timeout (match route REPLY_TIMEOUT_MS)

        // 2) start saga asynchronously; route will read property 'cid'
        producerTemplate.send("direct:startSagaMq", e -> {
            e.getIn().setBody(request);
            e.setProperty("cid", cid);
        });

        return dr;
    }

    // Optional: keep your older synchronous endpoint if you like under another path
    @PostMapping
    public ResponseEntity<Object> orchestrateSync(@RequestBody OrchestrationRequest request) {
        // For brevity, route not shown; keep existing if needed
        return ResponseEntity.accepted().body("Use /orchestrate/mq for async");
    }
}
