package com.example.model;

import java.io.Serializable;

public class CommandMessage implements Serializable {
    private String correlationId;
    private String targetService; // "A" or "B"
    private String action;        // "PROCESS" or "COMPENSATE"
    private TagRequest payload;

    public CommandMessage(String correlationId, String targetService, String action, TagRequest payload) {
        this.correlationId = correlationId;
        this.targetService = targetService;
        this.action = action;
        this.payload = payload;
    }

    public CommandMessage() {
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getTargetService() { return targetService; }
    public void setTargetService(String targetService) { this.targetService = targetService; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public TagRequest getPayload() { return payload; }
    public void setPayload(TagRequest payload) { this.payload = payload; }
}
