package com.example.model;

public class CommandMessage {
    private String correlationId;
    private String targetService; // "A" or "B"
    private String action;        // "PROCESS" or "COMPENSATE"
    private TagRequest payload;

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getTargetService() { return targetService; }
    public void setTargetService(String targetService) { this.targetService = targetService; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public TagRequest getPayload() { return payload; }
    public void setPayload(TagRequest payload) { this.payload = payload; }
}
