package com.example.model;

public class ReplyMessage {
    private String correlationId;
    private String service; // "A" or "B"
    private int status;
    private String message;
    private String error;

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getService() { return service; }
    public void setService(String service) { this.service = service; }
    public int getStatus() { return status; }
    public void setStatus(int status) { this.status = status; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
