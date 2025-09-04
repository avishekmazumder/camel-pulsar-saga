
package com.example.model;

public class OrchestrationResponse {
    private String responseA;
    private String responseB;

    public OrchestrationResponse(String responseA, String responseB) {
        this.responseA = responseA;
        this.responseB = responseB;
    }

    public String getResponseA() {
        return responseA;
    }

    public void setResponseA(String responseA) {
        this.responseA = responseA;
    }

    public String getResponseB() {
        return responseB;
    }

    public void setResponseB(String responseB) {
        this.responseB = responseB;
    }

    @Override
    public String toString() {
        return "OrchestrationResponse{" +
                "responseA='" + responseA + '\'' +
                ", responseB='" + responseB + '\'' +
                '}';
    }
}
