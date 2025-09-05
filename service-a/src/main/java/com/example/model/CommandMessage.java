package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommandMessage {
    private String correlationId;
    private String targetService; // "A" or "B"
    private String action;        // "PROCESS" or "COMPENSATE"
    private TagRequest payload;
}
