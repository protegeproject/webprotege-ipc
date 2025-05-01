package edu.stanford.protege.webprotege.ipc.util;

import org.slf4j.MDC;

public class CorrelationMDCUtil {
    public static final String CORRELATION_ID_KEY = "correlationId";
    
    public static void setCorrelationId(String correlationId) {
        if (correlationId != null) {
            MDC.put(CORRELATION_ID_KEY, correlationId);
        }
    }
    
    public static void clearCorrelationId() {
        MDC.remove(CORRELATION_ID_KEY);
    }
    
    public static String getCorrelationId() {
        return MDC.get(CORRELATION_ID_KEY);
    }
} 