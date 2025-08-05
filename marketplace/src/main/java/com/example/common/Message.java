package com.example.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Message protocol for marketplace-seller communication.
 * 
 * Design Overview:
 * - Supports both request and response messages in SAGA pattern
 * - Includes SAGA correlation ID for transaction tracking
 * - Simple JSON serialization for cross-service communication
 * - Extensible metadata field for future enhancements
 * 
 * Message Types:
 * - RESERVE_REQUEST/RESPONSE: Phase 1 of two-phase commit
 * - CONFIRM_REQUEST/RESPONSE: Phase 2 of two-phase commit  
 * - CANCEL_REQUEST/RESPONSE: Compensation phase for rollback
 * 
 * Note: Uses simplified JSON parsing to minimize external dependencies
 * as required by assignment constraints.
 */
public class Message {
    // Core message fields
    private MessageType type;           // Message type (request/response)
    private String sagaId;              // SAGA transaction correlation ID
    private String productId;           // Product being requested (for requests)
    private int quantity;               // Quantity requested (for requests)
    private boolean success;            // Success indicator (for responses)
    private String errorMessage;        // Error details (for failed responses)
    private Order order;                // Order data (for ORDER_REQUEST messages)
    private String sagaStatus;          // Final SAGA status (COMPLETED, ROLLED_BACK, TIMED_OUT)
    private String failureReason;       // Detailed reason for SAGA failures
    private long timestamp;             // Timestamp of SAGA completion
    private Map<String, Object> metadata; // Extensible metadata for future use

    /** Default constructor for JSON deserialization */
    public Message() {
        this.metadata = new HashMap<>();
    }

    /** Constructor for request messages (RESERVE, CONFIRM, CANCEL) */
    public Message(MessageType type, String sagaId, String productId, int quantity) {
        this();
        this.type = type;
        this.sagaId = sagaId;
        this.productId = productId;
        this.quantity = quantity;
    }

    /** Constructor for response messages (success/failure indication) */
    public Message(MessageType type, String sagaId, boolean success, String errorMessage) {
        this();
        this.type = type;
        this.sagaId = sagaId;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    /** Constructor for SAGA completion messages */
    public Message(MessageType type, String sagaId, String sagaStatus, String failureReason) {
        this();
        this.type = type;
        this.sagaId = sagaId;
        this.sagaStatus = sagaStatus;
        this.failureReason = failureReason;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Serializes message to JSON string.
     * 
     * Design Decision: Use simple string concatenation to avoid external JSON library
     * dependencies as required by assignment constraints. In production, would use
     * Jackson or Gson for robust JSON handling.
     */
    public String toJson() {
        StringBuilder json = new StringBuilder("{");
        json.append("\"type\":\"").append(type).append("\",");
        json.append("\"sagaId\":\"").append(sagaId).append("\",");

        // Include product details for request messages
        if (productId != null) {
            json.append("\"productId\":\"").append(productId).append("\",");
        }

        json.append("\"quantity\":").append(quantity).append(",");
        json.append("\"success\":").append(success);

        // Include error message for failed responses
        if (errorMessage != null) {
            json.append(",\"errorMessage\":\"").append(errorMessage).append("\"");
        }

        // Include order object for ORDER_REQUEST messages
        if (order != null) {
            json.append(",\"order\":").append(order.toJson());
        }

        // Include SAGA status fields for SAGA completion messages
        if (sagaStatus != null) {
            json.append(",\"sagaStatus\":\"").append(sagaStatus).append("\"");
        }
        if (failureReason != null) {
            json.append(",\"failureReason\":\"").append(failureReason).append("\"");
        }
        if (timestamp > 0) {
            json.append(",\"timestamp\":").append(timestamp);
        }

        json.append("}");
        return json.toString();
    }

    /**
     * Deserializes JSON string to Message object.
     * 
     * Design Decision: Use simple string parsing to avoid external dependencies.
     * This is sufficient for the controlled message format used in this system.
     * In production, would use proper JSON library for robust parsing.
     */
    public static Message fromJson(String json) {
        Message msg = new Message();

        // Handle order field separately since it's a nested JSON object
        String orderJson = null;
        int orderStart = json.indexOf("\"order\":");
        if (orderStart != -1) {
            // Find the opening brace of the order object
            int orderObjStart = json.indexOf("{", orderStart);
            if (orderObjStart != -1) {
                // Find the matching closing brace
                int braceCount = 0;
                int orderObjEnd = orderObjStart;
                for (int i = orderObjStart; i < json.length(); i++) {
                    if (json.charAt(i) == '{') {
                        braceCount++;
                    } else if (json.charAt(i) == '}') {
                        braceCount--;
                        if (braceCount == 0) {
                            orderObjEnd = i;
                            break;
                        }
                    }
                }
                orderJson = json.substring(orderObjStart, orderObjEnd + 1);
                // Remove the order part from the main JSON for simple parsing
                json = json.substring(0, orderStart - 1) + json.substring(orderObjEnd + 1);
            }
        }

        // Simple parsing for non-nested fields - remove braces and split by commas
        String[] parts = json.replace("{", "").replace("}", "").split(",");

        for (String part : parts) {
            String[] keyValue = part.split(":");
            if (keyValue.length == 2) {
                String key = keyValue[0].replace("\"", "").trim();
                String value = keyValue[1].replace("\"", "").trim();

                // Parse each field based on its type
                switch (key) {
                    case "type":
                        msg.type = MessageType.valueOf(value);
                        break;
                    case "sagaId":
                        msg.sagaId = value;
                        break;
                    case "productId":
                        msg.productId = value;
                        break;
                    case "quantity":
                        msg.quantity = Integer.parseInt(value);
                        break;
                    case "success":
                        msg.success = Boolean.parseBoolean(value);
                        break;
                    case "errorMessage":
                        msg.errorMessage = value;
                        break;
                    case "sagaStatus":
                        msg.sagaStatus = value;
                        break;
                    case "failureReason":
                        msg.failureReason = value;
                        break;
                    case "timestamp":
                        msg.timestamp = Long.parseLong(value);
                        break;
                }
            }
        }

        // Parse the order object if it exists
        if (orderJson != null) {
            msg.order = Order.fromJson(orderJson);
        }

        return msg;
    }

    /**
     * Standard getters and setters for message fields.
     * Provides access to all message properties for serialization and business logic.
     */
    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public String getSagaStatus() {
        return sagaStatus;
    }

    public void setSagaStatus(String sagaStatus) {
        this.sagaStatus = sagaStatus;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}