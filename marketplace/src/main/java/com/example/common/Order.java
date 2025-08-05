package com.example.common;

import java.util.List;
import java.util.ArrayList;

public class Order {
    private String customerId;
    private List<OrderItem> items;
    private long timestamp;

    public Order() {
        this.items = new ArrayList<>();
        this.timestamp = System.currentTimeMillis();
    }

    public Order(String customerId, List<OrderItem> items) {
        this.customerId = customerId;
        this.items = new ArrayList<>(items);
        this.timestamp = System.currentTimeMillis();
    }

    // Getters and setters
    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public List<OrderItem> getItems() {
        return items;
    }

    public void setItems(List<OrderItem> items) {
        this.items = items;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Serializes this Order to JSON string.
     * Uses simple string concatenation to avoid external dependencies.
     */
    public String toJson() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"customerId\":\"").append(customerId).append("\",");
        json.append("\"timestamp\":").append(timestamp).append(",");
        json.append("\"items\":[");
        
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append(items.get(i).toJson());
        }
        
        json.append("]}");
        return json.toString();
    }

    /**
     * Deserializes JSON string to Order object.
     * Uses simple string parsing to avoid external dependencies.
     */
    public static Order fromJson(String json) {
        Order order = new Order();
        
        // Extract the items array separately
        int itemsStart = json.indexOf("\"items\":[") + 9;
        int itemsEnd = json.lastIndexOf("]");
        String itemsJson = json.substring(itemsStart, itemsEnd);
        
        // Parse main order fields
        String mainPart = json.substring(0, itemsStart - 9);
        String[] parts = mainPart.replace("{", "").split(",");
        
        for (String part : parts) {
            String[] keyValue = part.split(":");
            if (keyValue.length == 2) {
                String key = keyValue[0].replace("\"", "").trim();
                String value = keyValue[1].replace("\"", "").trim();
                
                switch (key) {
                    case "customerId":
                        order.customerId = value;
                        break;
                    case "timestamp":
                        order.timestamp = Long.parseLong(value);
                        break;
                }
            }
        }
        
        // Parse items array
        if (!itemsJson.trim().isEmpty()) {
            // Split by },{ to separate individual items
            String[] itemParts = itemsJson.split("\\},\\{");
            for (String itemPart : itemParts) {
                // Add back braces if they were removed by split
                if (!itemPart.startsWith("{")) {
                    itemPart = "{" + itemPart;
                }
                if (!itemPart.endsWith("}")) {
                    itemPart = itemPart + "}";
                }
                
                OrderItem item = OrderItem.fromJson(itemPart);
                order.items.add(item);
            }
        }
        
        return order;
    }

    @Override
    public String toString() {
        return String.format("Order{customerId='%s', items=%d, timestamp=%d}",
                customerId, items.size(), timestamp);
    }
}
