package com.example.common;

public class OrderItem {
    private String sellerId;
    private String productId;
    private int quantity;

    public OrderItem() {}

    public OrderItem(String sellerId, String productId, int quantity) {
        this.sellerId = sellerId;
        this.productId = productId;
        this.quantity = quantity;
    }

    // Getters and setters
    public String getSellerId() {
        return sellerId;
    }

    public void setSellerId(String sellerId) {
        this.sellerId = sellerId;
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

    /**
     * Serializes this OrderItem to JSON string.
     * Uses simple string concatenation to avoid external dependencies.
     */
    public String toJson() {
        return String.format("{\"sellerId\":\"%s\",\"productId\":\"%s\",\"quantity\":%d}", 
                sellerId, productId, quantity);
    }

    /**
     * Deserializes JSON string to OrderItem object.
     * Uses simple string parsing to avoid external dependencies.
     */
    public static OrderItem fromJson(String json) {
        OrderItem item = new OrderItem();
        
        // Simple parsing - remove braces and split by commas
        String[] parts = json.replace("{", "").replace("}", "").split(",");
        
        for (String part : parts) {
            String[] keyValue = part.split(":");
            if (keyValue.length == 2) {
                String key = keyValue[0].replace("\"", "").trim();
                String value = keyValue[1].replace("\"", "").trim();
                
                switch (key) {
                    case "sellerId":
                        item.sellerId = value;
                        break;
                    case "productId":
                        item.productId = value;
                        break;
                    case "quantity":
                        item.quantity = Integer.parseInt(value);
                        break;
                }
            }
        }
        
        return item;
    }

    @Override
    public String toString() {
        return String.format("OrderItem{sellerId='%s', productId='%s', quantity=%d}",
            sellerId, productId, quantity);
    }
}