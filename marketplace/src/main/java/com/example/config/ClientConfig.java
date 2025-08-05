package com.example.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Configuration class for Client service instances.
 * 
 * Manages client behavior parameters including:
 * - Order generation patterns and timing
 * - Marketplace connection endpoints
 * - Order complexity and customer behavior simulation
 * - Load testing and concurrent client support
 */
public class ClientConfig {
    private static final Logger logger = Logger.getLogger(ClientConfig.class.getName());
    
    // Default configuration values
    private static final int DEFAULT_ORDER_INTERVAL_MS = 2000;
    private static final int DEFAULT_ORDER_TIMEOUT_MS = 30000;
    private static final int DEFAULT_MAX_ITEMS_PER_ORDER = 3;
    private static final int DEFAULT_MAX_QUANTITY_PER_ITEM = 3;
    private static final int DEFAULT_CONCURRENT_ORDERS = 1;
    private static final int DEFAULT_TOTAL_ORDERS = 100;
    
    // Core configuration
    private int orderIntervalMs = DEFAULT_ORDER_INTERVAL_MS;
    private int orderTimeoutMs = DEFAULT_ORDER_TIMEOUT_MS;
    private int maxItemsPerOrder = DEFAULT_MAX_ITEMS_PER_ORDER;
    private int maxQuantityPerItem = DEFAULT_MAX_QUANTITY_PER_ITEM;
    private int concurrentOrders = DEFAULT_CONCURRENT_ORDERS;
    private int totalOrders = DEFAULT_TOTAL_ORDERS;
    
    // Marketplace endpoints
    private Map<String, String> marketplaceEndpoints = new HashMap<>();
    
    // Customer behavior simulation
    private List<String> customerIds = new ArrayList<>();
    private List<String> productIds = new ArrayList<>();
    
    // Load testing parameters
    private boolean loadTestingEnabled = false;
    private int loadTestDurationSeconds = 60;
    private int loadTestClientsCount = 5;
    
    public static ClientConfig loadFromFile(String filename) {
        ClientConfig config = new ClientConfig();
        Properties props = new Properties();
        
        try (FileInputStream fis = new FileInputStream(filename)) {
            props.load(fis);
            config.loadFromProperties(props);
            logger.info("Loaded client configuration from " + filename);
        } catch (IOException e) {
            logger.warning("Could not load client config from " + filename + ", using defaults: " + e.getMessage());
            config.loadDefaults();
        }
        
        return config;
    }
    
    private void loadFromProperties(Properties props) {
        // Order generation settings
        orderIntervalMs = Integer.parseInt(props.getProperty("order.interval.ms", String.valueOf(DEFAULT_ORDER_INTERVAL_MS)));
        orderTimeoutMs = Integer.parseInt(props.getProperty("order.timeout.ms", String.valueOf(DEFAULT_ORDER_TIMEOUT_MS)));
        maxItemsPerOrder = Integer.parseInt(props.getProperty("order.max.items", String.valueOf(DEFAULT_MAX_ITEMS_PER_ORDER)));
        maxQuantityPerItem = Integer.parseInt(props.getProperty("order.max.quantity", String.valueOf(DEFAULT_MAX_QUANTITY_PER_ITEM)));
        concurrentOrders = Integer.parseInt(props.getProperty("order.concurrent", String.valueOf(DEFAULT_CONCURRENT_ORDERS)));
        totalOrders = Integer.parseInt(props.getProperty("order.total", String.valueOf(DEFAULT_TOTAL_ORDERS)));
        
        // Load testing settings
        loadTestingEnabled = Boolean.parseBoolean(props.getProperty("load.testing.enabled", "false"));
        loadTestDurationSeconds = Integer.parseInt(props.getProperty("load.testing.duration.seconds", "60"));
        loadTestClientsCount = Integer.parseInt(props.getProperty("load.testing.clients.count", "5"));
        
        // Load marketplace endpoints
        loadMarketplaceEndpoints(props);
        
        // Load customer and product lists
        loadCustomerIds(props);
        loadProductIds(props);
    }
    
    private void loadDefaults() {
        // Default marketplace endpoints
        marketplaceEndpoints.put("MARKETPLACE-1", "tcp://localhost:5555");
        marketplaceEndpoints.put("MARKETPLACE-2", "tcp://localhost:5556");
        
        // Default customer IDs
        for (int i = 1; i <= 100; i++) {
            customerIds.add("CUST-" + i);
        }
        
        // Default product IDs
        for (int i = 1; i <= 5; i++) {
            productIds.add("PROD-" + i);
        }
    }
    
    private void loadMarketplaceEndpoints(Properties props) {
        marketplaceEndpoints.clear();
        
        // Load marketplace endpoints from properties
        int index = 1;
        while (true) {
            String marketplaceKey = "marketplace." + index + ".id";
            String endpointKey = "marketplace." + index + ".endpoint";
            
            String marketplaceId = props.getProperty(marketplaceKey);
            String endpoint = props.getProperty(endpointKey);
            
            if (marketplaceId == null || endpoint == null) {
                break;
            }
            
            marketplaceEndpoints.put(marketplaceId, endpoint);
            index++;
        }
        
        // If no marketplaces configured, use defaults
        if (marketplaceEndpoints.isEmpty()) {
            marketplaceEndpoints.put("MARKETPLACE-1", "tcp://localhost:5555");
            marketplaceEndpoints.put("MARKETPLACE-2", "tcp://localhost:5556");
        }
    }
    
    private void loadCustomerIds(Properties props) {
        customerIds.clear();
        
        // Load customer IDs from properties
        String customerList = props.getProperty("customers");
        if (customerList != null) {
            String[] customers = customerList.split(",");
            for (String customer : customers) {
                customerIds.add(customer.trim());
            }
        }
        
        // If no customers configured, use defaults
        if (customerIds.isEmpty()) {
            for (int i = 1; i <= 100; i++) {
                customerIds.add("CUST-" + i);
            }
        }
    }
    
    private void loadProductIds(Properties props) {
        productIds.clear();
        
        // Load product IDs from properties
        String productList = props.getProperty("products");
        if (productList != null) {
            String[] products = productList.split(",");
            for (String product : products) {
                productIds.add(product.trim());
            }
        }
        
        // If no products configured, use defaults
        if (productIds.isEmpty()) {
            for (int i = 1; i <= 5; i++) {
                productIds.add("PROD-" + i);
            }
        }
    }
    
    // Getters
    public int getOrderIntervalMs() { return orderIntervalMs; }
    public int getOrderTimeoutMs() { return orderTimeoutMs; }
    public int getMaxItemsPerOrder() { return maxItemsPerOrder; }
    public int getMaxQuantityPerItem() { return maxQuantityPerItem; }
    public int getConcurrentOrders() { return concurrentOrders; }
    public int getTotalOrders() { return totalOrders; }
    
    public Map<String, String> getMarketplaceEndpoints() { return marketplaceEndpoints; }
    public List<String> getCustomerIds() { return customerIds; }
    public List<String> getProductIds() { return productIds; }
    
    public boolean isLoadTestingEnabled() { return loadTestingEnabled; }
    public int getLoadTestDurationSeconds() { return loadTestDurationSeconds; }
    public int getLoadTestClientsCount() { return loadTestClientsCount; }
}