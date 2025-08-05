package com.example.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration management for Marketplace instances.
 * 
 * Design Overview:
 * - Loads configuration from properties files with sensible defaults
 * - Supports dynamic seller discovery (up to 10 sellers)
 * - Includes timeout and retry parameters for robust operation
 * - Provides simulation parameters for testing system resilience
 * 
 * Configuration Categories:
 * - Seller Endpoints: Network locations of all seller services
 * - Timing Parameters: Timeouts, delays, and retry configurations
 * - Simulation Parameters: Failure rates and network delays for testing
 * 
 * Timeout Strategy:
 * - Base timeout + per-seller timeout = total SAGA timeout
 * - Allows longer timeouts for complex orders involving many sellers
 */
public class MarketplaceConfig {
    // Seller network configuration
    private final Map<String, String> sellerEndpoints;    // Seller ID → ZeroMQ endpoint mapping
    
    // Order generation and timing parameters
    private long orderArrivalDelayMs;                      // Delay between generated orders
    private long sagaTimeoutBaseMs;                        // Base SAGA timeout (all transactions)
    private long sagaTimeoutPerSellerMs;                   // Additional timeout per seller in transaction
    private long sagaCleanupGracePeriodMs;                 // Grace period before cleaning up timed-out SAGAs
    
    // Reliability and retry configuration
    private int maxRetries;                                // Maximum retry attempts per request
    private long responseTimeoutMs;                        // Timeout for individual responses before retry
    
    // Simulation parameters for testing distributed system resilience
    private double sellerCrashProbability;                 // Probability of seller crash simulation
    private double sellerTimeoutProbability;               // Probability of seller timeout simulation
    private double sellerInsufficientInventoryProbability; // Probability of inventory shortage simulation
    private long networkDelayMinMs;                        // Minimum simulated network delay
    private long networkDelayMaxMs;                        // Maximum simulated network delay

    /**
     * Initialize configuration with sensible defaults.
     * 
     * Design Decision: Provide conservative defaults that ensure system stability
     * while allowing customization through properties files. Timeout values are
     * generous to handle realistic network and processing delays.
     */
    public MarketplaceConfig() {
        this.sellerEndpoints = new HashMap<>();
        
        // Conservative timing defaults for reliable operation
        this.orderArrivalDelayMs = 2000;           // 2 seconds between orders
        this.sagaTimeoutBaseMs = 30000;            // 30 second base timeout
        this.sagaTimeoutPerSellerMs = 10000;       // +10 seconds per seller involved
        this.sagaCleanupGracePeriodMs = 60000;     // 60 seconds grace before cleanup
        
        // Retry configuration for transient failures
        this.maxRetries = 3;                       // 3 retry attempts
        this.responseTimeoutMs = 5000;             // 5 second response timeout
        
        // Simulation defaults (moderate failure rates for testing)
        this.sellerCrashProbability = 0.1;        // 10% crash rate
        this.sellerTimeoutProbability = 0.05;     // 5% timeout rate  
        this.sellerInsufficientInventoryProbability = 0.15; // 15% inventory shortage
        this.networkDelayMinMs = 100;             // 100ms minimum network delay
        this.networkDelayMaxMs = 2000;            // 2 second maximum network delay
    }

    /**
     * Loads configuration from properties file with fallback to defaults.
     * 
     * Design Decision: Support up to 10 sellers with automatic discovery.
     * Gracefully handle missing configuration files by providing working defaults.
     * This ensures system can start even with minimal configuration.
     */
    public static MarketplaceConfig loadFromFile(String filename) {
        MarketplaceConfig config = new MarketplaceConfig();
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream(filename)) {
            props.load(fis);

            // Auto-discover seller endpoints (seller.1.id, seller.1.endpoint, etc.)
            for (int i = 1; i <= 10; i++) {
                String sellerKey = "seller." + i + ".id";
                String endpointKey = "seller." + i + ".endpoint";

                if (props.containsKey(sellerKey) && props.containsKey(endpointKey)) {
                    String sellerId = props.getProperty(sellerKey);
                    String endpoint = props.getProperty(endpointKey);
                    config.sellerEndpoints.put(sellerId, endpoint);
                }
            }

            // Load timing configuration
            if (props.containsKey("order.arrival.delay.ms")) {
                config.orderArrivalDelayMs = Long.parseLong(props.getProperty("order.arrival.delay.ms"));
            }

            if (props.containsKey("saga.timeout.base.ms")) {
                config.sagaTimeoutBaseMs = Long.parseLong(props.getProperty("saga.timeout.base.ms"));
            }

            if (props.containsKey("saga.timeout.per.seller.ms")) {
                config.sagaTimeoutPerSellerMs = Long.parseLong(props.getProperty("saga.timeout.per.seller.ms"));
            }

            if (props.containsKey("saga.cleanup.grace.period.ms")) {
                config.sagaCleanupGracePeriodMs = Long.parseLong(props.getProperty("saga.cleanup.grace.period.ms"));
            }

            if (props.containsKey("max.retries")) {
                config.maxRetries = Integer.parseInt(props.getProperty("max.retries"));
            }

            if (props.containsKey("response.timeout.ms")) {
                config.responseTimeoutMs = Long.parseLong(props.getProperty("response.timeout.ms"));
            }

            // Load simulation parameters
            if (props.containsKey("seller.crash.probability")) {
                config.sellerCrashProbability = Double.parseDouble(props.getProperty("seller.crash.probability"));
            }

            if (props.containsKey("seller.timeout.probability")) {
                config.sellerTimeoutProbability = Double.parseDouble(props.getProperty("seller.timeout.probability"));
            }

            if (props.containsKey("seller.insufficient.inventory.probability")) {
                config.sellerInsufficientInventoryProbability = Double.parseDouble(props.getProperty("seller.insufficient.inventory.probability"));
            }

            if (props.containsKey("network.delay.min.ms")) {
                config.networkDelayMinMs = Long.parseLong(props.getProperty("network.delay.min.ms"));
            }

            if (props.containsKey("network.delay.max.ms")) {
                config.networkDelayMaxMs = Long.parseLong(props.getProperty("network.delay.max.ms"));
            }

        } catch (IOException e) {
            System.err.println("Could not load config file: " + filename);
            
            // Provide working defaults for development and testing
            config.sellerEndpoints.put("SELLER-1", "tcp://localhost:5561");
            config.sellerEndpoints.put("SELLER-2", "tcp://localhost:5562");
            config.sellerEndpoints.put("SELLER-3", "tcp://localhost:5563");
            config.sellerEndpoints.put("SELLER-4", "tcp://localhost:5564");
            config.sellerEndpoints.put("SELLER-5", "tcp://localhost:5565");
        }

        return config;
    }

    /**
     * Accessor methods for configuration values.
     * Provides read-only access to all configuration parameters.
     */
    public Map<String, String> getSellerEndpoints() {
        return sellerEndpoints;
    }

    public long getOrderArrivalDelayMs() {
        return orderArrivalDelayMs;
    }

    public long getSagaTimeoutBaseMs() {
        return sagaTimeoutBaseMs;
    }

    public long getSagaTimeoutPerSellerMs() {
        return sagaTimeoutPerSellerMs;
    }

    public long getSagaCleanupGracePeriodMs() {
        return sagaCleanupGracePeriodMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getResponseTimeoutMs() {
        return responseTimeoutMs;
    }

    public double getSellerCrashProbability() {
        return sellerCrashProbability;
    }

    public double getSellerTimeoutProbability() {
        return sellerTimeoutProbability;
    }

    public double getSellerInsufficientInventoryProbability() {
        return sellerInsufficientInventoryProbability;
    }

    public long getNetworkDelayMinMs() {
        return networkDelayMinMs;
    }

    public long getNetworkDelayMaxMs() {
        return networkDelayMaxMs;
    }

    /**
     * Calculates dynamic SAGA timeout based on transaction complexity.
     * 
     * Design Decision: Scale timeout with number of sellers involved to account for
     * increased coordination overhead and network round-trips in complex transactions.
     * 
     * @param sellerCount Number of sellers involved in the transaction
     * @return Total timeout in milliseconds
     */
    public long calculateSagaTimeout(int sellerCount) {
        return sagaTimeoutBaseMs + (sellerCount * sagaTimeoutPerSellerMs);
    }
}