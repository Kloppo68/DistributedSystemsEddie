package com.example.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration management for Seller instances with enhanced restocking support.
 *
 * Design Overview:
 * - Loads configuration from properties files with sensible defaults
 * - Supports inventory management with thresholds and restocking
 * - Includes failure simulation parameters for testing
 * - Provides proactive restocking configuration
 *
 * Configuration Categories:
 * - Inventory: Initial stock levels and minimum thresholds
 * - Processing: Average processing time and standard deviation
 * - Failures: Probabilities for various failure scenarios
 * - Restocking: Delays, intervals, and feature toggles
 */
public class SellerConfig {
    // Inventory configuration
    private final Map<String, Integer> initialInventory;      // Product ID → Initial stock level
    private final Map<String, Integer> minThresholds;         // Product ID → Minimum threshold for alerts

    // Processing simulation
    private double averageProcessingTimeMs;                   // Average time to process a request
    private double processingTimeStdDev;                      // Standard deviation for processing time

    // Failure simulation probabilities
    private double crashProbability;                          // Probability of complete crash
    private double noResponseProbability;                     // Probability of processing but no response
    private double processButNoConfirmProbability;            // Probability of processing failure
    private double successProbability;                        // Probability of success (calculated)

    // Inventory management features
    private boolean restockingEnabled;                        // Enable automatic restocking

    // Proactive restocking configuration
    private long minRestockIntervalMs;                        // Minimum time between restocks per product
    private long restockDelayMs;                              // Simulated time for restock to arrive
    private boolean proactiveRestockEnabled;                  // Enable restocking on insufficient inventory

    /**
     * Initialize configuration with sensible defaults.
     *
     * Design Decision: Provide conservative defaults that ensure system stability
     * while allowing customization through properties files. Enable restocking
     * by default for operational flexibility.
     */
    public SellerConfig() {
        this.initialInventory = new HashMap<>();
        this.minThresholds = new HashMap<>();

        // Processing defaults - realistic timing
        this.averageProcessingTimeMs = 1000;      // 1 second average
        this.processingTimeStdDev = 200;          // 200ms standard deviation

        // Failure probabilities - moderate for testing
        this.crashProbability = 0.05;             // 5% crash rate
        this.noResponseProbability = 0.05;        // 5% no response rate
        this.processButNoConfirmProbability = 0.05; // 5% processing failure
        this.successProbability = 0.85;           // 85% success rate

        // Inventory management defaults
        this.restockingEnabled = true;            // Enable restocking for flexibility

        // Proactive restocking defaults
        this.minRestockIntervalMs = 10000;        // 10 seconds between restocks
        this.restockDelayMs = 3000;               // 3 second restock delivery
        this.proactiveRestockEnabled = true;      // Enable proactive restocking
    }

    /**
     * Loads configuration from properties file with fallback to defaults.
     *
     * Design Decision: Support flexible product configuration with automatic
     * threshold calculation. Normalize probabilities to ensure they sum to 1.0.
     * Gracefully handle missing configuration files.
     */
    public static SellerConfig loadFromFile(String filename) {
        SellerConfig config = new SellerConfig();
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream(filename)) {
            props.load(fis);

            // Load inventory configuration
            for (int i = 1; i <= 10; i++) {
                String productKey = "product." + i + ".id";
                String quantityKey = "product." + i + ".quantity";
                String thresholdKey = "product." + i + ".threshold";

                if (props.containsKey(productKey) && props.containsKey(quantityKey)) {
                    String productId = props.getProperty(productKey);
                    int quantity = Integer.parseInt(props.getProperty(quantityKey));
                    config.initialInventory.put(productId, quantity);

                    // Load threshold (default to 20% of initial inventory)
                    int threshold = props.containsKey(thresholdKey) ?
                            Integer.parseInt(props.getProperty(thresholdKey)) :
                            Math.max(1, quantity / 5);
                    config.minThresholds.put(productId, threshold);
                }
            }

            // Load timing configuration
            if (props.containsKey("processing.time.average.ms")) {
                config.averageProcessingTimeMs = Double.parseDouble(
                        props.getProperty("processing.time.average.ms"));
            }

            if (props.containsKey("processing.time.stddev.ms")) {
                config.processingTimeStdDev = Double.parseDouble(
                        props.getProperty("processing.time.stddev.ms"));
            }

            // Load failure probabilities
            if (props.containsKey("failure.crash.probability")) {
                config.crashProbability = Double.parseDouble(
                        props.getProperty("failure.crash.probability"));
            }

            if (props.containsKey("failure.no.response.probability")) {
                config.noResponseProbability = Double.parseDouble(
                        props.getProperty("failure.no.response.probability"));
            }

            if (props.containsKey("failure.process.no.confirm.probability")) {
                config.processButNoConfirmProbability = Double.parseDouble(
                        props.getProperty("failure.process.no.confirm.probability"));
            }

            if (props.containsKey("success.probability")) {
                config.successProbability = Double.parseDouble(
                        props.getProperty("success.probability"));
            }

            // Load inventory management features
            if (props.containsKey("inventory.restocking.enabled")) {
                config.restockingEnabled = Boolean.parseBoolean(
                        props.getProperty("inventory.restocking.enabled"));
            }

            // Load proactive restocking configuration
            if (props.containsKey("restocking.min.interval.ms")) {
                config.minRestockIntervalMs = Long.parseLong(
                        props.getProperty("restocking.min.interval.ms"));
            }

            if (props.containsKey("restocking.delay.ms")) {
                config.restockDelayMs = Long.parseLong(
                        props.getProperty("restocking.delay.ms"));
            }

            if (props.containsKey("restocking.proactive.enabled")) {
                config.proactiveRestockEnabled = Boolean.parseBoolean(
                        props.getProperty("restocking.proactive.enabled"));
            }

        } catch (IOException e) {
            System.err.println("Could not load config file: " + filename + ". Using defaults.");

            // Set default inventory and thresholds
            for (int i = 1; i <= 5; i++) {
                String productId = "PROD-" + i;
                config.initialInventory.put(productId, 10);      // 10 units initial stock
                config.minThresholds.put(productId, 2);          // Alert at 2 units
            }
        }

        // Normalize probabilities to ensure they sum to 1.0
        double totalProb = config.crashProbability + config.noResponseProbability +
                config.processButNoConfirmProbability + config.successProbability;

        if (Math.abs(totalProb - 1.0) > 0.01) {
            System.err.println("Warning: Probabilities don't sum to 1.0. Normalizing...");
            config.crashProbability /= totalProb;
            config.noResponseProbability /= totalProb;
            config.processButNoConfirmProbability /= totalProb;
            config.successProbability /= totalProb;
        }

        return config;
    }

    /**
     * Accessor methods for all configuration values.
     * Provides read-only access to configuration parameters.
     */

    // Inventory getters
    public Map<String, Integer> getInitialInventory() {
        return initialInventory;
    }

    public Map<String, Integer> getMinThresholds() {
        return minThresholds;
    }

    // Processing getters
    public double getAverageProcessingTimeMs() {
        return averageProcessingTimeMs;
    }

    public double getProcessingTimeStdDev() {
        return processingTimeStdDev;
    }

    // Failure probability getters
    public double getCrashProbability() {
        return crashProbability;
    }

    public double getNoResponseProbability() {
        return noResponseProbability;
    }

    public double getProcessButNoConfirmProbability() {
        return processButNoConfirmProbability;
    }

    public double getSuccessProbability() {
        return successProbability;
    }

    // Feature toggle getters
    public boolean isRestockingEnabled() {
        return restockingEnabled;
    }

    // Proactive restocking getters
    public long getMinRestockIntervalMs() {
        return minRestockIntervalMs;
    }

    public long getRestockDelayMs() {
        return restockDelayMs;
    }

    public boolean isProactiveRestockEnabled() {
        return proactiveRestockEnabled;
    }
}