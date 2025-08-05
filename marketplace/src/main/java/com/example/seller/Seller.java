package com.example.seller;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.example.common.*;
import com.example.config.SellerConfig;

/**
 * Distributed Seller service implementing inventory management and order processing.
 *
 * Architecture Overview:
 * - Manages local product inventory with thread-safe operations
 * - Processes marketplace requests (reserve, confirm, cancel)
 * - Simulates realistic failure scenarios for testing distributed system resilience
 * - Uses ZeroMQ ROUTER socket for handling multiple marketplace connections
 * - Implements proactive restocking based on demand patterns
 *
 * Business Logic:
 * - Maintains limited inventory that can be exhausted
 * - Supports reservation-confirmation pattern for atomic transactions
 * - Implements configurable failure simulation for testing
 * - Ensures inventory consistency under concurrent access
 * - Proactively restocks based on unfulfilled demand
 *
 * Threading Model:
 * - Single-threaded event loop for message processing
 * - Individual request handler threads for parallel processing
 * - Thread-safe inventory operations using synchronized blocks
 * - Asynchronous restocking operations
 */
public class Seller {
    private static final Logger logger = Logger.getLogger(Seller.class.getName());

    // Core identification and configuration
    private final String sellerId;                          // Unique identifier for this seller instance
    private final SellerConfig config;                      // Configuration loaded from properties file

    // ZeroMQ communication infrastructure
    private final ZContext context;                         // ZeroMQ context for socket management
    private final ZMQ.Socket socket;                        // ROUTER socket for marketplace connections

    // Business state management
    private final Map<String, Integer> inventory;           // Product ID → Available quantity
    private final Map<String, ReservedItem> reservations;   // SAGA ID → Reserved items (temporary holds)
    private final Map<String, Long> lastRestockTime;        // Product ID → Last restock timestamp (rate limiting)

    // Concurrency control - ensures inventory consistency
    private final Object inventoryLock = new Object();      // Guards inventory modifications
    private final Object reservationLock = new Object();    // Guards reservation operations

    // Virtual Threading infrastructure
    private final ThreadLocal<Thread> requestThreads = new ThreadLocal<>(); // Track request handler threads
    private volatile boolean shutdown = false;              // Graceful shutdown flag
    private Thread messageHandlerThread;                    // Main message processing thread

    // Utilities
    private final Random random;                            // For failure simulation and processing delays

    /**
     * Initializes a new Seller instance with inventory and networking.
     *
     * Design Decision: Use ROUTER socket to handle multiple marketplace connections
     * simultaneously. Initialize inventory and start message processing immediately
     * to ensure seller is ready for requests as soon as construction completes.
     */
    public Seller(String sellerId, String endpoint, SellerConfig config) {
        this.sellerId = sellerId;
        this.config = config;
        this.context = new ZContext();
        this.socket = context.createSocket(SocketType.ROUTER); // Handles multiple clients
        this.inventory = new ConcurrentHashMap<>();            // Thread-safe inventory storage
        this.reservations = new ConcurrentHashMap<>();         // Thread-safe reservation tracking
        this.lastRestockTime = new ConcurrentHashMap<>();      // Track restock timing
        this.random = new Random();

        // Initialize business state
        initializeInventory();

        // Bind to network endpoint - seller becomes available
        socket.bind(endpoint);
        logger.info(String.format("Seller %s listening on %s", sellerId, endpoint));

        // Start processing marketplace requests
        startMessageHandler();
    }

    /**
     * Initializes product inventory from configuration.
     *
     * Design Decision: Load inventory from configuration to enable testing different
     * scenarios (high/low stock, specific product availability). Default to 10 units
     * if not configured to ensure basic functionality.
     */
    private void initializeInventory() {
        // Initialize standard product catalog (PROD-1 through PROD-5)
        for (int i = 1; i <= 5; i++) {
            String productId = "PROD-" + i;

            // Load from config or use sensible default
            int quantity = config.getInitialInventory().getOrDefault(productId, 10);
            inventory.put(productId, quantity);

            logger.info(String.format("Initialized %s with %d units", productId, quantity));
        }
    }

    /**
     * Starts the main message processing loop for handling marketplace requests.
     *
     * Design Decision: Process each request in a separate thread to prevent any single
     * request from blocking others. This supports the non-blocking requirement and
     * enables high throughput under concurrent load from multiple marketplaces.
     *
     * Threading Strategy: Main event loop + individual request handlers
     * ensures responsiveness and parallel processing capability.
     */
    private void startMessageHandler() {
        messageHandlerThread = Thread.ofVirtual().name("MessageHandler-" + sellerId).start(() -> {
            while (!Thread.currentThread().isInterrupted() && !shutdown) {
                try {
                    // Blocking receive - waits for marketplace requests
                    ZMsg msg = ZMsg.recvMsg(socket);
                    if (msg != null) {
                        // Process each request in separate virtual thread for parallelism
                        Thread requestThread = Thread.ofVirtual()
                            .name("RequestHandler-" + sellerId + "-" + System.currentTimeMillis())
                            .start(() -> {
                                try {
                                    handleMessage(msg);
                                } finally {
                                    // Cleanup thread reference to prevent memory leaks
                                    requestThreads.remove();
                                }
                            });
                        requestThreads.set(requestThread);
                    }
                } catch (org.zeromq.ZMQException e) {
                    if (!shutdown) {
                        logger.log(Level.SEVERE, "ZMQ error in message handler", e);
                    }
                    break; // Exit on ZMQ errors
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Unexpected error in message handler", e);
                    break; // Exit on unexpected errors
                }
            }
            logger.info("Message handler thread stopped for " + sellerId);
        });
    }

    /**
     * Processes individual marketplace requests with failure simulation.
     *
     * Design Decision: Implement realistic processing delays and configurable failure
     * scenarios to test distributed system resilience. Process failures before business
     * logic to simulate real-world conditions where systems can fail at any point.
     */
    private void handleMessage(ZMsg zmsg) {
        try {
            // Extract marketplace identity for response routing
            byte[] identity = zmsg.pop().getData();
            String content = new String(zmsg.getLast().getData());

            // Parse marketplace request
            Message request = Message.fromJson(content);
            logger.info(String.format("Received %s for saga %s", request.getType(), request.getSagaId()));

            // Simulate realistic processing time with normal distribution
            simulateProcessingDelay();

            // Simulate various failure scenarios for testing system resilience
            if (shouldSimulateFailure()) {
                handleSimulatedFailure(identity, request);
                return; // Exit early on simulated failure
            }

            // Route to appropriate business logic handler
            switch (request.getType()) {
                case RESERVE_REQUEST:
                    handleReserveRequest(identity, request);
                    break;
                case CONFIRM_REQUEST:
                    handleConfirmRequest(identity, request);
                    break;
                case CANCEL_REQUEST:
                    handleCancelRequest(identity, request);
                    break;
                case RESTOCK_REQUEST:
                    handleRestockRequest(identity, request);
                    break;
                default:
                    logger.warning("Unknown request type: " + request.getType());
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling message", e);
        }
    }

    /**
     * Enhanced reservation handler with proactive restocking on insufficient inventory.
     *
     * Design Decision: Trigger restocking when demand exceeds supply, even if current
     * stock is above threshold. This prevents repeated failures for large orders while
     * maintaining deadlock safety through immediate failure response.
     */
    private void handleReserveRequest(byte[] identity, Message request) {
        String productId = request.getProductId();
        int quantity = request.getQuantity();
        String sagaId = request.getSagaId();

        // Critical section: atomic inventory check and reservation
        // Lock ordering prevents deadlocks: always inventory lock first, then reservation lock
        synchronized (inventoryLock) {
            synchronized (reservationLock) {
                Integer available = inventory.get(productId);

                // Validate product exists
                if (available == null) {
                    sendResponse(identity, new Message(
                            MessageType.RESERVE_RESPONSE,
                            sagaId,
                            false,
                            "Product not found: " + productId
                    ));
                    return;
                }

                // Check if sufficient inventory available
                if (available >= quantity) {
                    // Atomically reserve inventory - reduce available and record reservation
                    inventory.put(productId, available - quantity);
                    reservations.put(sagaId, new ReservedItem(productId, quantity));

                    logger.info(String.format("Reserved %d units of %s for saga %s. Remaining: %d",
                            quantity, productId, sagaId, available - quantity));

                    // Monitor inventory levels for operational alerts
                    checkLowStockAlert(productId);

                    // Handle out-of-stock scenarios if restocking disabled
                    checkOutOfStockShutdown(productId);

                    // Send success response to marketplace
                    sendResponse(identity, new Message(
                            MessageType.RESERVE_RESPONSE,
                            sagaId,
                            true,
                            null
                    ));
                } else {
                    // Insufficient inventory - business failure, no partial fulfillment
                    logger.warning(String.format("Insufficient inventory for %s. Requested: %d, Available: %d",
                            productId, quantity, available));

                    // NEW: Trigger proactive restocking for unfulfilled demand
                    triggerProactiveRestock(productId, quantity - available);

                    // Still fail immediately to prevent deadlock
                    sendResponse(identity, new Message(
                            MessageType.RESERVE_RESPONSE,
                            sagaId,
                            false,
                            "Insufficient inventory"
                    ));
                }
            }
        }
    }

    /**
     * Triggers proactive restocking when demand exceeds current inventory.
     *
     * Design Principles:
     * - Non-blocking: Restocking happens asynchronously
     * - Smart quantity: Restock based on actual demand, not fixed amounts
     * - Rate limiting: Prevent restock storms from repeated failures
     */
    private void triggerProactiveRestock(String productId, int shortfall) {
        if (!config.isRestockingEnabled()) {
            return; // Respect configuration
        }

        // Prevent restock storms - track last restock time per product
        Long lastRestock = lastRestockTime.get(productId);
        long now = System.currentTimeMillis();

        if (lastRestock != null && (now - lastRestock) < config.getMinRestockIntervalMs()) {
            logger.fine(String.format("Skipping restock for %s - too soon after last restock", productId));
            return;
        }

        // Calculate intelligent restock quantity
        Integer currentStock = inventory.get(productId);
        Integer threshold = config.getMinThresholds().get(productId);
        Integer initialStock = config.getInitialInventory().get(productId);

        // Restock to handle current shortfall + buffer to threshold
        int restockQuantity = shortfall + (threshold != null ? threshold : 5);

        // Option: Cap at initial inventory level to prevent over-stocking
        if (initialStock != null && currentStock != null && currentStock + restockQuantity > initialStock) {
            restockQuantity = initialStock - currentStock;
        }

        logger.info(String.format("PROACTIVE RESTOCK: Ordering %d units of %s due to insufficient inventory (shortfall: %d)",
                restockQuantity, productId, shortfall));

        // Make variables effectively final for lambda
        final String finalProductId = productId;
        final int finalRestockQuantity = restockQuantity;
        
        // Asynchronous restock simulation using virtual threads
        CompletableFuture.runAsync(() -> {
            try {
                // Simulate restock delay
                Thread.sleep(config.getRestockDelayMs());

                // Add inventory
                synchronized (inventoryLock) {
                    inventory.merge(finalProductId, finalRestockQuantity, Integer::sum);
                    lastRestockTime.put(finalProductId, System.currentTimeMillis());

                    logger.info(String.format("RESTOCK COMPLETE: Added %d units of %s. New total: %d",
                            finalRestockQuantity, finalProductId, inventory.get(finalProductId)));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Restock interrupted for " + finalProductId);
            }
        });
    }

    /**
     * Handles order confirmation requests - Phase 2 of two-phase commit.
     *
     * Design Decision: Remove reservation from temporary storage to finalize the sale.
     * At this point, inventory has already been deducted, so confirmation just cleans up
     * the reservation tracking. This makes the transaction permanent and irreversible.
     */
    private void handleConfirmRequest(byte[] identity, Message request) {
        String sagaId = request.getSagaId();
        ReservedItem reservation;

        // Remove reservation to make sale permanent
        synchronized (reservationLock) {
            reservation = reservations.remove(sagaId);
        }

        if (reservation != null) {
            // Reservation found and removed - sale is now final
            logger.info(String.format("Confirmed order for saga %s: %d units of %s",
                    sagaId, reservation.quantity, reservation.productId));

            sendResponse(identity, new Message(
                    MessageType.CONFIRM_RESPONSE,
                    sagaId,
                    true,
                    null
            ));
        } else {
            // No reservation found - possible duplicate request or timeout
            logger.warning("No reservation found for saga: " + sagaId);
            sendResponse(identity, new Message(
                    MessageType.CONFIRM_RESPONSE,
                    sagaId,
                    false,
                    "No reservation found"
            ));
        }
    }

    /**
     * Handles order cancellation requests - Compensation phase of SAGA pattern.
     *
     * Design Decision: Atomically return reserved inventory and process any waiting orders.
     * Always respond with success to ensure marketplace receives confirmation of cancellation,
     * even if no reservation exists (idempotent operation for reliability).
     */
    private void handleCancelRequest(byte[] identity, Message request) {
        String sagaId = request.getSagaId();
        ReservedItem reservation;

        // Critical section: atomic inventory restoration and reservation cleanup
        // Same lock ordering as reservation to prevent deadlocks
        synchronized (inventoryLock) {
            synchronized (reservationLock) {
                reservation = reservations.remove(sagaId);

                if (reservation != null) {
                    // Return reserved inventory to available pool
                    inventory.merge(reservation.productId, reservation.quantity, Integer::sum);
                    logger.info(String.format("Cancelled reservation for saga %s: returned %d units of %s",
                            sagaId, reservation.quantity, reservation.productId));
                }
                // Note: No error if reservation not found - cancellation is idempotent
            }
        }

        // Always confirm cancellation to marketplace (idempotent operation)
        sendResponse(identity, new Message(
                MessageType.CANCEL_RESPONSE,
                sagaId,
                true,
                null
        ));
    }

    /**
     * Simulates realistic processing delays using normal distribution.
     *
     * Design Decision: Model real-world processing time variability to test system
     * behavior under realistic conditions. Uses normal distribution around configured
     * average with standard deviation to create realistic timing patterns.
     */
    private void simulateProcessingDelay() {
        try {
            // Calculate delay using normal distribution (mean ± standard deviation)
            long delay = (long) (config.getAverageProcessingTimeMs() +
                    (random.nextGaussian() * config.getProcessingTimeStdDev()));

            // Ensure non-negative delay (clamp to zero minimum)
            delay = Math.max(0, delay);

            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
    }

    /**
     * Determines if this request should simulate a failure scenario.
     *
     * Design Decision: Use configurable probability distribution to simulate various
     * failure modes that occur in real distributed systems. This enables testing
     * of marketplace resilience and recovery mechanisms.
     */
    private boolean shouldSimulateFailure() {
        double rand = random.nextDouble();

        // Check if random value falls within any failure probability range
        return rand < (config.getCrashProbability() +
                config.getNoResponseProbability() +
                config.getProcessButNoConfirmProbability());
    }

    /**
     * Simulates various failure scenarios to test distributed system resilience.
     *
     * Design Decision: Implement three distinct failure modes that occur in real systems:
     * 1. Complete crash (no processing, no response)
     * 2. Silent failure (process but no response)
     * 3. Processing failure (process and respond with error)
     *
     * These scenarios test marketplace timeout handling, retry logic, and compensation mechanisms.
     */
    private void handleSimulatedFailure(byte[] identity, Message request) {
        double rand = random.nextDouble();

        if (rand < config.getCrashProbability()) {
            // Scenario 1: Complete system crash - no processing, no response
            // Tests marketplace timeout and rollback mechanisms
            logger.warning(String.format("Simulating crash for saga %s", request.getSagaId()));
            return; // Exit without processing or responding

        } else if (rand < config.getCrashProbability() + config.getNoResponseProbability()) {
            // Scenario 2: Process request but fail to send response
            // Tests marketplace retry and timeout logic with potential state inconsistency
            logger.warning(String.format("Simulating no response for saga %s", request.getSagaId()));

            // Actually process reservation but don't send confirmation
            if (request.getType() == MessageType.RESERVE_REQUEST) {
                String productId = request.getProductId();
                int quantity = request.getQuantity();

                // Perform actual reservation to create potential inconsistency
                synchronized (inventoryLock) {
                    synchronized (reservationLock) {
                        Integer available = inventory.get(productId);
                        if (available != null && available >= quantity) {
                            inventory.put(productId, available - quantity);
                            reservations.put(request.getSagaId(), new ReservedItem(productId, quantity));
                        }
                    }
                }
            }
            return; // Exit without sending response

        } else if (rand < config.getCrashProbability() +
                config.getNoResponseProbability() +
                config.getProcessButNoConfirmProbability()) {
            // Scenario 3: Process request and respond with business failure
            // Tests marketplace handling of explicit business errors
            logger.warning(String.format("Simulating processing failure for saga %s", request.getSagaId()));

            // Determine appropriate response type
            MessageType responseType = switch (request.getType()) {
                case RESERVE_REQUEST -> MessageType.RESERVE_RESPONSE;
                case CONFIRM_REQUEST -> MessageType.CONFIRM_RESPONSE;
                case CANCEL_REQUEST -> MessageType.CANCEL_RESPONSE;
                default -> MessageType.ERROR_RESPONSE;
            };

            // Send explicit failure response
            sendResponse(identity, new Message(
                    responseType,
                    request.getSagaId(),
                    false,
                    "Simulated processing failure"
            ));
        }
    }

    private void sendResponse(byte[] identity, Message response) {
        ZMsg reply = new ZMsg();
        reply.add(identity);
        reply.add(response.toJson().getBytes());
        reply.send(socket);

        logger.fine(String.format("Sent %s response for saga %s",
                response.getType(), response.getSagaId()));
    }

    /**
     * Gracefully shuts down the seller, ensuring proper resource cleanup.
     *
     * Design Decision: Implement graceful shutdown to prevent resource leaks and
     * ensure any in-flight requests are handled properly. Close resources in
     * reverse dependency order: threads, then sockets, then context.
     */
    public void shutdown() {
        logger.info("Shutting down seller " + sellerId);
        shutdown = true; // Signal message handler to stop

        // Step 1: Stop message processing thread
        if (messageHandlerThread != null && messageHandlerThread.isAlive()) {
            messageHandlerThread.interrupt(); // Request graceful shutdown
            try {
                messageHandlerThread.join(2000); // Wait up to 2 seconds for completion
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Interrupted while waiting for message handler to stop");
            }
        }

        // Step 2: Close network resources in dependency order
        if (socket != null) {
            socket.close(); // Close socket first
        }

        if (context != null) {
            context.close(); // Close ZeroMQ context last
        }

        logger.info("Seller " + sellerId + " shutdown complete");
    }

    private void checkLowStockAlert(String productId) {
        Integer currentStock = inventory.get(productId);
        Integer threshold = config.getMinThresholds().get(productId);

        if (currentStock != null && threshold != null && currentStock <= threshold) {
            logger.warning(String.format("LOW STOCK ALERT: %s has %d units remaining (threshold: %d)",
                    productId, currentStock, threshold));
            notifyRestock(productId);
        }
    }

    private void notifyRestock(String productId) {
        logger.info(String.format("RESTOCK NOTIFICATION: %s needs replenishment", productId));

        // Automatic restocking if enabled
        if (config.isRestockingEnabled()) {
            Integer threshold = config.getMinThresholds().get(productId);
            Integer initialStock = config.getInitialInventory().get(productId);
            Integer currentStock = inventory.get(productId);

            if (threshold != null && initialStock != null && currentStock != null) {
                // Restock to initial inventory level
                int restockQuantity = initialStock - currentStock;

                logger.info(String.format("AUTO-RESTOCK: Ordering %d units of %s to restore to initial level",
                        restockQuantity, productId));

                // Make variables effectively final for lambda
                final String finalProductId = productId;
                final int finalRestockQuantity = restockQuantity;

                // Asynchronous restock using virtual threads
                CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(config.getRestockDelayMs());

                        synchronized (inventoryLock) {
                            inventory.merge(finalProductId, finalRestockQuantity, Integer::sum);
                            lastRestockTime.put(finalProductId, System.currentTimeMillis());

                            logger.info(String.format("AUTO-RESTOCK COMPLETE: Added %d units of %s. New total: %d",
                                    finalRestockQuantity, finalProductId, inventory.get(finalProductId)));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
    }

    private void checkOutOfStockShutdown(String productId) {
        if (!config.isRestockingEnabled()) {
            Integer currentStock = inventory.get(productId);
            if (currentStock != null && currentStock == 0) {
                logger.severe(String.format("OUT OF STOCK: %s has 0 units remaining and restocking is disabled. Shutting down seller %s!",
                        productId, sellerId));

                // Create a separate virtual thread to handle shutdown to avoid deadlock
                Thread shutdownThread = Thread.ofVirtual()
                    .name("OutOfStockShutdown-" + sellerId)
                    .start(() -> {
                        try {
                            Thread.sleep(1000); // Give time for current response to be sent
                            logger.severe(String.format("SELLER %s SHUTTING DOWN DUE TO OUT OF STOCK CONDITION", sellerId));
                            System.exit(1); // Immediate shutdown with error code
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
            }
        }
    }


    private void handleRestockRequest(byte[] identity, Message request) {
        String productId = request.getProductId();
        int quantity = request.getQuantity();

        if (!config.isRestockingEnabled()) {
            logger.severe(String.format("Restocking disabled for %s - rejecting restock request for %s",
                    sellerId, productId));
            sendResponse(identity, new Message(
                    MessageType.RESTOCK_RESPONSE,
                    request.getSagaId(),
                    false,
                    "Restocking is disabled"
            ));
            return;
        }

        synchronized (inventoryLock) {
            inventory.merge(productId, quantity, Integer::sum);
            lastRestockTime.put(productId, System.currentTimeMillis());
            logger.info(String.format("Restocked %s with %d units. New total: %d",
                    productId, quantity, inventory.get(productId)));
        }

        sendResponse(identity, new Message(
                MessageType.RESTOCK_RESPONSE,
                request.getSagaId(),
                true,
                "Restock completed"
        ));
    }

    /**
     * Waits for the message handler thread to complete.
     * Used by main thread to keep the seller process running.
     */
    public void waitForCompletion() throws InterruptedException {
        if (messageHandlerThread != null) {
            messageHandlerThread.join();
        }
    }

    // Inner classes
    private static class ReservedItem {
        final String productId;
        final int quantity;
        final long timestamp;

        ReservedItem(String productId, int quantity) {
            this.productId = productId;
            this.quantity = quantity;
            this.timestamp = System.currentTimeMillis();
        }
    }


    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java Seller <sellerId> <endpoint> [configFile]");
            System.exit(1);
        }

        String sellerId = args[0];
        String endpoint = args[1];
        String configFile = args.length > 2 ? args[2] : "seller.properties";

        SellerConfig config = SellerConfig.loadFromFile(configFile);
        Seller seller = new Seller(sellerId, endpoint, config);

        Runtime.getRuntime().addShutdownHook(new Thread(seller::shutdown));

        logger.info("Seller " + sellerId + " started");
        
        // Keep the main thread alive to let the background message handler run
        try {
            seller.waitForCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Seller " + sellerId + " interrupted");
        }
    }
}