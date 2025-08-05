package com.example.client;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.example.common.*;
import com.example.config.ClientConfig;

/**
 * Client entity that generates and sends orders to marketplace instances.
 * 
 * Architecture Overview:
 * - Replaces internal order generation in Marketplace with external client-driven orders
 * - Connects to multiple marketplaces using ZeroMQ REQ sockets
 * - Simulates realistic customer behavior with configurable patterns
 * - Supports load testing with multiple concurrent client instances
 * - Uses virtual threads for high concurrency and scalability
 * 
 * Order Flow:
 * 1. Client generates realistic orders based on configuration
 * 2. Sends orders to available marketplaces using round-robin or random selection
 * 3. Tracks order completion and maintains statistics
 * 4. Handles marketplace failures and retries
 * 
 * Threading Model:
 * - Order generation thread: Creates orders at configured intervals
 * - Order sender threads: Send orders to marketplaces (virtual threads)
 * - Response handler thread: Processes order completion notifications
 * - Load testing threads: Multiple concurrent clients for stress testing
 */
public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    
    // Core identification and configuration
    private final String clientId;
    private final ClientConfig config;
    
    // ZeroMQ communication infrastructure
    private final ZContext context;
    private final Map<String, ZMQ.Socket> marketplaceSockets;
    private final Map<String, ZMQ.Socket> notificationSockets;  // PULL sockets for SAGA notifications
    private final List<String> availableMarketplaces;
    private volatile int marketplaceIndex = 0; // For round-robin selection
    
    // Order tracking and statistics
    private final Map<String, PendingOrder> pendingOrders;
    private final Map<String, CompletableFuture<OrderResult>> orderFutures;
    private volatile int ordersGenerated = 0;
    private volatile int ordersAcknowledged = 0;  // Orders acknowledged by marketplace
    private volatile int ordersCompleted = 0;     // Orders with successful SAGA completion
    private volatile int ordersFailed = 0;        // Orders with SAGA failure
    private volatile int ordersTimedOut = 0;      // Orders with SAGA timeout
    
    // Threading infrastructure (virtual threads)
    private volatile boolean shutdown = false;
    private Thread orderGeneratorThread;
    private Thread responseHandlerThread;
    private Thread notificationHandlerThread;
    private Thread statisticsThread;
    
    // Utilities
    private final Random random;
    private final ScheduledExecutorService scheduler;
    
    /**
     * Initializes a new Client instance.
     */
    public Client(String clientId, ClientConfig config) {
        this.clientId = clientId;
        this.config = config;
        this.context = new ZContext();
        this.marketplaceSockets = new ConcurrentHashMap<>();
        this.notificationSockets = new ConcurrentHashMap<>();
        this.availableMarketplaces = new ArrayList<>();
        this.pendingOrders = new ConcurrentHashMap<>();
        this.orderFutures = new ConcurrentHashMap<>();
        this.random = new Random();
        this.scheduler = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
        
        // Initialize marketplace connections
        initializeMarketplaceConnections();
        initializeNotificationConnections();
        
        // Start client operations
        startResponseHandler();
        startNotificationHandler();
        startStatisticsReporter();
        startOrderGenerator();
    }
    
    /**
     * Establishes ZeroMQ connections to all configured marketplaces.
     */
    private void initializeMarketplaceConnections() {
        for (Map.Entry<String, String> entry : config.getMarketplaceEndpoints().entrySet()) {
            String marketplaceId = entry.getKey();
            String endpoint = entry.getValue();
            
            try {
                ZMQ.Socket socket = context.createSocket(SocketType.REQ);
                socket.setIdentity((clientId + "-" + marketplaceId).getBytes());
                socket.connect(endpoint);
                
                // Set socket options for reliability
                socket.setReceiveTimeOut(config.getOrderTimeoutMs());
                socket.setReqRelaxed(true); // Allow new requests before receiving reply
                
                marketplaceSockets.put(marketplaceId, socket);
                availableMarketplaces.add(marketplaceId);
                
                logger.info(String.format("Client %s connected to marketplace %s at %s", 
                    clientId, marketplaceId, endpoint));
            } catch (Exception e) {
                logger.warning(String.format("Failed to connect to marketplace %s at %s: %s", 
                    marketplaceId, endpoint, e.getMessage()));
            }
        }
        
        if (availableMarketplaces.isEmpty()) {
            throw new IllegalStateException("No marketplaces available for client " + clientId);
        }
    }
    
    /**
     * Establishes ZeroMQ PULL connections to all configured marketplaces for SAGA notifications.
     */
    private void initializeNotificationConnections() {
        for (Map.Entry<String, String> entry : config.getMarketplaceEndpoints().entrySet()) {
            String marketplaceId = entry.getKey();
            String orderEndpoint = entry.getValue();
            
            try {
                // Calculate notification endpoint (6555+ instead of 5555+)
                String[] parts = orderEndpoint.split(":");
                int orderPort = Integer.parseInt(parts[2]);
                int notificationPort = orderPort + 1000; // 6555+ range
                String notificationEndpoint = parts[0] + ":" + parts[1] + ":" + notificationPort;
                
                ZMQ.Socket socket = context.createSocket(SocketType.PULL);
                socket.setIdentity((clientId + "-notif-" + marketplaceId).getBytes());
                socket.connect(notificationEndpoint);
                
                // Set socket options for reliability
                socket.setReceiveTimeOut(1000); // 1 second timeout for polling
                
                notificationSockets.put(marketplaceId, socket);
                
                logger.info(String.format("Client %s connected to marketplace %s notifications at %s", 
                    clientId, marketplaceId, notificationEndpoint));
            } catch (Exception e) {
                logger.warning(String.format("Failed to connect to marketplace %s notifications: %s", 
                    marketplaceId, e.getMessage()));
            }
        }
    }
    
    /**
     * Starts the main order generation thread.
     */
    private void startOrderGenerator() {
        orderGeneratorThread = Thread.ofVirtual()
            .name("OrderGenerator-" + clientId)
            .start(() -> {
                try {
                    Thread.sleep(1000); // Initial delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                
                while (!Thread.currentThread().isInterrupted() && !shutdown) {
                    try {
                        // Check if we've reached the total order limit (skip check if totalOrders <= 0 for unlimited)
                        if (config.getTotalOrders() > 0 && ordersGenerated >= config.getTotalOrders()) {
                            logger.info(String.format("Client %s reached order limit (%d), stopping generation", 
                                clientId, config.getTotalOrders()));
                            break;
                        }
                        
                        // Generate and send order
                        Order order = generateRandomOrder();
                        sendOrderToMarketplace(order);
                        
                        // Wait for configured interval
                        Thread.sleep(config.getOrderIntervalMs());
                        
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Error in order generation", e);
                    }
                }
                
                logger.info("Order generator stopped for client " + clientId);
            });
    }
    
    /**
     * Starts the response handler thread for marketplace responses.
     */
    private void startResponseHandler() {
        responseHandlerThread = Thread.ofVirtual()
            .name("ResponseHandler-" + clientId)
            .start(() -> {
                // Create poller for all marketplace sockets
                ZMQ.Poller poller = context.createPoller(marketplaceSockets.size());
                for (ZMQ.Socket socket : marketplaceSockets.values()) {
                    poller.register(socket, ZMQ.Poller.POLLIN);
                }
                
                while (!Thread.currentThread().isInterrupted() && !shutdown) {
                    try {
                        if (poller.poll(1000) > 0) {
                            for (Map.Entry<String, ZMQ.Socket> entry : marketplaceSockets.entrySet()) {
                                ZMQ.Socket socket = entry.getValue();
                                int socketIndex = new ArrayList<>(marketplaceSockets.values()).indexOf(socket);
                                
                                if (poller.pollin(socketIndex)) {
                                    ZMsg msg = ZMsg.recvMsg(socket);
                                    if (msg != null) {
                                        // Process response in virtual thread
                                        Thread.ofVirtual()
                                            .name("ResponseProcessor-" + clientId + "-" + System.currentTimeMillis())
                                            .start(() -> handleMarketplaceResponse(entry.getKey(), msg));
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        if (!shutdown) {
                            logger.log(Level.SEVERE, "Error in response handler", e);
                        }
                    }
                }
                
                logger.info("Response handler stopped for client " + clientId);
            });
    }
    
    /**
     * Starts the notification handler thread for SAGA completion notifications.
     */
    private void startNotificationHandler() {
        notificationHandlerThread = Thread.ofVirtual()
            .name("NotificationHandler-" + clientId)
            .start(() -> {
                // Create poller for all notification sockets
                ZMQ.Poller poller = context.createPoller(notificationSockets.size());
                for (ZMQ.Socket socket : notificationSockets.values()) {
                    poller.register(socket, ZMQ.Poller.POLLIN);
                }
                
                while (!Thread.currentThread().isInterrupted() && !shutdown) {
                    try {
                        if (poller.poll(1000) > 0) {
                            for (Map.Entry<String, ZMQ.Socket> entry : notificationSockets.entrySet()) {
                                ZMQ.Socket socket = entry.getValue();
                                int socketIndex = new ArrayList<>(notificationSockets.values()).indexOf(socket);
                                
                                if (poller.pollin(socketIndex)) {
                                    ZMsg msg = ZMsg.recvMsg(socket);
                                    if (msg != null) {
                                        // Process notification in virtual thread
                                        Thread.ofVirtual()
                                            .name("NotificationProcessor-" + clientId + "-" + System.currentTimeMillis())
                                            .start(() -> handleSagaNotification(entry.getKey(), msg));
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        if (!shutdown) {
                            logger.log(Level.SEVERE, "Error in notification handler", e);
                        }
                    }
                }
                
                logger.info("Notification handler stopped for client " + clientId);
            });
    }
    
    /**
     * Starts the statistics reporting thread.
     */
    private void startStatisticsReporter() {
        statisticsThread = Thread.ofVirtual()
            .name("Statistics-" + clientId)
            .start(() -> {
                while (!Thread.currentThread().isInterrupted() && !shutdown) {
                    try {
                        Thread.sleep(10000); // Report every 10 seconds
                        
                        logger.info(String.format("Client %s Statistics: Generated=%d, Acknowledged=%d, Completed=%d, Failed=%d, TimedOut=%d, Pending=%d",
                            clientId, ordersGenerated, ordersAcknowledged, ordersCompleted, ordersFailed, ordersTimedOut, pendingOrders.size()));
                        
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                logger.info("Statistics reporter stopped for client " + clientId);
            });
    }
    
    /**
     * Generates a realistic random order based on configuration.
     */
    private Order generateRandomOrder() {
        List<OrderItem> items = new ArrayList<>();
        int numItems = random.nextInt(config.getMaxItemsPerOrder()) + 1;
        
        // Select random customer
        String customerId = config.getCustomerIds().get(random.nextInt(config.getCustomerIds().size()));
        
        // Generate random items
        for (int i = 0; i < numItems; i++) {
            // Random product
            String productId = config.getProductIds().get(random.nextInt(config.getProductIds().size()));
            
            // Random quantity
            int quantity = random.nextInt(config.getMaxQuantityPerItem()) + 1;
            
            // Random seller - distribute across available sellers
            String sellerId = "SELLER-" + (random.nextInt(5) + 1);
            
            items.add(new OrderItem(sellerId, productId, quantity));
        }
        
        return new Order(customerId, items);
    }
    
    /**
     * Sends an order to an available marketplace using round-robin selection.
     */
    private void sendOrderToMarketplace(Order order) {
        if (availableMarketplaces.isEmpty()) {
            logger.severe("No marketplaces available to send order");
            return;
        }
        
        // Round-robin marketplace selection
        String selectedMarketplace = availableMarketplaces.get(marketplaceIndex % availableMarketplaces.size());
        marketplaceIndex++;
        
        ZMQ.Socket socket = marketplaceSockets.get(selectedMarketplace);
        if (socket == null) {
            logger.warning("Socket not found for marketplace: " + selectedMarketplace);
            return;
        }
        
        // Create order message
        String orderId = UUID.randomUUID().toString();
        Message orderMessage = new Message(
            MessageType.ORDER_REQUEST,
            orderId,
            order.getCustomerId(),
            0 // Will be set by marketplace
        );
        orderMessage.setOrder(order);
        
        // Track pending order
        PendingOrder pendingOrder = new PendingOrder(orderId, order, selectedMarketplace, System.currentTimeMillis());
        pendingOrders.put(orderId, pendingOrder);
        
        // Create completable future for async handling
        CompletableFuture<OrderResult> future = new CompletableFuture<>();
        orderFutures.put(orderId, future);
        
        // Schedule timeout handling - use longer timeout for SAGA completion
        scheduler.schedule(() -> {
            if (pendingOrders.containsKey(orderId)) {
                handleOrderTimeout(orderId);
            }
        }, config.getOrderTimeoutMs() * 10, TimeUnit.MILLISECONDS);  // 10x longer for SAGA completion
        
        // Send order in virtual thread
        Thread.ofVirtual()
            .name("OrderSender-" + clientId + "-" + orderId)
            .start(() -> {
                try {
                    String json = orderMessage.toJson();
                    socket.send(json.getBytes());
                    
                    ordersGenerated++;
                    logger.info(String.format("Client %s sent order %s to marketplace %s (%d items)",
                        clientId, orderId, selectedMarketplace, order.getItems().size()));
                        
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error sending order " + orderId, e);
                    handleOrderFailure(orderId, "Failed to send order: " + e.getMessage());
                }
            });
    }
    
    /**
     * Handles marketplace response messages.
     */
    private void handleMarketplaceResponse(String marketplaceId, ZMsg msg) {
        try {
            String content = new String(msg.getLast().getData());
            Message response = Message.fromJson(content);
            String orderId = response.getSagaId();
            
            PendingOrder pendingOrder = pendingOrders.get(orderId);
            if (pendingOrder == null) {
                logger.warning("Received response for unknown order: " + orderId);
                return;
            }
            
            switch (response.getType()) {
                case ORDER_RESPONSE:
                    // This is just acknowledgment that marketplace received the order
                    ordersAcknowledged++;
                    if (response.isSuccess()) {
                        logger.info(String.format("Order %s acknowledged by marketplace %s",
                            orderId, marketplaceId));
                        
                        // Keep the order in pending state for SAGA notifications
                        // Don't complete the future - wait for SAGA completion notification
                        orderFutures.put(orderId, orderFutures.getOrDefault(orderId, new CompletableFuture<>()));
                    } else {
                        // Order rejected at marketplace level - remove from pending and complete future
                        pendingOrders.remove(orderId);
                        CompletableFuture<OrderResult> future = orderFutures.remove(orderId);
                        ordersFailed++;
                        logger.warning(String.format("Order %s rejected at marketplace %s: %s",
                            orderId, marketplaceId, response.getErrorMessage()));
                        
                        if (future != null) {
                            future.complete(new OrderResult(orderId, false, response.getErrorMessage()));
                        }
                    }
                    break;
                    
                default:
                    logger.warning("Unexpected response type from marketplace: " + response.getType());
                    // Keep order in pending state - might be processed by notification handler
            }
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling marketplace response", e);
        }
    }
    
    /**
     * Handles SAGA completion notifications from marketplace.
     */
    private void handleSagaNotification(String marketplaceId, ZMsg msg) {
        try {
            String content = new String(msg.getLast().getData());
            Message notification = Message.fromJson(content);
            String orderId = notification.getSagaId();
            
            logger.info(String.format("Received SAGA notification from %s for order %s: %s",
                marketplaceId, orderId, notification.getType()));
            
            // Remove from pending orders for terminal states
            PendingOrder pendingOrder = pendingOrders.remove(orderId);
            CompletableFuture<OrderResult> future = orderFutures.remove(orderId);
            
            switch (notification.getType()) {
                case SAGA_COMPLETED:
                    // SAGA successfully completed
                    ordersCompleted++;
                    logger.info(String.format("Order %s SAGA completed successfully", orderId));
                    
                    if (future != null) {
                        future.complete(new OrderResult(orderId, true, null));
                    }
                    break;
                    
                case SAGA_FAILED:
                    // SAGA failed and was rolled back
                    ordersFailed++;
                    logger.warning(String.format("Order %s SAGA failed: %s", 
                        orderId, notification.getFailureReason()));
                    
                    if (future != null) {
                        future.complete(new OrderResult(orderId, false, notification.getFailureReason()));
                    }
                    break;
                    
                case SAGA_TIMED_OUT:
                    // SAGA timed out
                    ordersTimedOut++;
                    logger.warning(String.format("Order %s SAGA timed out: %s", 
                        orderId, notification.getFailureReason()));
                    
                    if (future != null) {
                        future.complete(new OrderResult(orderId, false, notification.getFailureReason()));
                    }
                    break;
                    
                default:
                    logger.warning("Unexpected notification type: " + notification.getType());
                    ordersFailed++;
                    if (future != null) {
                        future.complete(new OrderResult(orderId, false, "Unexpected notification type"));
                    }
            }
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling SAGA notification", e);
        }
    }
    
    /**
     * Handles order timeout.
     */
    private void handleOrderTimeout(String orderId) {
        PendingOrder pendingOrder = pendingOrders.remove(orderId);
        if (pendingOrder != null) {
            ordersTimedOut++;
            logger.warning(String.format("Order %s timed out (marketplace: %s)",
                orderId, pendingOrder.getMarketplaceId()));
            
            CompletableFuture<OrderResult> future = orderFutures.remove(orderId);
            if (future != null) {
                future.complete(new OrderResult(orderId, false, "Order timed out"));
            }
        }
    }
    
    /**
     * Handles order failure.
     */
    private void handleOrderFailure(String orderId, String errorMessage) {
        PendingOrder pendingOrder = pendingOrders.remove(orderId);
        if (pendingOrder != null) {
            ordersFailed++;
            logger.warning(String.format("Order %s failed: %s", orderId, errorMessage));
            
            CompletableFuture<OrderResult> future = orderFutures.remove(orderId);
            if (future != null) {
                future.complete(new OrderResult(orderId, false, errorMessage));
            }
        }
    }
    
    /**
     * Gracefully shuts down the client.
     */
    public void shutdown() {
        logger.info("Shutting down client " + clientId);
        shutdown = true;
        
        // Stop all threads
        Thread[] threads = {orderGeneratorThread, responseHandlerThread, notificationHandlerThread, statisticsThread};
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
            }
        }
        
        // Wait for threads to finish
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) {
                try {
                    thread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        // Close sockets
        for (ZMQ.Socket socket : marketplaceSockets.values()) {
            if (socket != null) {
                socket.close();
            }
        }
        
        for (ZMQ.Socket socket : notificationSockets.values()) {
            if (socket != null) {
                socket.close();
            }
        }
        
        // Shutdown scheduler
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Close context
        if (context != null) {
            context.close();
        }
        
        logger.info("Client " + clientId + " shutdown complete");
    }
    
    /**
     * Gets current statistics.
     */
    public ClientStats getStats() {
        return new ClientStats(ordersGenerated, ordersAcknowledged, ordersCompleted, ordersFailed, ordersTimedOut, pendingOrders.size());
    }
    
    // Inner classes
    private static class PendingOrder {
        private final String orderId;
        private final Order order;
        private final String marketplaceId;
        private final long timestamp;
        
        public PendingOrder(String orderId, Order order, String marketplaceId, long timestamp) {
            this.orderId = orderId;
            this.order = order;
            this.marketplaceId = marketplaceId;
            this.timestamp = timestamp;
        }
        
        public String getOrderId() { return orderId; }
        public Order getOrder() { return order; }
        public String getMarketplaceId() { return marketplaceId; }
        public long getTimestamp() { return timestamp; }
    }
    
    public static class OrderResult {
        private final String orderId;
        private final boolean success;
        private final String errorMessage;
        
        public OrderResult(String orderId, boolean success, String errorMessage) {
            this.orderId = orderId;
            this.success = success;
            this.errorMessage = errorMessage;
        }
        
        public String getOrderId() { return orderId; }
        public boolean isSuccess() { return success; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    public static class ClientStats {
        private final int ordersGenerated;
        private final int ordersAcknowledged;
        private final int ordersCompleted;
        private final int ordersFailed;
        private final int ordersTimedOut;
        private final int ordersPending;
        
        public ClientStats(int ordersGenerated, int ordersAcknowledged, int ordersCompleted, int ordersFailed, int ordersTimedOut, int ordersPending) {
            this.ordersGenerated = ordersGenerated;
            this.ordersAcknowledged = ordersAcknowledged;
            this.ordersCompleted = ordersCompleted;
            this.ordersFailed = ordersFailed;
            this.ordersTimedOut = ordersTimedOut;
            this.ordersPending = ordersPending;
        }
        
        public int getOrdersGenerated() { return ordersGenerated; }
        public int getOrdersAcknowledged() { return ordersAcknowledged; }
        public int getOrdersCompleted() { return ordersCompleted; }
        public int getOrdersFailed() { return ordersFailed; }
        public int getOrdersTimedOut() { return ordersTimedOut; }
        public int getOrdersPending() { return ordersPending; }
    }

    /**
     * Waits for all background threads to complete.
     * Used by main thread to keep the client process running.
     */
    public void waitForCompletion() throws InterruptedException {
        Thread[] threads = {orderGeneratorThread, responseHandlerThread, notificationHandlerThread, statisticsThread};
        
        for (Thread thread : threads) {
            if (thread != null) {
                thread.join();
            }
        }
    }
    
    public static void main(String[] args) {
        String clientId = args.length > 0 ? args[0] : "CLIENT-1";
        String configFile = args.length > 1 ? args[1] : "client.properties";
        
        ClientConfig config = ClientConfig.loadFromFile(configFile);
        Client client = new Client(clientId, config);
        
        Runtime.getRuntime().addShutdownHook(new Thread(client::shutdown));
        
        logger.info("Client " + clientId + " started");
        
        // Keep the main thread alive to let the background threads run
        try {
            client.waitForCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Client " + clientId + " interrupted");
        }
    }
}