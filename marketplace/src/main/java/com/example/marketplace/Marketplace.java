package com.example.marketplace;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.example.common.*;
import com.example.config.MarketplaceConfig;

/**
 * Distributed Marketplace implementation using SAGA pattern for atomic transactions.
 * 
 * Architecture Overview:
 * - Acts as SAGA coordinator for distributed transactions across multiple sellers
 * - Uses ZeroMQ DEALER sockets for asynchronous communication with sellers
 * - Implements two-phase commit protocol: Reserve → Confirm/Cancel
 * - Ensures atomic transactions: customers get all products or none
 * 
 * Threading Model:
 * - Order Generator: Creates random orders at configurable intervals
 * - Response Handler: Processes seller responses asynchronously
 * - Timeout Checker: Monitors and rolls back timed-out transactions
 * - Response Processing Pool: Individual threads per seller response
 */
public class Marketplace {
    private static final Logger logger = Logger.getLogger(Marketplace.class.getName());

    // Core identification and configuration
    private final String marketplaceId;                    // Unique identifier for this marketplace instance
    private final MarketplaceConfig config;                // Configuration loaded from properties file
    
    // ZeroMQ communication infrastructure
    private final ZContext context;                        // ZeroMQ context for socket management
    private final Map<String, ZMQ.Socket> sellerSockets;   // DEALER sockets to each seller (thread-safe)
    private final ZMQ.Socket clientSocket;                 // REP socket for client connections
    private final ZMQ.Socket notificationSocket;           // PUSH socket for SAGA completion notifications
    
    // SAGA transaction management
    private final Map<String, OrderSaga> activeSagas;      // Active transactions being processed
    private final Map<String, ScheduledFuture<?>> sagaTimeouts; // Track timeout tasks per saga
    private final ScheduledExecutorService scheduler;      // For retry logic and timeouts
    
    // Virtual Threading infrastructure
    private volatile boolean shutdown = false;             // Graceful shutdown flag
    private Thread responseHandlerThread;                  // Handles incoming seller responses
    private Thread timeoutCheckerThread;                   // Monitors for timed-out transactions
    private Thread clientHandlerThread;                    // Handles incoming client order requests
    
    // Utilities
    private static final long CLEANUP_GRACE_PERIOD_MS = 60000; // Time to wait before cleaning up timed-out SAGAs

    /**
     * Initializes a new Marketplace instance.
     * 
     * Design Decision: Start all background threads immediately to ensure system is ready
     * for processing as soon as construction completes. This supports the requirement
     * that no system should ever block.
     */
    public Marketplace(String marketplaceId, MarketplaceConfig config) {
        this.marketplaceId = marketplaceId;
        this.config = config;
        this.context = new ZContext();
        this.sellerSockets = new ConcurrentHashMap<>();  // Thread-safe for concurrent access
        this.clientSocket = context.createSocket(SocketType.REP); // REP socket for client requests
        this.notificationSocket = context.createSocket(SocketType.PUSH); // PUSH socket for SAGA notifications
        this.activeSagas = new ConcurrentHashMap<>();    // Thread-safe for SAGA management
        this.sagaTimeouts = new ConcurrentHashMap<>();   // Track timeout tasks
        this.scheduler = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory()); // Virtual threads for retries and timeouts

        // Initialize all components in dependency order
        initializeSellerConnections();
        initializeClientSocket();
        initializeNotificationSocket();
        startResponseHandler();
        startClientHandler();
        startTimeoutChecker();
    }

    /**
     * Establishes ZeroMQ connections to all configured sellers.
     * 
     * Design Decision: Use DEALER sockets for asynchronous, load-balanced communication.
     * Each seller gets a dedicated socket to avoid message routing complexities.
     * Identity is set to enable seller-side tracking of marketplace instances.
     */
    private void initializeSellerConnections() {
        for (String sellerId : config.getSellerEndpoints().keySet()) {
            String endpoint = config.getSellerEndpoints().get(sellerId);
            ZMQ.Socket socket = context.createSocket(SocketType.DEALER);
            
            // Set unique identity for this marketplace-seller connection
            socket.setIdentity((marketplaceId + "-" + sellerId).getBytes());
            socket.connect(endpoint);
            sellerSockets.put(sellerId, socket);
            
            logger.info(String.format("Connected to seller %s at %s", sellerId, endpoint));
        }
    }

    /**
     * Initializes the client socket for receiving order requests from clients.
     */
    private void initializeClientSocket() {
        // Bind to client endpoint based on marketplace ID
        String clientEndpoint = "tcp://*:" + (5555 + Integer.parseInt(marketplaceId.split("-")[1]) - 1);
        clientSocket.bind(clientEndpoint);
        logger.info(String.format("Marketplace %s listening for client orders on %s", marketplaceId, clientEndpoint));
    }

    /**
     * Initializes the notification socket for sending SAGA completion notifications to clients.
     */
    private void initializeNotificationSocket() {
        // Bind to notification endpoint based on marketplace ID (different port range)
        String notificationEndpoint = "tcp://*:" + (6555 + Integer.parseInt(marketplaceId.split("-")[1]) - 1);
        notificationSocket.bind(notificationEndpoint);
        logger.info(String.format("Marketplace %s sending SAGA notifications on %s", marketplaceId, notificationEndpoint));
    }

    /**
     * Starts the client handler thread for processing client order requests.
     */
    private void startClientHandler() {
        clientHandlerThread = Thread.ofVirtual().name("ClientHandler-" + marketplaceId).start(() -> {
            while (!Thread.currentThread().isInterrupted() && !shutdown) {
                try {
                    // Blocking receive for client requests
                    ZMsg msg = ZMsg.recvMsg(clientSocket);
                    if (msg != null) {
                        // Process each client request in virtual thread
                        Thread.ofVirtual()
                            .name("ClientRequestProcessor-" + marketplaceId + "-" + System.currentTimeMillis())
                            .start(() -> handleClientRequest(msg));
                    }
                } catch (Exception e) {
                    if (!shutdown) {
                        logger.log(Level.SEVERE, "Error in client handler", e);
                    }
                }
            }
            logger.info("Client handler thread stopped for " + marketplaceId);
        });
    }

    /**
     * Handles individual client order requests.
     */
    private void handleClientRequest(ZMsg msg) {
        try {
            String content = new String(msg.getLast().getData());
            Message request = Message.fromJson(content);
            
            if (request.getType() == MessageType.ORDER_REQUEST) {
                Order order = request.getOrder();
                String orderId = request.getSagaId();
                
                logger.info(String.format("Received order request %s from client (%d items)", 
                    orderId, order.getItems().size()));
                
                // Process the order through existing SAGA mechanism using original order ID
                processOrderWithId(order, orderId);
                
                // Set client info for notifications after SAGA is created
                OrderSaga saga = activeSagas.get(orderId);
                if (saga != null) {
                    saga.setClientInfo("CLIENT", notificationSocket);  // Use PUSH socket for notifications
                }
                
                // Send immediate acknowledgment to client
                Message response = new Message(MessageType.ORDER_RESPONSE, orderId, true, null);
                ZMsg reply = new ZMsg();
                reply.add(response.toJson().getBytes());
                reply.send(clientSocket);
                
                logger.info(String.format("Acknowledged order %s to client", orderId));
            } else {
                logger.warning("Received unexpected message type from client: " + request.getType());
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling client request", e);
        }
    }

    /**
     * Starts the main response handling thread that processes seller responses.
     * 
     * Design Decision: Use ZeroMQ poller for efficient I/O multiplexing across all seller sockets.
     * Each response is processed in a separate thread to prevent any single response from
     * blocking others, supporting the non-blocking requirement.
     * 
     * Threading Strategy: Main polling thread + dynamic response processing threads
     * ensures high throughput and prevents head-of-line blocking.
     */
    private void startResponseHandler() {
        responseHandlerThread = Thread.ofVirtual().name("ResponseHandler-" + marketplaceId).start(() -> {
            // Create poller for all seller sockets - efficient I/O multiplexing
            ZMQ.Poller poller = context.createPoller(sellerSockets.size());
            for (ZMQ.Socket socket : sellerSockets.values()) {
                poller.register(socket, ZMQ.Poller.POLLIN);
            }

            while (!Thread.currentThread().isInterrupted() && !shutdown) {
                try {
                    // Non-blocking poll with 1-second timeout for graceful shutdown
                    if (poller.poll(1000) > 0) {
                        for (Map.Entry<String, ZMQ.Socket> entry : sellerSockets.entrySet()) {
                            ZMQ.Socket socket = entry.getValue();
                            if (poller.pollin(sellerSockets.values().stream().toList().indexOf(socket))) {
                                ZMsg msg = ZMsg.recvMsg(socket);
                                if (msg != null) {
                                    // Process each response in separate virtual thread to prevent blocking
                                    Thread responseThread = Thread.ofVirtual()
                                        .name("ResponseHandler-" + entry.getKey() + "-" + System.currentTimeMillis())
                                        .start(() -> handleSellerResponse(entry.getKey(), msg));
                                }
                            }
                        }
                    }
                } catch (org.zeromq.ZMQException e) {
                    if (!shutdown) {
                        logger.log(Level.SEVERE, "ZMQ error in response handler", e);
                    }
                    break; // Exit on ZMQ errors
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Unexpected error in response handler", e);
                    break; // Exit on unexpected errors
                }
            }
            logger.info("Response handler thread stopped for " + marketplaceId);
        });
    }

    /**
     * Processes individual seller responses within dedicated threads.
     * 
     * Design Decision: Each response is processed independently to prevent any single
     * response from affecting others. Includes robust error handling and state validation
     * to ensure SAGA consistency even with malformed or late responses.
     */
    private void handleSellerResponse(String sellerId, ZMsg msg) {
        try {
            // Parse JSON response from seller
            String content = new String(msg.getLast().getData());
            Message response = Message.fromJson(content);
            String sagaId = response.getSagaId();

            // Validate SAGA still exists and can accept responses
            OrderSaga saga = activeSagas.get(sagaId);
            if (saga != null && saga.canProcessResponse()) {
                logger.info(String.format("Received response from %s for saga %s: %s",
                        sellerId, sagaId, response.getType()));
                
                // Track response for retry logic - prevents duplicate retries
                saga.markResponseReceived(sellerId, response.getType());

                // Route to appropriate handler based on SAGA phase
                switch (response.getType()) {
                    case RESERVE_RESPONSE:
                        handleReserveResponse(saga, sellerId, response);
                        break;
                    case CONFIRM_RESPONSE:
                        handleConfirmResponse(saga, sellerId, response);
                        break;
                    case CANCEL_RESPONSE:
                        handleCancelResponse(saga, sellerId, response);
                        break;
                    default:
                        logger.warning("Unknown response type: " + response.getType());
                }
            } else if (saga == null) {
                // SAGA may have been cleaned up due to timeout - this is normal
                logger.warning("Received response for unknown saga: " + sagaId);
            } else {
                // SAGA exists but is in wrong state (e.g., already completed/rolled back)
                logger.fine(String.format("Ignoring late response from %s for saga %s (status: %s)",
                        sellerId, sagaId, saga.getStatus()));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error handling seller response", e);
        }
    }

    /**
     * Handles RESERVE_RESPONSE messages - Phase 1 of two-phase commit.
     * 
     * Design Decision: Implement all-or-nothing semantics. If ANY seller fails to reserve,
     * the entire order fails and all successful reservations are rolled back.
     * This ensures atomic transaction behavior across distributed system.
     */
    private void handleReserveResponse(OrderSaga saga, String sellerId, Message response) {
        if (response.isSuccess()) {
            saga.markReserved(sellerId);
            logger.info(String.format("Product reserved at %s for saga %s", sellerId, saga.getSagaId()));

            // Check if all required sellers have successfully reserved
            if (saga.areAllReserved()) {
                // Phase 1 complete - proceed to Phase 2 (confirmation)
                confirmOrder(saga);
            }
            // Otherwise wait for more reservation responses
        } else {
            // ANY failure triggers complete rollback - ensures atomicity
            logger.warning(String.format("Reservation failed at %s for saga %s: %s",
                    sellerId, saga.getSagaId(), response.getErrorMessage()));
            
            // Initiate compensation transactions for all successful reservations
            rollbackOrder(saga);
        }
    }

    /**
     * Handles CONFIRM_RESPONSE messages - Phase 2 of two-phase commit.
     * 
     * Design Decision: Confirmation failures are rare since reservations were already successful.
     * However, we still handle them gracefully with rollback to maintain consistency.
     */
    private void handleConfirmResponse(OrderSaga saga, String sellerId, Message response) {
        if (response.isSuccess()) {
            saga.markConfirmed(sellerId);
            logger.info(String.format("Order confirmed at %s for saga %s", sellerId, saga.getSagaId()));

            // Check if all confirmations received
            if (saga.areAllConfirmed()) {
                // SAGA successfully completed - customer gets all products
                saga.setStatus(SagaStatus.COMPLETED);
                logger.info(String.format("Order %s completed successfully!", saga.getSagaId()));
                
                // Notify client about successful completion
                saga.notifyClient(SagaStatus.COMPLETED, null);
                
                // Clean up all saga resources
                cleanupSaga(saga.getSagaId());
            }
        } else {
            // Confirmation failure after successful reservation - unusual but possible
            logger.severe(String.format("Confirm failed at %s for saga %s", sellerId, saga.getSagaId()));
            
            // Rollback to maintain consistency even though reservations succeeded
            rollbackOrder(saga);
        }
    }

    /**
     * Handles CANCEL_RESPONSE messages - Compensation phase of SAGA pattern.
     * 
     * Design Decision: Track all cancellation confirmations to ensure complete cleanup.
     * Only mark SAGA as rolled back when all sellers have confirmed cancellation,
     * ensuring no inventory remains incorrectly reserved.
     */
    private void handleCancelResponse(OrderSaga saga, String sellerId, Message response) {
        saga.markCancelled(sellerId);
        logger.info(String.format("Cancellation confirmed at %s for saga %s", sellerId, saga.getSagaId()));

        // Check if all required cancellations are complete
        if (saga.areAllCancelledOrNotReserved()) {
            // Compensation complete - order failed but system is consistent
            saga.setStatus(SagaStatus.ROLLED_BACK);
            logger.info(String.format("Order %s rolled back successfully", saga.getSagaId()));
            
            // Notify client about rollback
            saga.notifyClient(SagaStatus.ROLLED_BACK, "Order was rolled back due to seller failures");
            
            // Clean up all saga resources
            cleanupSaga(saga.getSagaId());
        }
    }

    /**
     * Initiates a new distributed transaction using SAGA pattern with provided order ID.
     */
    public void processOrderWithId(Order order, String orderId) {
        processOrderInternal(order, orderId);
    }

    /**
     * Initiates a new distributed transaction using SAGA pattern.
     * 
     * Design Decision: Generate unique SAGA ID for transaction tracking and calculate
     * dynamic timeout based on transaction complexity (number of sellers involved).
     * Start with parallel reservation requests to minimize total transaction time.
     */
    public String processOrder(Order order) {
        String sagaId = UUID.randomUUID().toString();
        processOrderInternal(order, sagaId);
        return sagaId;
    }
    
    /**
     * Internal method to process orders with a given ID.
     */
    private void processOrderInternal(Order order, String sagaId) {
        
        // Determine all sellers involved in this order
        Set<String> sellers = new HashSet<>();
        for (OrderItem item : order.getItems()) {
            sellers.add(item.getSellerId());
        }
        
        // Calculate timeout based on complexity - more sellers = longer timeout
        long sagaTimeout = config.calculateSagaTimeout(sellers.size());
        
        // Create and register new SAGA transaction
        OrderSaga saga = new OrderSaga(sagaId, order, sagaTimeout);
        activeSagas.put(sagaId, saga);
        
        // Schedule timeout check for this specific saga
        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            OrderSaga currentSaga = activeSagas.get(sagaId);
            if (currentSaga != null && currentSaga.getStatus() == SagaStatus.STARTED) {
                logger.warning(String.format("Saga %s timed out, initiating rollback", sagaId));
                currentSaga.markTimedOut();
                
                // Notify client about timeout before rollback
                currentSaga.notifyClient(SagaStatus.TIMED_OUT, "Order timed out waiting for seller responses");
                
                rollbackOrder(currentSaga);
            }
        }, sagaTimeout, TimeUnit.MILLISECONDS);
        sagaTimeouts.put(sagaId, timeoutTask);

        logger.info(String.format("Starting order %s with %d items across %d sellers (timeout: %dms)", 
                sagaId, order.getItems().size(), sellers.size(), sagaTimeout));

        // Phase 1: Send reservation requests to all sellers in parallel
        // Parallel execution reduces total transaction time
        for (OrderItem item : order.getItems()) {
            sendReserveRequestWithRetry(saga, item.getSellerId(), item, 1);
        }
    }

    private void sendReserveRequest(OrderSaga saga, String sellerId, OrderItem item) {
        Message request = new Message(
                MessageType.RESERVE_REQUEST,
                saga.getSagaId(),
                item.getProductId(),
                item.getQuantity()
        );

        sendToSeller(sellerId, request);
        saga.markReservationSent(sellerId);
    }
    
    /**
     * Sends reservation request with retry logic and exponential backoff.
     * 
     * Design Decision: Implement retry mechanism to handle transient failures
     * (network issues, temporary seller unavailability). Uses exponential backoff
     * to avoid overwhelming failed sellers while ensuring timely retries.
     */
    private void sendReserveRequestWithRetry(OrderSaga saga, String sellerId, OrderItem item, int attempt) {
        if (attempt > config.getMaxRetries()) {
            logger.severe(String.format("Max retries exceeded for reservation request to %s for saga %s", 
                    sellerId, saga.getSagaId()));
            // Treat max retries as seller failure - rollback entire order
            rollbackOrder(saga);
            return;
        }
        
        // Create reservation request message
        Message request = new Message(
                MessageType.RESERVE_REQUEST,
                saga.getSagaId(),
                item.getProductId(),
                item.getQuantity()
        );
        
        // Send request and update SAGA tracking
        sendToSeller(sellerId, request);
        saga.markReservationSent(sellerId);
        saga.incrementRetryCount(sellerId);
        
        // Schedule retry check using exponential backoff strategy
        scheduler.schedule(() -> {
            OrderSaga currentSaga = activeSagas.get(saga.getSagaId());
            
            // Only retry if SAGA still active and no response received
            if (currentSaga != null && currentSaga.canProcessResponse() && 
                !currentSaga.hasReceivedResponse(sellerId, MessageType.RESERVE_RESPONSE)) {
                
                // Exponential backoff: 2^attempt seconds
                long delay = (long) Math.pow(2, attempt) * 1000;
                logger.info(String.format("Retrying reservation request to %s for saga %s (attempt %d) after %dms",
                        sellerId, saga.getSagaId(), attempt + 1, delay));
                
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                
                // Recursive retry with incremented attempt counter
                sendReserveRequestWithRetry(currentSaga, sellerId, item, attempt + 1);
            }
        }, config.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void confirmOrder(OrderSaga saga) {
        saga.setStatus(SagaStatus.CONFIRMING);

        for (String sellerId : saga.getReservedSellers()) {
            OrderItem item = saga.getOrder().getItems().stream()
                    .filter(i -> i.getSellerId().equals(sellerId))
                    .findFirst()
                    .orElse(null);

            if (item != null) {
                sendConfirmRequestWithRetry(saga, sellerId, item, 1);
            }
        }
    }
    
    private void sendConfirmRequestWithRetry(OrderSaga saga, String sellerId, OrderItem item, int attempt) {
        if (attempt > config.getMaxRetries()) {
            logger.severe(String.format("Max retries exceeded for confirm request to %s for saga %s", 
                    sellerId, saga.getSagaId()));
            rollbackOrder(saga);
            return;
        }
        
        Message request = new Message(
                MessageType.CONFIRM_REQUEST,
                saga.getSagaId(),
                item.getProductId(),
                item.getQuantity()
        );
        
        sendToSeller(sellerId, request);
        
        // Schedule retry check
        scheduler.schedule(() -> {
            OrderSaga currentSaga = activeSagas.get(saga.getSagaId());
            if (currentSaga != null && currentSaga.canProcessResponse() && 
                !currentSaga.hasReceivedResponse(sellerId, MessageType.CONFIRM_RESPONSE)) {
                
                long delay = (long) Math.pow(2, attempt) * 1000; // Exponential backoff
                logger.info(String.format("Retrying confirm request to %s for saga %s (attempt %d) after %dms",
                        sellerId, saga.getSagaId(), attempt + 1, delay));
                
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                
                sendConfirmRequestWithRetry(currentSaga, sellerId, item, attempt + 1);
            }
        }, config.getResponseTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void rollbackOrder(OrderSaga saga) {
        saga.setStatus(SagaStatus.ROLLING_BACK);

        // Cancel all reserved items
        Set<String> reservedSellers = saga.getReservedSellers();
        if (reservedSellers.isEmpty()) {
            // No reservations to cancel - complete rollback immediately
            saga.setStatus(SagaStatus.ROLLED_BACK);
            logger.info(String.format("Order %s rolled back immediately (no reservations)", saga.getSagaId()));
            
            // Notify client about immediate rollback
            saga.notifyClient(SagaStatus.ROLLED_BACK, "Order failed before any reservations were made");
            
            cleanupSaga(saga.getSagaId());
            return;
        }

        for (String sellerId : reservedSellers) {
            OrderItem item = saga.getOrder().getItems().stream()
                    .filter(i -> i.getSellerId().equals(sellerId))
                    .findFirst()
                    .orElse(null);

            if (item != null && !saga.getCancelledSellers().contains(sellerId)) {
                Message request = new Message(
                        MessageType.CANCEL_REQUEST,
                        saga.getSagaId(),
                        item.getProductId(),
                        item.getQuantity()
                );

                sendToSeller(sellerId, request);
            }
        }
    }

    private void sendToSeller(String sellerId, Message message) {
        ZMQ.Socket socket = sellerSockets.get(sellerId);
        if (socket != null) {
            String json = message.toJson();
            socket.send(json.getBytes());
            logger.fine(String.format("Sent %s to %s for saga %s",
                    message.getType(), sellerId, message.getSagaId()));
        } else {
            logger.severe("No socket found for seller: " + sellerId);
        }
    }

    /**
     * Starts background thread for monitoring and handling transaction timeouts.
     * 
     * Design Decision: Regular monitoring prevents stuck transactions from consuming
     * resources indefinitely. 5-second interval balances responsiveness with overhead.
     * Timeout handling ensures eventual consistency even when sellers become unresponsive.
     */
    private void startTimeoutChecker() {
        timeoutCheckerThread = Thread.ofVirtual().name("TimeoutChecker-" + marketplaceId).start(() -> {
            while (!Thread.currentThread().isInterrupted() && !shutdown) {
                try {
                    Thread.sleep(5000); // Check every 5 seconds - balances responsiveness vs overhead
                    
                    // Monitor active SAGAs for timeout violations
                    checkForTimeouts();
                    
                    // Clean up old timed-out SAGAs to prevent memory leaks
                    cleanupTimedOutSagas();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error in timeout checker", e);
                }
            }
            logger.info("Timeout checker thread stopped for " + marketplaceId);
        });
    }
    
    private void checkForTimeouts() {
        // Individual saga timeouts are now handled by scheduled tasks
        // No backup timeout detection needed - just periodic cleanup
    }
    
    private void cleanupTimedOutSagas() {
        long now = System.currentTimeMillis();
        activeSagas.entrySet().removeIf(entry -> {
            OrderSaga saga = entry.getValue();
            return saga.getStatus() == SagaStatus.TIMED_OUT && 
                   saga.getTimeoutTime() > 0 &&
                   (now - saga.getTimeoutTime()) > config.getSagaCleanupGracePeriodMs();
        });
    }
    
    /**
     * Centralized saga cleanup method - ensures all resources are properly released.
     * 
     * Design Decision: Centralize cleanup logic to prevent resource leaks and ensure
     * consistent cleanup behavior across all completion paths (success, rollback, timeout).
     */
    private void cleanupSaga(String sagaId) {
        // Remove saga from active tracking
        activeSagas.remove(sagaId);
        
        // Cancel and remove timeout task to prevent duplicate rollbacks
        ScheduledFuture<?> timeoutTask = sagaTimeouts.remove(sagaId);
        if (timeoutTask != null && !timeoutTask.isDone()) {
            timeoutTask.cancel(false);
            logger.fine(String.format("Cancelled timeout task for saga %s", sagaId));
        }
        
        logger.fine(String.format("Cleaned up saga %s", sagaId));
    }


    /**
     * Gracefully shuts down the marketplace, ensuring all resources are properly cleaned up.
     * 
     * Design Decision: Implement graceful shutdown to prevent data corruption and
     * resource leaks. Order is important: stop new work first, then clean up resources.
     * Allow time for in-flight operations to complete before forcing shutdown.
     */
    public void shutdown() {
        logger.info("Shutting down marketplace " + marketplaceId);
        shutdown = true; // Signal all threads to stop
        
        // Step 1: Interrupt all worker threads
        Thread[] threads = {responseHandlerThread, timeoutCheckerThread, clientHandlerThread};
        
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) {
                thread.interrupt(); // Request graceful shutdown
            }
        }
        
        // Step 2: Wait for threads to finish gracefully
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) {
                try {
                    thread.join(2000); // Give each thread 2 seconds to finish
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warning("Interrupted while waiting for thread to stop: " + thread.getName());
                }
            }
        }
        
        // Step 3: Close all ZeroMQ sockets
        for (ZMQ.Socket socket : sellerSockets.values()) {
            if (socket != null) {
                socket.close();
            }
        }
        
        if (clientSocket != null) {
            clientSocket.close();
        }
        
        if (notificationSocket != null) {
            notificationSocket.close();
        }
        
        // Step 4: Cancel all pending timeout tasks
        for (ScheduledFuture<?> timeoutTask : sagaTimeouts.values()) {
            if (timeoutTask != null && !timeoutTask.isDone()) {
                timeoutTask.cancel(false);
            }
        }
        sagaTimeouts.clear();
        
        // Step 5: Shutdown scheduled executor service
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                // Allow 5 seconds for pending tasks to complete
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow(); // Force shutdown if needed
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Step 5: Close ZeroMQ context (must be last)
        if (context != null) {
            context.close();
        }
        
        logger.info("Marketplace " + marketplaceId + " shutdown complete");
    }

    /**
     * Waits for all background threads to complete.
     * Used by main thread to keep the marketplace process running.
     */
    public void waitForCompletion() throws InterruptedException {
        Thread[] threads = {responseHandlerThread, timeoutCheckerThread, clientHandlerThread};
        
        for (Thread thread : threads) {
            if (thread != null) {
                thread.join();
            }
        }
    }

    public static void main(String[] args) {
        String marketplaceId = args.length > 0 ? args[0] : "MARKETPLACE-1";
        String configFile = args.length > 1 ? args[1] : "marketplace.properties";

        MarketplaceConfig config = MarketplaceConfig.loadFromFile(configFile);
        Marketplace marketplace = new Marketplace(marketplaceId, config);

        Runtime.getRuntime().addShutdownHook(new Thread(marketplace::shutdown));

        logger.info("Marketplace " + marketplaceId + " started");
        
        // Keep the main thread alive to let the background threads run
        try {
            marketplace.waitForCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Marketplace " + marketplaceId + " interrupted");
        }
    }
}