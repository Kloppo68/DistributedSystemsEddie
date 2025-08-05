package com.example.marketplace;

import com.example.common.Order;
import com.example.common.OrderItem;
import com.example.common.MessageType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SAGA Pattern implementation for managing distributed transactions across multiple sellers.
 * 
 * Architecture Overview:
 * - Represents a single distributed transaction (order) with multiple participants (sellers)
 * - Tracks transaction state through all phases: Reserve → Confirm/Cancel
 * - Implements timeout-based failure detection and recovery
 * - Ensures atomic semantics: all sellers succeed or all are rolled back
 * 
 * State Management:
 * - Uses thread-safe collections for concurrent access from multiple threads
 * - Tracks individual seller states independently for fine-grained control
 * - Maintains response tracking for retry logic and duplicate detection
 * 
 * Threading Model:
 * - All state modifications are synchronized to prevent race conditions
 * - Immutable snapshots provided for external access
 * - Thread-safe operations support concurrent marketplace and timeout threads
 */
public class OrderSaga {
    // Core transaction identification
    private final String sagaId;                           // Unique transaction identifier
    private final Order order;                              // Original customer order being processed
    private final long startTime;                           // Transaction start timestamp
    private final long sagaTimeout;                         // Maximum allowed transaction duration
    private volatile long timeoutTime;                      // When timeout occurred (0 if not timed out)
    private SagaStatus status;                              // Current transaction state

    // Seller state tracking - each seller progresses through phases independently
    private final Set<String> reservationsSent;             // Sellers we've sent RESERVE_REQUEST to
    private final Set<String> reservedSellers;              // Sellers that confirmed reservations
    private final Set<String> confirmedSellers;             // Sellers that confirmed final orders
    private final Set<String> cancelledSellers;             // Sellers that confirmed cancellations
    private final Map<String, Integer> retryCount;          // Retry attempts per seller
    
    // Response tracking for reliability and retry logic
    private final Map<String, Set<MessageType>> receivedResponses; // Which responses received from each seller
    private final Map<String, Long> lastMessageTime;        // Last response timestamp per seller
    
    // Client notification
    private String clientId;                                // Client that originated this order
    private org.zeromq.ZMQ.Socket clientSocket;            // Socket to send notifications to client

    /**
     * Creates a new SAGA transaction for the given order.
     * 
     * Design Decision: Initialize all state tracking collections as thread-safe
     * to support concurrent access from response handlers and timeout checkers.
     * Use concurrent collections to minimize lock contention while ensuring consistency.
     */
    public OrderSaga(String sagaId, Order order, long sagaTimeout) {
        this.sagaId = sagaId;
        this.order = order;
        this.startTime = System.currentTimeMillis();
        this.sagaTimeout = sagaTimeout;
        this.timeoutTime = 0; // Will be set when timeout detected
        this.status = SagaStatus.STARTED;

        // Initialize thread-safe collections for concurrent access
        this.reservationsSent = ConcurrentHashMap.newKeySet();
        this.reservedSellers = ConcurrentHashMap.newKeySet();
        this.confirmedSellers = ConcurrentHashMap.newKeySet();
        this.cancelledSellers = ConcurrentHashMap.newKeySet();
        this.retryCount = new ConcurrentHashMap<>();
        this.receivedResponses = new ConcurrentHashMap<>();
        this.lastMessageTime = new ConcurrentHashMap<>();
    }
    
    /**
     * Sets client information for notifications.
     */
    public void setClientInfo(String clientId, org.zeromq.ZMQ.Socket clientSocket) {
        this.clientId = clientId;
        this.clientSocket = clientSocket;
    }
    
    /**
     * Notifies the client about SAGA completion using PUSH socket.
     */
    public void notifyClient(SagaStatus finalStatus, String failureReason) {
        if (clientSocket == null || clientId == null) {
            return; // No client to notify
        }
        
        try {
            com.example.common.MessageType messageType;
            String sagaStatusStr;
            
            switch (finalStatus) {
                case COMPLETED:
                    messageType = com.example.common.MessageType.SAGA_COMPLETED;
                    sagaStatusStr = "COMPLETED";
                    break;
                case ROLLED_BACK:
                    messageType = com.example.common.MessageType.SAGA_FAILED;
                    sagaStatusStr = "ROLLED_BACK";
                    break;
                case TIMED_OUT:
                    messageType = com.example.common.MessageType.SAGA_TIMED_OUT;
                    sagaStatusStr = "TIMED_OUT";
                    break;
                default:
                    return; // Don't notify for non-terminal states
            }
            
            com.example.common.Message notification = new com.example.common.Message(
                messageType, sagaId, sagaStatusStr, failureReason);
            
            // Use PUSH socket - no need for ZMsg wrapper, just send JSON directly
            String json = notification.toJson();
            clientSocket.send(json.getBytes());
            
        } catch (Exception e) {
            // Log error but don't fail the SAGA because of notification issues
            System.err.println("Failed to notify client " + clientId + " about SAGA " + sagaId + ": " + e.getMessage());
        }
    }

    /**
     * State transition methods - all synchronized to ensure consistent state updates.
     * These methods track the progress of individual sellers through the SAGA phases.
     */
    
    /** Mark that we've sent a reservation request to this seller */
    public synchronized void markReservationSent(String sellerId) {
        reservationsSent.add(sellerId);
    }

    /** Mark that this seller has successfully reserved the requested items */
    public synchronized void markReserved(String sellerId) {
        reservedSellers.add(sellerId);
    }

    /** Mark that this seller has confirmed the final order */
    public synchronized void markConfirmed(String sellerId) {
        confirmedSellers.add(sellerId);
    }

    /** Mark that this seller has confirmed cancellation of their reservation */
    public synchronized void markCancelled(String sellerId) {
        cancelledSellers.add(sellerId);
    }

    /**
     * Phase completion checks - determine when to proceed to next phase or complete transaction.
     * These methods implement the core SAGA logic for phase transitions.
     */
    
    /** 
     * Check if all required sellers have completed reservation phase.
     * When true, can proceed to confirmation phase.
     */
    public synchronized boolean areAllReserved() {
        // Determine all unique sellers involved in this order
        Set<String> allSellers = new HashSet<>();
        for (OrderItem item : order.getItems()) {
            allSellers.add(item.getSellerId());
        }
        // All required sellers must have confirmed reservations
        return reservedSellers.containsAll(allSellers);
    }

    /** 
     * Check if all reserved sellers have confirmed their orders.
     * When true, SAGA can be marked as successfully completed.
     */
    public synchronized boolean areAllConfirmed() {
        // Only sellers who successfully reserved need to confirm
        return confirmedSellers.containsAll(reservedSellers);
    }

    /** 
     * Check if compensation (rollback) phase is complete.
     * When true, SAGA can be marked as successfully rolled back.
     */
    public synchronized boolean areAllCancelledOrNotReserved() {
        // Only need to cancel sellers who reserved but haven't confirmed
        Set<String> needsCancellation = new HashSet<>(reservedSellers);
        needsCancellation.removeAll(confirmedSellers); // Already confirmed items can't be cancelled
        return cancelledSellers.containsAll(needsCancellation);
    }

    /**
     * Retry and response tracking methods - support reliable message delivery.
     * These methods enable sophisticated retry logic and duplicate detection.
     */
    
    /** Increment retry counter for a specific seller */
    public synchronized void incrementRetryCount(String sellerId) {
        retryCount.merge(sellerId, 1, Integer::sum);
    }

    /** Get current retry count for a seller */
    public synchronized int getRetryCount(String sellerId) {
        return retryCount.getOrDefault(sellerId, 0);
    }
    
    /** Track that we received a specific response type from a seller */
    public synchronized void markResponseReceived(String sellerId, MessageType messageType) {
        receivedResponses.computeIfAbsent(sellerId, k -> ConcurrentHashMap.newKeySet()).add(messageType);
        lastMessageTime.put(sellerId, System.currentTimeMillis());
    }
    
    /** Check if we've received a specific response type from a seller */
    public synchronized boolean hasReceivedResponse(String sellerId, MessageType messageType) {
        Set<MessageType> responses = receivedResponses.get(sellerId);
        return responses != null && responses.contains(messageType);
    }
    
    /**
     * Timeout and lifecycle management methods.
     * These methods handle transaction lifetime and failure detection.
     */
    
    /** Check if this SAGA has exceeded its allowed execution time */
    public synchronized boolean isTimedOut() {
        return System.currentTimeMillis() - startTime > sagaTimeout;
    }
    
    /** Mark this SAGA as timed out and record when it happened */
    public synchronized void markTimedOut() {
        this.timeoutTime = System.currentTimeMillis();
        this.status = SagaStatus.TIMED_OUT;
    }
    
    /** Get timestamp when timeout was detected (0 if not timed out) */
    public synchronized long getTimeoutTime() {
        return timeoutTime;
    }
    
    /** Get count of unique sellers involved in this order */
    public synchronized int getSellerCount() {
        Set<String> allSellers = new HashSet<>();
        for (OrderItem item : order.getItems()) {
            allSellers.add(item.getSellerId());
        }
        return allSellers.size();
    }
    
    /** Check if this SAGA can still accept and process responses */
    public synchronized boolean canProcessResponse() {
        // Terminal states should not process new responses
        return status != SagaStatus.TIMED_OUT && 
               status != SagaStatus.COMPLETED && 
               status != SagaStatus.ROLLED_BACK;
    }

    /**
     * Accessor methods - provide safe access to SAGA state.
     * Returns immutable snapshots to prevent external modification of internal state.
     */
    
    /** Get unique identifier for this transaction */
    public String getSagaId() {
        return sagaId;
    }

    /** Get the original customer order being processed */
    public Order getOrder() {
        return order;
    }

    /** Get timestamp when this SAGA was created */
    public long getStartTime() {
        return startTime;
    }

    /** Get current SAGA status (thread-safe) */
    public synchronized SagaStatus getStatus() {
        return status;
    }

    /** Update SAGA status (thread-safe) */
    public synchronized void setStatus(SagaStatus status) {
        this.status = status;
    }

    /** Get immutable snapshot of sellers who have completed reservations */
    public Set<String> getReservedSellers() {
        return new HashSet<>(reservedSellers);
    }

    /** Get immutable snapshot of sellers who have confirmed orders */
    public Set<String> getConfirmedSellers() {
        return new HashSet<>(confirmedSellers);
    }

    /** Get immutable snapshot of sellers who have confirmed cancellations */
    public Set<String> getCancelledSellers() {
        return new HashSet<>(cancelledSellers);
    }
}

/**
 * SAGA transaction states representing the lifecycle of a distributed transaction.
 * 
 * State Transitions:
 * STARTED → RESERVING (when reservation requests sent)
 * RESERVING → CONFIRMING (when all reservations successful)
 * RESERVING → ROLLING_BACK (when any reservation fails)
 * CONFIRMING → COMPLETED (when all confirmations received)
 * CONFIRMING → ROLLING_BACK (when any confirmation fails)
 * ROLLING_BACK → ROLLED_BACK (when all cancellations confirmed)
 * Any state → TIMED_OUT (when transaction exceeds timeout)
 * TIMED_OUT → ROLLING_BACK (timeout triggers rollback)
 */
enum SagaStatus {
    STARTED,        // Initial state - transaction created
    RESERVING,      // Phase 1 - collecting reservations from sellers
    CONFIRMING,     // Phase 2 - confirming reservations to complete sale
    COMPLETED,      // Terminal state - transaction successful
    ROLLING_BACK,   // Compensation - cancelling reservations
    ROLLED_BACK,    // Terminal state - transaction rolled back successfully
    TIMED_OUT,      // Intermediate state - timeout detected, rollback needed
    FAILED          // Terminal state - unrecoverable error
}