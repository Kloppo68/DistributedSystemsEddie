package com.example.common;

public enum MessageType {
    // Request types
    RESERVE_REQUEST,
    CONFIRM_REQUEST,
    CANCEL_REQUEST,
    RESTOCK_REQUEST,
    ORDER_REQUEST,

    // Response types
    RESERVE_RESPONSE,
    CONFIRM_RESPONSE,
    CANCEL_RESPONSE,
    RESTOCK_RESPONSE,
    ORDER_RESPONSE,

    // SAGA completion types
    SAGA_COMPLETED,
    SAGA_FAILED,
    SAGA_TIMED_OUT,

    // Error types
    ERROR_RESPONSE
}