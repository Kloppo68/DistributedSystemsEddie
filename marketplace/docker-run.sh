#!/bin/bash

# Docker build and run script for the Distributed Marketplace System

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage
usage() {
    echo "Usage: $0 [build|start|stop|restart|logs|clean]"
    echo "  build   - Build Docker images"
    echo "  start   - Start all services"
    echo "  stop    - Stop all services"
    echo "  restart - Restart all services"
    echo "  logs    - Show logs (follow mode)"
    echo "  clean   - Stop services and remove containers/images"
    exit 1
}

# Function to build images
build() {
    buildMarketplace
    buildSeller
    buildClient
}

buildMarketplace() {
    echo -e "${YELLOW}Building Docker image for marketplace...${NC}"
    docker build -t marketplace:latest -f Dockerfile.marketplace .
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Build successful!${NC}"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

buildSeller() {
    echo -e "${YELLOW}Building Docker image for seller...${NC}"
        docker build -t marketplace-seller:latest -f Dockerfile.seller .
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Build successful!${NC}"
        else
            echo -e "${RED}Build failed!${NC}"
            exit 1
        fi
}

buildClient() {
    echo -e "${YELLOW}Building Docker image for client...${NC}"
        docker build -t marketplace-client:latest -f Dockerfile.client .
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}Build successful!${NC}"
        else
            echo -e "${RED}Build failed!${NC}"
            exit 1
        fi
}

# Function to start services
start() {
    echo -e "${YELLOW}Starting services...${NC}"

    docker compose up -d
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Services started successfully!${NC}"
        echo ""
        echo -e "${BLUE}Services running:${NC}"
        docker ps
        echo ""
        echo -e "${BLUE}To view logs: $0 logs${NC}"
    else
        echo -e "${RED}Failed to start services!${NC}"
        exit 1
    fi
}

# Function to stop services
stop() {
    echo -e "${YELLOW}Stopping services...${NC}"
    docker compose down
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Services stopped successfully!${NC}"
    else
        echo -e "${RED}Failed to stop services!${NC}"
        exit 1
    fi
}

# Function to restart services
restart() {
    stop
    sleep 2
    start
}

# Function to show logs
logs() {
    echo -e "${BLUE}Showing logs (press Ctrl+C to exit)...${NC}"
    docker compose logs -f
}

# Function to clean up
clean() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker compose down -v --rmi all
    rm -rf logs/*
    echo -e "${GREEN}Cleanup complete!${NC}"
}

# Main script
if [ $# -eq 0 ]; then
    usage
fi

case "$1" in
    build)
        build
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    logs)
        logs
        ;;
    clean)
        clean
        ;;
    *)
        usage
        ;;
esac
