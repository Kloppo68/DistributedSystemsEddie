# Distributed Marketplace - Setup Overview

## General Purpose

This distributed marketplace simulates coordinated orders across multiple sellers via redundant marketplace services using the SAGA pattern. It demonstrates fault tolerance, compensation on failure, and high concurrency through Java virtual threads. Components communicate over ZeroMQ.

## Roles

- **Seller:** Manages inventory, participates in multi-step orders (reserve/confirm/cancel), and can simulate failures.  
- **Marketplace:** Coordinates SAGA transactions across sellers to execute client orders reliably.  
- **Client:** Generates orders, submits them to marketplaces, and receives notifications about order outcomes.

## Setup Scripts

### Make helper scripts executable
```bash
chmod +x start-system-new.sh
chmod +x start-system-silent.sh
chmod +x stop-system-new.sh
chmod +x docker-run.sh
```

### Local execution scripts

```bash
start-system-new.sh
```

Starts all components in foreground (logs shown in the terminal) and tracks their PIDs.

```bash
stop-system-new.sh
```

Stops previously started local components by reading stored PIDs and terminating them.

To step the separate components in different terminal, using Java, use the following commands:

#### SELLER-1
```bash
java -jar seller.jar SELLER-1 'tcp://*:5561' seller.properties | tee -a logs/SELLER-1.log
```

#### SELLER-2
```bash
java -jar seller.jar SELLER-2 'tcp://*:5562' seller.properties | tee -a logs/SELLER-2.log
```

#### SELLER-3
```bash
java -jar seller.jar SELLER-3 'tcp://*:5563' seller.properties | tee -a logs/SELLER-3.log
```

#### SELLER-4
```bash
java -jar seller.jar SELLER-4 'tcp://*:5564' seller.properties | tee -a logs/SELLER-4.log
```

#### SELLER-5
```bash
java -jar seller.jar SELLER-5 'tcp://*:5565' seller.properties | tee -a logs/SELLER-5.log
```

#### MARKETPLACE-1
```bash
java -jar marketplace.jar MARKETPLACE-1 marketplace.properties | tee -a logs/MARKETPLACE-1.log
```

#### MARKETPLACE-2
```bash
java -jar marketplace.jar MARKETPLACE-2 marketplace.properties | tee -a logs/MARKETPLACE-2.log
```

#### CLIENT-1
```bash
java -jar client.jar CLIENT-1 client.properties | tee -a logs/CLIENT-1.log
```

### Docker orchestration

#### Build images
```bash
./docker-run.sh build
```
Builds all Docker images (sellers, marketplaces, client).

#### Start system
```bash
./docker-run.sh start
```
Brings up the full system (clients, sellers, marketplaces) via Docker Compose or scripted orchestration.

#### View logs
```bash
./docker-run.sh logs
```
Streams container logs.

#### Stop system
```bash
./docker-run.sh stop
```
Stops all containers cleanly.

## Docker Manual Setup (optional)

Create shared network:
```bash
docker network create marketplace-network
```

Run individual containers:

### SELLER-1
```bash
docker run --rm --name seller-1 \
  -e INSTANCE_ID=SELLER-1 \
  -e ENDPOINT=tcp://*:5561 \
  -p 5561:5561 \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/seller-docker.properties:/app/seller.properties:ro" \
  marketplace-seller:latest
```

### SELLER-2
```bash
docker run --rm --name seller-2 \
  -e INSTANCE_ID=SELLER-2 \
  -e ENDPOINT=tcp://*:5562 \
  -p 5562:5562 \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/seller-docker.properties:/app/seller.properties:ro" \
  marketplace-seller:latest
```

### SELLER-3
```bash
docker run --rm --name seller-3 \
  -e INSTANCE_ID=SELLER-3 \
  -e ENDPOINT=tcp://*:5563 \
  -p 5563:5563 \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/seller-docker.properties:/app/seller.properties:ro" \
  marketplace-seller:latest
```

### SELLER-4
```bash
docker run --rm --name seller-4 \
  -e INSTANCE_ID=SELLER-4 \
  -e ENDPOINT=tcp://*:5564 \
  -p 5564:5564 \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/seller-docker.properties:/app/seller.properties:ro" \
  marketplace-seller:latest
```

### SELLER-5
```bash
docker run --rm --name seller-5 \
  -e INSTANCE_ID=SELLER-5 \
  -e ENDPOINT=tcp://*:5565 \
  -p 5565:5565 \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/seller-docker.properties:/app/seller.properties:ro" \
  marketplace-seller:latest
```

### MARKETPLACE-1
```bash
docker run --rm --name marketplace-1 \
  -e INSTANCE_ID=MARKETPLACE-1 \
  -e SERVICE_TYPE=marketplace \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/marketplace-docker.properties:/app/marketplace.properties:ro" \
  marketplace:latest
```

### MARKETPLACE-2
```bash
docker run --rm --name marketplace-2 \
  -e INSTANCE_ID=MARKETPLACE-2 \
  -e SERVICE_TYPE=marketplace \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/marketplace-docker.properties:/app/marketplace.properties:ro" \
  marketplace:latest
```

```bash
docker run --rm --name client-1 \
  -e INSTANCE_ID=CLIENT-1 \
  -p 5555:5555 -p 5556:5556 \
  --network marketplace-network \
  -v "$(pwd)/logs:/app/logs" \
  -v "$(pwd)/client-docker.properties:/app/client.properties:ro" \
  marketplace-client:latest
```

## Stopping the System

### Local
```bash
./stop-system-new.sh
```

### Docker (explicit)
```bash
docker stop client-1 marketplace-1 marketplace-2 seller-1 seller-2 seller-3 seller-4 seller-5
```
