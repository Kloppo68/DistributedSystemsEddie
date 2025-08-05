#!/bin/bash

# Pfade anpassen, wenn nötig
LOG_DIR="./logs"
PID_DIR="./pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

# Funktion zum Starten einer Komponente
start_component() {
    local name="$1"
    local jar="$2"
    shift 2
    local args=("$@")
    local logfile="$LOG_DIR/${name}.log"
    local pidfile="$PID_DIR/${name}.pid"

    echo "Starte $name ..."
    java -jar "$jar" "${args[@]}" >> "$logfile" 2>&1 &
    local pid=$!
    echo $pid > "$pidfile"
}

# Sellers starten (SELLER-1 bis SELLER-5)
for i in {1..5}; do
    JAR="seller.jar"
    INSTANCE="SELLER-$i"
    ENDPOINT="tcp://*:556$i"
    start_component "$INSTANCE" "$JAR" "$INSTANCE" "$ENDPOINT" "seller.properties"
done

# Marketplaces starten (MARKETPLACE-1 bis MARKETPLACE-2)
for i in {1..2}; do
    JAR="marketplace.jar"
    INSTANCE="MARKETPLACE-$i"
    start_component "$INSTANCE" "$JAR" "$INSTANCE" "marketplace.properties"
done

# Clients starten (CLIENT-1 bis CLIENT-3)
for i in {1..3}; do
    JAR="client.jar"
    INSTANCE="CLIENT-$i"
    start_component "$INSTANCE" "$JAR" "$INSTANCE" "client.properties"
done

echo -e "${GREEN}System erfolgreich gestartet!${NC}"
echo ""
echo "Komponenten:"
echo "  - 5 Sellers (SELLER-1 bis SELLER-5)"
echo "  - 2 Marketplaces (MARKETPLACE-1 bis MARKETPLACE-2)"  
echo "  - 3 Clients (CLIENT-1 bis CLIENT-3)"
echo ""
echo "Beende alles mit:"
echo "  ./stop-system.sh"
echo ""
echo "Logs anschauen mit:"
echo "  tail -f logs/MARKETPLACE-1.log"
echo "  tail -f logs/CLIENT-1.log"
