#!/bin/bash

echo "=== Stoppe alle Marketplace/Seller/Client Instanzen ==="

if [ ! -d pids ]; then
    echo "Kein pid-Verzeichnis gefunden. Nichts zu stoppen."
    exit 0
fi

for pidfile in pids/*.pid; do
    [ -e "$pidfile" ] || continue
    pid=$(cat "$pidfile")
    name=$(basename "$pidfile" .pid)

    if kill -0 "$pid" > /dev/null 2>&1; then
        echo "Beende $name (PID: $pid)..."
        kill "$pid"
        wait "$pid" 2>/dev/null
        echo "$name wurde beendet."
    else
        echo "$name läuft nicht (mehr)."
    fi

    rm -f "$pidfile"
done

# Kill any remaining marketplace Java processes
echo "Cleaning up any remaining processes..."
pkill -f "marketplace.jar" 2>/dev/null || true
pkill -f "client.jar" 2>/dev/null || true
pkill -f "seller.jar" 2>/dev/null || true

# Wait for processes to fully terminate
sleep 2

echo "Alle Prozesse gestoppt."
