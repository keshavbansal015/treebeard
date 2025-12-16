#!/bin/bash
# This script requires root privileges to manage systemd services and files.

# --- Configuration: File Paths ---
# !!! EDIT THESE PATHS !!!
# Ensure these paths point to the ACTUAL, fully rendered .service files
# on your local machine (no Jinja2 variables remaining).

ORAMNODE_FILE="~/treebeard-oramnode-0-0.service"
SHARDNODE_FILE="~/treebeard-shardnode-0-0.service"
ROUTER_FILE="~/treebeard-router-0.service"

# --- End Configuration ---

# Function to create and start a systemd service by copying a local file
deploy_and_start() {
    LOCAL_SRC_PATH="$1"
    SERVICE_NAME="$2"

    echo "--- Deploying: ${SERVICE_NAME}.service from ${LOCAL_SRC_PATH} ---"

    if [ ! -f "${LOCAL_SRC_PATH}" ]; then
        echo "ERROR: Local source file not found at ${LOCAL_SRC_PATH}. Skipping deployment."
        return 1
    fi  

    # 1. Copy/Replace the service file (Requires sudo)
    # The destination is /lib/systemd/system/
    sudo cp "${LOCAL_SRC_PATH}" "/lib/systemd/system/${SERVICE_NAME}.service"

    # 2. Daemon Reload (Required for new/modified files)
    sudo systemctl daemon-reload
    # 3. Enable and Restart the service (Ensures 'state: restarted')
    sudo systemctl enable "${SERVICE_NAME}.service"
    sudo systemctl restart "${SERVICE_NAME}.service"
    echo "${SERVICE_NAME} started successfully. Check status with: sudo systemctl status ${SERVICE_NAME}"
}

# --- 1. Redis Services (Lines 101-112) ---
# Stop all Redis services first (Ignore the 'redis-0.conf.service' error)
echo "--- Stopping previous Redis services (if any) ---"
sudo systemctl stop redis* 2>/dev/null || true

# Start the services (assuming redis-0.service exists from previous manual steps)
echo "--- Restarting Redis service: redis-0 ---"
sudo systemctl daemon-reload
sudo systemctl restart redis-0

# --- 2. ORAM Node Services (Lines 166-185) ---
# Stop all previous treebeard services before starting
echo "--- Stopping previous Treebeard services ---"
sudo systemctl stop treebeard-* 2>/dev/null || true

# Create and Start ORAM Node 0-0
deploy_and_start "${ORAMNODE_FILE}" "treebeard-oramnode-0-0"
# If you had a pause/sleep here, you would add: sleep 1s

# --- 3. Shard Node Services (Lines 187-202) ---
# Create and Start Shard Node 0-0
deploy_and_start "${SHARDNODE_FILE}" "treebeard-shardnode-0-0"
# If you had a pause/sleep here, you would add: sleep 1s

# --- 4. Router Services (Lines 204-217) ---
# Create and Start Router 0
deploy_and_start "${ROUTER_FILE}" "treebeard-router-0"

echo "--- Deployment cycle complete. ---"