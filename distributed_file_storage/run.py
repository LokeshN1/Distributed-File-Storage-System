#!/usr/bin/env python3

import os
import sys
import time
import signal
import subprocess
import argparse

# Paths to the components
METADATA_SERVER = os.path.join("metadata_server", "server.py")
STORAGE_NODE_1 = os.path.join("storage_node_1", "node.py")
STORAGE_NODE_2 = os.path.join("storage_node_2", "node.py")
STORAGE_NODE_3 = os.path.join("storage_node_3", "node.py")

# Process list to track all running processes
processes = []

def signal_handler(sig, frame):
    """Handle Ctrl+C to clean up processes."""
    print("\nShutting down all components...")
    for process in processes:
        if process.poll() is None:  # If process is still running
            process.terminate()
    sys.exit(0)

def start_component(component_path, name):
    """Start a component in a separate process."""
    print(f"Starting {name}...")
    process = subprocess.Popen([sys.executable, component_path])
    processes.append(process)
    return process

def main():
    parser = argparse.ArgumentParser(description="Run the Distributed File Storage System")
    parser.add_argument("--metadata-only", action="store_true", help="Run only the metadata server")
    parser.add_argument("--nodes-only", action="store_true", help="Run only the storage nodes")
    args = parser.parse_args()
    
    # Register signal handler for clean shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Start metadata server if requested
        if not args.nodes_only:
            metadata_process = start_component(METADATA_SERVER, "Metadata Server")
            # Give it time to initialize
            time.sleep(2)
        
        # Start storage nodes if requested
        if not args.metadata_only:
            node1_process = start_component(STORAGE_NODE_1, "Storage Node 1")
            node2_process = start_component(STORAGE_NODE_2, "Storage Node 2")
            node3_process = start_component(STORAGE_NODE_3, "Storage Node 3")
        
        print("\nAll components started!")
        print("Press Ctrl+C to shut down")
        
        # Wait for all processes to complete (which they won't unless terminated)
        for process in processes:
            process.wait()
    
    except Exception as e:
        print(f"Error starting components: {str(e)}")
        # Clean up any running processes
        for process in processes:
            if process.poll() is None:
                process.terminate()
        sys.exit(1)

if __name__ == "__main__":
    main() 