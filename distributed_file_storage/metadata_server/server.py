import os
import json
import time
import threading
import random
import requests
from flask import Flask, request, jsonify, send_file
from dotenv import load_dotenv
import sys
import base64

# Add the parent directory to sys.path to access utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from utils.healthcheck import HealthMonitor

# Load environment variables
load_dotenv()

# Configure the metadata server
PORT = 5000
METADATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata")
REPLICATION_FACTOR = 2  # Number of replicas for each chunk

# Create metadata directory if it doesn't exist
os.makedirs(METADATA_DIR, exist_ok=True)

# Define storage nodes
STORAGE_NODES = [
    {"id": "node1", "url": "http://localhost:5001"},
    {"id": "node2", "url": "http://localhost:5002"},
    {"id": "node3", "url": "http://localhost:5003"}
]

# Initialize Flask app
app = Flask(__name__)

# Health monitor for storage nodes
health_monitor = HealthMonitor(STORAGE_NODES)

def save_file_metadata(file_id, metadata):
    """Save file metadata to disk."""
    metadata_path = os.path.join(METADATA_DIR, f"{file_id}.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

def load_file_metadata(file_id):
    """Load file metadata from disk."""
    metadata_path = os.path.join(METADATA_DIR, f"{file_id}.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, 'r') as f:
            return json.load(f)
    return None

def get_all_file_metadata():
    """Get metadata for all files."""
    files = []
    for filename in os.listdir(METADATA_DIR):
        if filename.endswith('.json'):
            with open(os.path.join(METADATA_DIR, filename), 'r') as f:
                files.append(json.load(f))
    return files

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    """Endpoint for health checking."""
    return jsonify({"status": "healthy", "server": "metadata"}), 200

@app.route('/register_file', methods=['POST'])
def register_file():
    """
    Register a new file in the metadata server.
    
    Expected JSON payload:
    {
        "file_id": "unique_file_id",
        "filename": "original_filename.txt",
        "total_chunks": 3,
        "size": 1234567,
        "content_type": "text/plain"
    }
    """
    try:
        # Parse request data
        data = request.json
        
        if not all(k in data for k in ["file_id", "filename", "total_chunks"]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Create file metadata
        file_metadata = {
            "file_id": data["file_id"],
            "filename": data["filename"],
            "total_chunks": data["total_chunks"],
            "size": data.get("size", 0),
            "content_type": data.get("content_type", "application/octet-stream"),
            "chunks": {},
            "created_at": time.time(),
            "updated_at": time.time()
        }
        
        # Save metadata to disk
        save_file_metadata(data["file_id"], file_metadata)
        
        return jsonify({
            "status": "success",
            "message": "File registered successfully",
            "file_id": data["file_id"]
        }), 201
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/register_chunk', methods=['POST'])
def register_chunk():
    """
    Register a chunk's location in the metadata server.
    
    Expected JSON payload:
    {
        "file_id": "unique_file_id",
        "chunk_id": "unique_chunk_id",
        "index": 0,
        "size": 1048576,
        "nodes": ["node1", "node2"]
    }
    """
    try:
        # Parse request data
        data = request.json
        
        if not all(k in data for k in ["file_id", "chunk_id", "index", "nodes"]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Load file metadata
        file_metadata = load_file_metadata(data["file_id"])
        if file_metadata is None:
            return jsonify({"error": "File not found"}), 404
        
        # Update chunk information
        file_metadata["chunks"][str(data["index"])] = {
            "chunk_id": data["chunk_id"],
            "index": data["index"],
            "size": data.get("size", 0),
            "nodes": data["nodes"]
        }
        
        file_metadata["updated_at"] = time.time()
        
        # Save updated metadata
        save_file_metadata(data["file_id"], file_metadata)
        
        return jsonify({
            "status": "success",
            "message": "Chunk registered successfully",
            "file_id": data["file_id"],
            "chunk_id": data["chunk_id"]
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_upload_locations', methods=['GET'])
def get_upload_locations():
    """
    Get storage nodes for uploading chunks.
    Returns a list of healthy nodes based on the replication factor.
    """
    try:
        healthy_nodes = health_monitor.get_healthy_nodes()
        
        if len(healthy_nodes) < REPLICATION_FACTOR:
            return jsonify({
                "error": f"Not enough healthy nodes. Need {REPLICATION_FACTOR}, have {len(healthy_nodes)}"
            }), 503
        
        # Select random nodes for the upload
        selected_nodes = random.sample(healthy_nodes, REPLICATION_FACTOR)
        
        return jsonify({
            "status": "success",
            "nodes": selected_nodes
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_file_info/<file_id>', methods=['GET'])
def get_file_info(file_id):
    """Get metadata for a specific file."""
    try:
        file_metadata = load_file_metadata(file_id)
        if file_metadata is None:
            return jsonify({"error": "File not found"}), 404
        
        return jsonify({
            "status": "success",
            "file": file_metadata
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/list_files', methods=['GET'])
def list_files():
    """List all files in the system."""
    try:
        files = get_all_file_metadata()
        
        # Simplify the output by returning only basic info
        simplified_files = [{
            "file_id": file["file_id"],
            "filename": file["filename"],
            "size": file.get("size", 0),
            "total_chunks": file["total_chunks"],
            "created_at": file["created_at"]
        } for file in files]
        
        return jsonify({
            "status": "success",
            "files": simplified_files
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/chunk_locations/<file_id>/<int:chunk_index>', methods=['GET'])
def chunk_locations(file_id, chunk_index):
    """Get locations of a specific chunk for a file."""
    try:
        file_metadata = load_file_metadata(file_id)
        if file_metadata is None:
            return jsonify({"error": "File not found"}), 404
        
        chunk_key = str(chunk_index)
        if chunk_key not in file_metadata["chunks"]:
            return jsonify({"error": "Chunk not found"}), 404
        
        chunk_info = file_metadata["chunks"][chunk_key]
        nodes = chunk_info["nodes"]
        
        # Filter for healthy nodes
        healthy_nodes = []
        for node_id in nodes:
            if health_monitor.is_node_healthy(node_id):
                node_info = next((node for node in STORAGE_NODES if node["id"] == node_id), None)
                if node_info:
                    healthy_nodes.append(node_info)
        
        if not healthy_nodes:
            return jsonify({"error": "No healthy nodes found for this chunk"}), 503
        
        return jsonify({
            "status": "success",
            "chunk_id": chunk_info["chunk_id"],
            "nodes": healthy_nodes
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/node_status', methods=['GET'])
def node_status():
    """Get the health status of all storage nodes."""
    try:
        status = health_monitor.get_node_status()
        nodes_info = []
        
        for node in STORAGE_NODES:
            nodes_info.append({
                "node_id": node["id"],
                "url": node["url"],
                "healthy": status.get(node["id"], False)
            })
        
        return jsonify({
            "status": "success",
            "nodes": nodes_info
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete_file/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    """Delete a file and all its chunks from the system."""
    try:
        file_metadata = load_file_metadata(file_id)
        if file_metadata is None:
            return jsonify({"error": "File not found"}), 404
        
        # Delete each chunk from storage nodes
        for chunk_index, chunk_info in file_metadata["chunks"].items():
            chunk_id = chunk_info["chunk_id"]
            nodes = chunk_info["nodes"]
            
            for node_id in nodes:
                node_info = next((node for node in STORAGE_NODES if node["id"] == node_id), None)
                if node_info and health_monitor.is_node_healthy(node_id):
                    try:
                        requests.delete(f"{node_info['url']}/delete/{chunk_id}", timeout=5)
                    except requests.RequestException:
                        # Continue even if one node fails
                        pass
        
        # Delete metadata file
        metadata_path = os.path.join(METADATA_DIR, f"{file_id}.json")
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        
        return jsonify({
            "status": "success",
            "message": "File and all chunks deleted successfully"
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Start health monitoring
    health_monitor.start_monitoring()
    
    print(f"Metadata Server running on port {PORT}")
    print(f"Metadata directory: {METADATA_DIR}")
    print(f"Monitoring {len(STORAGE_NODES)} storage nodes")
    
    app.run(host='0.0.0.0', port=PORT)
    
    # Stop health monitoring when the server exits
    health_monitor.stop_monitoring() 