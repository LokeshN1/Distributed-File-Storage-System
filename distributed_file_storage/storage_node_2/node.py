import os
import json
import base64
import tempfile
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure the storage node
NODE_ID = "node2"
PORT = 5002
STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chunks")

# Create storage directory if it doesn't exist
os.makedirs(STORAGE_DIR, exist_ok=True)

# Initialize Flask app
app = Flask(__name__)

@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    """Endpoint for health checking."""
    return jsonify({"status": "healthy", "node_id": NODE_ID}), 200

@app.route('/store', methods=['POST'])
def store_chunk():
    """
    Store a file chunk.
    
    Expected JSON payload:
    {
        "chunk_id": "unique_chunk_id",
        "data": "base64_encoded_data",
        "file_id": "unique_file_id",
        "index": 0
    }
    """
    try:
        # Parse request data
        data = request.json
        
        if not all(k in data for k in ["chunk_id", "data", "file_id", "index"]):
            return jsonify({"error": "Missing required fields"}), 400
        
        # Decode base64 data
        chunk_data = base64.b64decode(data["data"])
        
        # Create directory for this file if it doesn't exist
        file_dir = os.path.join(STORAGE_DIR, data["file_id"])
        os.makedirs(file_dir, exist_ok=True)
        
        # Store the chunk
        chunk_path = os.path.join(file_dir, data["chunk_id"])
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)
        
        # Store metadata about the chunk
        metadata = {
            "chunk_id": data["chunk_id"],
            "file_id": data["file_id"],
            "index": data["index"],
            "size": len(chunk_data)
        }
        
        metadata_path = chunk_path + ".meta"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)
        
        return jsonify({
            "status": "success", 
            "message": "Chunk stored successfully", 
            "node_id": NODE_ID,
            "chunk_id": data["chunk_id"]
        }), 201
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/retrieve/<chunk_id>', methods=['GET'])
def retrieve_chunk(chunk_id):
    """Retrieve a stored chunk by its ID."""
    try:
        # Find the chunk
        for root, dirs, files in os.walk(STORAGE_DIR):
            if chunk_id in files:
                chunk_path = os.path.join(root, chunk_id)
                metadata_path = chunk_path + ".meta"
                
                # Read the chunk data
                with open(chunk_path, 'rb') as f:
                    chunk_data = f.read()
                
                # Read the metadata
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                # Base64 encode the data for transmission
                encoded_data = base64.b64encode(chunk_data).decode('utf-8')
                
                return jsonify({
                    "status": "success",
                    "data": encoded_data,
                    "metadata": metadata,
                    "node_id": NODE_ID
                }), 200
        
        return jsonify({"error": "Chunk not found"}), 404
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/list', methods=['GET'])
def list_chunks():
    """List all chunks stored on this node."""
    try:
        chunks = []
        
        for root, dirs, files in os.walk(STORAGE_DIR):
            for file in files:
                if file.endswith('.meta'):
                    with open(os.path.join(root, file), 'r') as f:
                        metadata = json.load(f)
                        chunks.append(metadata)
        
        return jsonify({
            "status": "success",
            "node_id": NODE_ID,
            "chunks": chunks
        }), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete/<chunk_id>', methods=['DELETE'])
def delete_chunk(chunk_id):
    """Delete a chunk by its ID."""
    try:
        # Find the chunk
        for root, dirs, files in os.walk(STORAGE_DIR):
            if chunk_id in files:
                chunk_path = os.path.join(root, chunk_id)
                metadata_path = chunk_path + ".meta"
                
                # Delete the chunk and its metadata
                os.remove(chunk_path)
                if os.path.exists(metadata_path):
                    os.remove(metadata_path)
                
                return jsonify({
                    "status": "success",
                    "message": "Chunk deleted successfully",
                    "node_id": NODE_ID
                }), 200
        
        return jsonify({"error": "Chunk not found"}), 404
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print(f"Storage Node {NODE_ID} running on port {PORT}")
    print(f"Storage directory: {STORAGE_DIR}")
    app.run(host='0.0.0.0', port=PORT) 