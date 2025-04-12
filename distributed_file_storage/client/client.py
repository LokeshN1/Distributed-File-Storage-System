#!/usr/bin/env python3

import os
import sys
import time
import json
import base64
import requests
import argparse
import mimetypes
from tabulate import tabulate

# Add the parent directory to sys.path to access utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from utils.chunker import Chunker

# Configuration
METADATA_SERVER = "http://localhost:5000"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def check_server_status():
    """Check if the metadata server is available."""
    try:
        response = requests.get(f"{METADATA_SERVER}/healthcheck", timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False

def upload_file(file_path):
    """
    Upload a file to the distributed storage system.
    
    Args:
        file_path (str): Path to the file to upload
    """
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found")
        return False
    
    # Check server status
    if not check_server_status():
        print("Error: Metadata server is not available")
        return False
    
    # Split the file into chunks
    print(f"Splitting {file_path} into chunks...")
    chunker = Chunker()
    chunk_info = chunker.split_file(file_path)
    
    file_id = chunk_info["file_id"]
    total_chunks = chunk_info["total_chunks"]
    original_filename = chunk_info["original_filename"]
    
    # Register the file with the metadata server
    print(f"Registering file {original_filename} with metadata server...")
    file_size = os.path.getsize(file_path)
    content_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    
    response = requests.post(f"{METADATA_SERVER}/register_file", json={
        "file_id": file_id,
        "filename": original_filename,
        "total_chunks": total_chunks,
        "size": file_size,
        "content_type": content_type
    })
    
    if response.status_code != 201:
        print(f"Error registering file: {response.json().get('error', 'Unknown error')}")
        return False
    
    # Upload each chunk to storage nodes
    print(f"Uploading {total_chunks} chunks with replication...")
    for chunk in chunk_info["chunks"]:
        # Get upload locations
        locations_response = requests.get(f"{METADATA_SERVER}/get_upload_locations")
        if locations_response.status_code != 200:
            print(f"Error getting upload locations: {locations_response.json().get('error', 'Unknown error')}")
            continue
        
        upload_nodes = locations_response.json()["nodes"]
        successful_nodes = []
        
        # Upload to each node
        for node in upload_nodes:
            chunk_data = base64.b64encode(chunk["data"]).decode('utf-8')
            
            try:
                store_response = requests.post(f"{node['url']}/store", json={
                    "chunk_id": chunk["chunk_id"],
                    "data": chunk_data,
                    "file_id": file_id,
                    "index": chunk["index"]
                }, timeout=30)  # Higher timeout for large chunks
                
                if store_response.status_code == 201:
                    successful_nodes.append(node["id"])
                    print(f"  Chunk {chunk['index'] + 1}/{total_chunks} uploaded to node {node['id']}")
                else:
                    print(f"  Failed to upload chunk {chunk['index'] + 1}/{total_chunks} to node {node['id']}")
            
            except requests.RequestException as e:
                print(f"  Error uploading to node {node['id']}: {str(e)}")
        
        # Register chunk location with metadata server
        if successful_nodes:
            register_response = requests.post(f"{METADATA_SERVER}/register_chunk", json={
                "file_id": file_id,
                "chunk_id": chunk["chunk_id"],
                "index": chunk["index"],
                "size": chunk["size"],
                "nodes": successful_nodes
            })
            
            if register_response.status_code != 200:
                print(f"  Error registering chunk {chunk['index'] + 1}/{total_chunks}: {register_response.json().get('error', 'Unknown error')}")
        else:
            print(f"  Failed to upload chunk {chunk['index'] + 1}/{total_chunks} to any node")
    
    print(f"\nFile {original_filename} (ID: {file_id}) uploaded successfully")
    return True

def download_file(file_id, output_path=None):
    """
    Download a file from the distributed storage system.
    
    Args:
        file_id (str): ID of the file to download
        output_path (str, optional): Path where to save the downloaded file
    """
    # Check server status
    if not check_server_status():
        print("Error: Metadata server is not available")
        return False
    
    # Get file metadata
    response = requests.get(f"{METADATA_SERVER}/get_file_info/{file_id}")
    if response.status_code != 200:
        print(f"Error: {response.json().get('error', 'File not found')}")
        return False
    
    file_metadata = response.json()["file"]
    original_filename = file_metadata["filename"]
    total_chunks = file_metadata["total_chunks"]
    
    if not output_path:
        output_path = os.path.join(DOWNLOAD_DIR, original_filename)
    
    print(f"Downloading {original_filename} (ID: {file_id}) with {total_chunks} chunks...")
    
    # Prepare to reconstruct the file
    chunks = []
    
    # Download each chunk
    for i in range(total_chunks):
        chunk_response = requests.get(f"{METADATA_SERVER}/chunk_locations/{file_id}/{i}")
        if chunk_response.status_code != 200:
            print(f"  Error locating chunk {i+1}/{total_chunks}: {chunk_response.json().get('error', 'Unknown error')}")
            continue
        
        chunk_info = chunk_response.json()
        chunk_id = chunk_info["chunk_id"]
        available_nodes = chunk_info["nodes"]
        
        if not available_nodes:
            print(f"  No available nodes for chunk {i+1}/{total_chunks}")
            return False
        
        # Try each node until successful
        chunk_data = None
        for node in available_nodes:
            try:
                retrieve_response = requests.get(f"{node['url']}/retrieve/{chunk_id}")
                if retrieve_response.status_code == 200:
                    encoded_data = retrieve_response.json()["data"]
                    chunk_data = base64.b64decode(encoded_data)
                    node_id = node["id"]
                    print(f"  Retrieved chunk {i+1}/{total_chunks} from node {node_id}")
                    break
            except requests.RequestException:
                continue
        
        if chunk_data is None:
            print(f"  Failed to retrieve chunk {i+1}/{total_chunks} from any node")
            return False
        
        chunks.append({
            "data": chunk_data,
            "index": i
        })
    
    # Reassemble the file
    print(f"Reassembling file from {len(chunks)} chunks...")
    chunker = Chunker()
    result = chunker.reassemble_file(chunks, output_path)
    
    if result:
        print(f"File downloaded successfully to {output_path}")
        return True
    else:
        print("Error reassembling the file")
        return False

def list_files():
    """List all files in the distributed storage system."""
    # Check server status
    if not check_server_status():
        print("Error: Metadata server is not available")
        return False
    
    response = requests.get(f"{METADATA_SERVER}/list_files")
    if response.status_code != 200:
        print(f"Error listing files: {response.json().get('error', 'Unknown error')}")
        return False
    
    files = response.json()["files"]
    
    if not files:
        print("No files found in the system")
        return True
    
    # Format file information for display
    file_info = []
    for file in files:
        # Convert timestamp to human-readable format
        created_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file["created_at"]))
        
        # Format file size
        size = file["size"]
        if size < 1024:
            size_str = f"{size} B"
        elif size < 1024 * 1024:
            size_str = f"{size / 1024:.1f} KB"
        else:
            size_str = f"{size / (1024 * 1024):.1f} MB"
        
        file_info.append([
            file["filename"], 
            file["file_id"], 
            size_str, 
            file["total_chunks"],
            created_time
        ])
    
    # Display file list
    headers = ["Filename", "File ID", "Size", "Chunks", "Created"]
    print(tabulate(file_info, headers=headers, tablefmt="pretty"))
    return True

def delete_file(file_id):
    """Delete a file from the distributed storage system."""
    # Check server status
    if not check_server_status():
        print("Error: Metadata server is not available")
        return False
    
    # Get file info first to display filename
    info_response = requests.get(f"{METADATA_SERVER}/get_file_info/{file_id}")
    if info_response.status_code != 200:
        print(f"Error: {info_response.json().get('error', 'File not found')}")
        return False
    
    filename = info_response.json()["file"]["filename"]
    
    # Confirm deletion
    confirm = input(f"Are you sure you want to delete '{filename}' (ID: {file_id})? (y/n): ")
    if confirm.lower() != 'y':
        print("Deletion cancelled")
        return False
    
    # Send delete request
    response = requests.delete(f"{METADATA_SERVER}/delete_file/{file_id}")
    if response.status_code != 200:
        print(f"Error deleting file: {response.json().get('error', 'Unknown error')}")
        return False
    
    print(f"File '{filename}' (ID: {file_id}) deleted successfully")
    return True

def check_node_status():
    """Check the status of all storage nodes."""
    # Check server status
    if not check_server_status():
        print("Error: Metadata server is not available")
        return False
    
    response = requests.get(f"{METADATA_SERVER}/node_status")
    if response.status_code != 200:
        print(f"Error checking node status: {response.json().get('error', 'Unknown error')}")
        return False
    
    nodes = response.json()["nodes"]
    
    # Format node information for display
    node_info = []
    for node in nodes:
        status = "✅ ONLINE" if node["healthy"] else "❌ OFFLINE"
        node_info.append([
            node["node_id"],
            node["url"],
            status
        ])
    
    # Display node status
    headers = ["Node ID", "URL", "Status"]
    print(tabulate(node_info, headers=headers, tablefmt="pretty"))
    return True

def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(description="Distributed File Storage Client")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Upload a file")
    upload_parser.add_argument("file_path", help="Path to the file to upload")
    
    # Download command
    download_parser = subparsers.add_parser("download", help="Download a file")
    download_parser.add_argument("file_id", help="ID of the file to download")
    download_parser.add_argument("--output", help="Path where to save the downloaded file")
    
    # List command
    subparsers.add_parser("list", help="List all files")
    
    # Delete command
    delete_parser = subparsers.add_parser("delete", help="Delete a file")
    delete_parser.add_argument("file_id", help="ID of the file to delete")
    
    # Status command
    subparsers.add_parser("status", help="Check node status")
    
    args = parser.parse_args()
    
    # Execute the appropriate command
    if args.command == "upload":
        upload_file(args.file_path)
    elif args.command == "download":
        download_file(args.file_id, args.output)
    elif args.command == "list":
        list_files()
    elif args.command == "delete":
        delete_file(args.file_id)
    elif args.command == "status":
        check_node_status()
    else:
        parser.print_help()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1) 