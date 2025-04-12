import os
import math
import hashlib
import uuid

# Default chunk size: 1MB
DEFAULT_CHUNK_SIZE = 1024 * 1024

class Chunker:
    """
    Class responsible for splitting files into chunks and reassembling them.
    """
    def __init__(self, chunk_size=DEFAULT_CHUNK_SIZE):
        """Initialize the chunker with a specific chunk size."""
        self.chunk_size = chunk_size

    def split_file(self, file_path):
        """
        Split a file into chunks of the specified size.
        
        Args:
            file_path (str): Path to the file to be split
        
        Returns:
            dict: A dictionary containing:
                - 'file_id': unique file identifier
                - 'total_chunks': total number of chunks
                - 'original_filename': name of the original file
                - 'chunks': list of dictionaries, each containing:
                  - 'chunk_id': unique identifier for the chunk
                  - 'data': binary data of the chunk
                  - 'size': size of the chunk in bytes
                  - 'index': position of the chunk in the sequence
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")
        
        file_size = os.path.getsize(file_path)
        total_chunks = math.ceil(file_size / self.chunk_size)
        
        # Create a unique file_id
        file_id = str(uuid.uuid4())
        
        # Get the original filename without path
        original_filename = os.path.basename(file_path)
        
        chunks = []
        
        with open(file_path, 'rb') as f:
            for i in range(total_chunks):
                # Read a chunk
                data = f.read(self.chunk_size)
                
                # Create a unique identifier for the chunk
                chunk_hash = hashlib.md5(data).hexdigest()
                chunk_id = f"{file_id}_{i}_{chunk_hash}"
                
                chunks.append({
                    'chunk_id': chunk_id,
                    'data': data,
                    'size': len(data),
                    'index': i
                })
        
        return {
            'file_id': file_id,
            'total_chunks': total_chunks,
            'original_filename': original_filename,
            'chunks': chunks
        }
    
    def reassemble_file(self, chunks, output_path):
        """
        Reassemble a file from its chunks.
        
        Args:
            chunks (list): List of chunk dictionaries, each containing 'data' and 'index'
            output_path (str): Path where the reassembled file will be written
        
        Returns:
            bool: True if successful, False otherwise
        """
        # Sort chunks by index to ensure correct order
        sorted_chunks = sorted(chunks, key=lambda x: x['index'])
        
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        
        # Write the chunks to the output file
        with open(output_path, 'wb') as f:
            for chunk in sorted_chunks:
                f.write(chunk['data'])
        
        return True 