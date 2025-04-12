import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class HealthMonitor:
    """
    Class responsible for monitoring the health of storage nodes.
    """
    def __init__(self, nodes, check_interval=30):
        """
        Initialize the health monitor with a list of nodes and check interval.
        
        Args:
            nodes (list): List of dictionaries with node information (id, url)
            check_interval (int): Interval in seconds between health checks
        """
        self.nodes = nodes
        self.check_interval = check_interval
        self.node_status = {node['id']: False for node in nodes}  # Initially all nodes are down
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Start the health monitoring in a background thread."""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.stop_event.clear()
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            return True
        return False
    
    def stop_monitoring(self):
        """Stop the health monitoring."""
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.stop_event.set()
            self.monitor_thread.join(timeout=10)
            return True
        return False
    
    def _check_node_health(self, node):
        """
        Check the health of a single node.
        
        Args:
            node (dict): Dictionary with node information (id, url)
        
        Returns:
            tuple: (node_id, is_healthy)
        """
        try:
            response = requests.get(f"{node['url']}/healthcheck", timeout=5)
            return node['id'], response.status_code == 200
        except requests.RequestException:
            return node['id'], False
    
    def _monitor_loop(self):
        """Main monitoring loop that periodically checks node health."""
        while not self.stop_event.is_set():
            self._check_all_nodes()
            
            # Wait for the next check interval or until the stop event is set
            self.stop_event.wait(self.check_interval)
    
    def _check_all_nodes(self):
        """Check the health of all nodes in parallel."""
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            futures = [executor.submit(self._check_node_health, node) for node in self.nodes]
            
            for future in as_completed(futures):
                node_id, is_healthy = future.result()
                with self.lock:
                    self.node_status[node_id] = is_healthy
    
    def get_node_status(self):
        """
        Get the current status of all nodes.
        
        Returns:
            dict: Dictionary with node IDs as keys and boolean status as values
        """
        with self.lock:
            return self.node_status.copy()
    
    def is_node_healthy(self, node_id):
        """
        Check if a specific node is healthy.
        
        Args:
            node_id (str): ID of the node to check
        
        Returns:
            bool: True if node is healthy, False otherwise
        """
        with self.lock:
            return self.node_status.get(node_id, False)
    
    def get_healthy_nodes(self):
        """
        Get a list of all healthy nodes.
        
        Returns:
            list: List of dictionaries with information about healthy nodes
        """
        healthy_nodes = []
        with self.lock:
            for node in self.nodes:
                if self.node_status.get(node['id'], False):
                    healthy_nodes.append(node)
        return healthy_nodes 