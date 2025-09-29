"""
Raft Cluster Management

This module provides cluster-level management for Raft nodes including:
- Cluster initialization and configuration
- Inter-node communication setup
- Leader discovery and client request routing
- Failure simulation and testing utilities
- Cluster health monitoring

Educational implementation for testing and demonstrating Raft consensus.
"""

import time
import logging
import threading
from typing import List, Dict, Optional, Any, Set
from .node import RaftNode
from .messages import NodeState, ClusterStats


class RaftCluster:
    """
    Educational Raft cluster for testing distributed consensus.
    
    Manages multiple Raft nodes and provides cluster-level operations:
    - Automatic leader election
    - Client request routing to leader
    - Node failure simulation
    - Cluster state monitoring
    - Performance testing utilities
    """
    
    def __init__(self, num_nodes: int = 5, 
                 election_timeout_range: tuple = (0.15, 0.30),
                 heartbeat_interval: float = 0.05):
        """
        Initialize Raft cluster with specified number of nodes.
        
        Args:
            num_nodes: Number of nodes in the cluster (should be odd)
            election_timeout_range: Min/max election timeout in seconds
            heartbeat_interval: Leader heartbeat interval in seconds
        """
        if num_nodes < 3:
            raise ValueError("Cluster must have at least 3 nodes for fault tolerance")
        
        if num_nodes % 2 == 0:
            logging.warning("Even number of nodes reduces fault tolerance")
        
        self.num_nodes = num_nodes
        self.nodes: Dict[str, RaftNode] = {}
        self.failed_nodes: Set[str] = set()
        
        # Create node identifiers
        node_ids = [f"node_{i}" for i in range(num_nodes)]
        
        # Initialize all nodes
        for node_id in node_ids:
            node = RaftNode(
                node_id=node_id,
                cluster_nodes=node_ids,
                election_timeout_range=election_timeout_range,
                heartbeat_interval=heartbeat_interval
            )
            self.nodes[node_id] = node
        
        # Set up inter-node communication
        self._setup_rpc_handlers()
        
        # Cluster state
        self.lock = threading.Lock()
        self.request_count = 0
        
        self.logger = logging.getLogger("raft.cluster")
        self.logger.info(f"Raft cluster initialized with {num_nodes} nodes: {node_ids}")
        
        # Allow time for initial leader election
        time.sleep(1.0)
    
    def _setup_rpc_handlers(self) -> None:
        """Set up RPC handlers between all nodes for communication"""
        for node_id, node in self.nodes.items():
            for other_node_id, other_node in self.nodes.items():
                if node_id != other_node_id:
                    node.add_rpc_handler(other_node_id, other_node)
    
    def get_leader(self) -> Optional[str]:
        """
        Find current cluster leader.
        
        Returns:
            str: Node ID of current leader, or None if no leader
        """
        with self.lock:
            for node_id, node in self.nodes.items():
                if node_id not in self.failed_nodes and node.is_leader():
                    return node_id
            return None
    
    def wait_for_leader_election(self, timeout: float = 5.0) -> bool:
        """
        Wait for a leader to be elected.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if leader was elected within timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            leader = self.get_leader()
            if leader is not None:
                self.logger.info(f"Leader elected: {leader}")
                return True
            time.sleep(0.1)
        
        self.logger.warning("No leader elected within timeout")
        return False
    
    def wait_for_convergence(self, timeout: float = 10.0) -> bool:
        """
        Wait for cluster to converge on leader and consistent state.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if cluster converged within timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check for exactly one leader
            leaders = []
            active_nodes = []
            
            for node_id, node in self.nodes.items():
                if node_id not in self.failed_nodes and node.running:
                    active_nodes.append(node_id)
                    if node.is_leader():
                        leaders.append(node_id)
            
            if len(leaders) == 1 and len(active_nodes) >= len(self.nodes) // 2 + 1:
                # Check if all active nodes have committed the same entries
                leader_node = self.nodes[leaders[0]]
                leader_commit_index = leader_node.commit_index
                
                converged = True
                for node_id in active_nodes:
                    node = self.nodes[node_id]
                    # Allow some lag in commit index (followers may be slightly behind)
                    if abs(node.commit_index - leader_commit_index) > 5:
                        converged = False
                        break
                
                if converged:
                    self.logger.info(f"Cluster converged with leader: {leaders[0]}")
                    return True
            
            time.sleep(0.1)
        
        self.logger.warning("Cluster failed to converge within timeout")
        return False
    
    def submit_request(self, command: str, data: Dict[str, Any]) -> bool:
        """
        Submit client request to cluster.
        
        Automatically routes request to current leader. If no leader is available
        or request fails, returns False.
        
        Args:
            command: Command to execute (e.g., "SET", "DELETE")
            data: Command data
            
        Returns:
            bool: True if request was successfully processed
        """
        with self.lock:
            self.request_count += 1
            request_id = self.request_count
        
        leader_id = self.get_leader()
        
        if not leader_id:
            self.logger.warning(f"Request {request_id} failed: No leader available")
            return False
        
        leader = self.nodes[leader_id]
        
        try:
            success = leader.client_request(command, data)
            if success:
                self.logger.debug(f"Request {request_id} processed by leader {leader_id}")
            else:
                self.logger.warning(f"Request {request_id} rejected by {leader_id}")
            return success
            
        except Exception as e:
            self.logger.error(f"Request {request_id} failed: {e}")
            return False
    
    def simulate_node_failure(self, node_id: str) -> bool:
        """
        Simulate failure of a specific node.
        
        Args:
            node_id: ID of node to fail
            
        Returns:
            bool: True if node was successfully failed
        """
        if node_id not in self.nodes:
            self.logger.warning(f"Cannot fail unknown node: {node_id}")
            return False
        
        if node_id in self.failed_nodes:
            self.logger.warning(f"Node {node_id} already failed")
            return False
        
        # Stop the node
        self.nodes[node_id].stop()
        
        with self.lock:
            self.failed_nodes.add(node_id)
        
        # Remove RPC handlers pointing to this node
        for other_node_id, other_node in self.nodes.items():
            if other_node_id != node_id and node_id in other_node.rpc_handlers:
                del other_node.rpc_handlers[node_id]
        
        self.logger.info(f"Simulated failure of node {node_id}")
        return True
    
    def recover_node(self, node_id: str) -> bool:
        """
        Recover a failed node.
        
        Args:
            node_id: ID of node to recover
            
        Returns:
            bool: True if node was successfully recovered
        """
        if node_id not in self.nodes:
            self.logger.warning(f"Cannot recover unknown node: {node_id}")
            return False
        
        if node_id not in self.failed_nodes:
            self.logger.warning(f"Node {node_id} is not failed")
            return False
        
        # Create new node to replace failed one
        old_node = self.nodes[node_id]
        node_ids = [node.node_id for node in self.nodes.values()]
        
        new_node = RaftNode(
            node_id=node_id,
            cluster_nodes=node_ids,
            election_timeout_range=old_node.election_timeout_range,
            heartbeat_interval=old_node.heartbeat_interval
        )
        
        self.nodes[node_id] = new_node
        
        # Re-establish RPC handlers
        for other_node_id, other_node in self.nodes.items():
            if other_node_id != node_id:
                new_node.add_rpc_handler(other_node_id, other_node)
                other_node.add_rpc_handler(node_id, new_node)
        
        with self.lock:
            self.failed_nodes.discard(node_id)
        
        self.logger.info(f"Recovered node {node_id}")
        return True
    
    def simulate_network_partition(self, partition_a: List[str], partition_b: List[str]) -> bool:
        """
        Simulate network partition between two groups of nodes.
        
        Args:
            partition_a: List of node IDs in first partition
            partition_b: List of node IDs in second partition
            
        Returns:
            bool: True if partition was successfully created
        """
        # Validate partitions
        all_partitioned = set(partition_a + partition_b)
        all_nodes = set(self.nodes.keys())
        
        if not all_partitioned.issubset(all_nodes):
            self.logger.error("Invalid partition: unknown nodes specified")
            return False
        
        if len(set(partition_a) & set(partition_b)) > 0:
            self.logger.error("Invalid partition: nodes cannot be in both partitions")
            return False
        
        # Remove RPC handlers between partitions
        for node_a in partition_a:
            if node_a in self.nodes:
                for node_b in partition_b:
                    if node_b in self.nodes[node_a].rpc_handlers:
                        del self.nodes[node_a].rpc_handlers[node_b]
        
        for node_b in partition_b:
            if node_b in self.nodes:
                for node_a in partition_a:
                    if node_a in self.nodes[node_b].rpc_handlers:
                        del self.nodes[node_b].rpc_handlers[node_a]
        
        self.logger.info(f"Created network partition: {partition_a} | {partition_b}")
        return True
    
    def heal_network_partition(self) -> None:
        """Heal all network partitions by restoring RPC handlers"""
        # Restore all RPC handlers
        for node_id, node in self.nodes.items():
            if node_id not in self.failed_nodes:
                for other_node_id, other_node in self.nodes.items():
                    if (other_node_id != node_id and 
                        other_node_id not in self.failed_nodes and
                        other_node_id not in node.rpc_handlers):
                        node.add_rpc_handler(other_node_id, other_node)
        
        self.logger.info("Healed all network partitions")
    
    def get_cluster_stats(self) -> ClusterStats:
        """
        Get comprehensive cluster statistics.
        
        Returns:
            ClusterStats: Current cluster state and statistics
        """
        leader_id = self.get_leader()
        active_nodes = 0
        current_term = 0
        node_stats = {}
        
        for node_id, node in self.nodes.items():
            if node_id not in self.failed_nodes and node.running:
                active_nodes += 1
                stats = node.get_stats()
                node_stats[node_id] = stats
                current_term = max(current_term, stats.term)
        
        return ClusterStats(
            total_nodes=len(self.nodes),
            active_nodes=active_nodes,
            leader_id=leader_id,
            current_term=current_term,
            nodes=node_stats
        )
    
    def get_state_consistency(self) -> Dict[str, Any]:
        """
        Check state machine consistency across all nodes.
        
        Returns:
            dict: Consistency analysis including mismatched keys
        """
        state_machines = {}
        all_keys = set()
        
        # Collect state machines from all active nodes
        for node_id, node in self.nodes.items():
            if node_id not in self.failed_nodes and node.running:
                state_machine = node.get_state_machine()
                state_machines[node_id] = state_machine
                all_keys.update(state_machine.keys())
        
        # Check consistency
        mismatched_keys = {}
        consistent_keys = 0
        
        for key in all_keys:
            values = {}
            for node_id, state_machine in state_machines.items():
                values[node_id] = state_machine.get(key, "<MISSING>")
            
            unique_values = set(values.values())
            if len(unique_values) == 1:
                consistent_keys += 1
            else:
                mismatched_keys[key] = values
        
        return {
            "total_keys": len(all_keys),
            "consistent_keys": consistent_keys,
            "mismatched_keys": mismatched_keys,
            "consistency_percentage": (consistent_keys / len(all_keys) * 100) if all_keys else 100,
            "nodes_checked": list(state_machines.keys())
        }
    
    def stop(self) -> None:
        """Stop all nodes in the cluster"""
        for node in self.nodes.values():
            node.stop()
        self.logger.info("Cluster stopped")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure cleanup"""
        self.stop()
    
    def __str__(self) -> str:
        active = len([n for n in self.nodes.values() if n.running])
        leader = self.get_leader()
        return f"RaftCluster({active}/{len(self.nodes)} nodes, leader={leader})"
