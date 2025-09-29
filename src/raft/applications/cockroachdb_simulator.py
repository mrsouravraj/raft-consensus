"""
CockroachDB Simulator - Distributed SQL Database

Educational simulation of CockroachDB's use of Raft for distributed SQL transactions.

CockroachDB uses Multi-Raft, where each range (contiguous block of keys) has its own
Raft group for replication. This enables:
- Horizontal scalability across thousands of ranges
- Strong consistency with ACID transactions
- Automatic data rebalancing
- Geographic distribution with locality awareness

This simulator demonstrates how CockroachDB achieves distributed ACID transactions
while maintaining consistency during node failures and network partitions.
"""

import json
import time
import hashlib
import random
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from ..cluster import RaftCluster


@dataclass
class TransactionRecord:
    """Represents a transaction record"""
    txn_id: str
    timestamp: float
    operations: List[Dict[str, Any]]
    status: str  # PENDING, COMMITTED, ABORTED
    commit_timestamp: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "txn_id": self.txn_id,
            "timestamp": self.timestamp,
            "operations": self.operations,
            "status": self.status,
            "commit_timestamp": self.commit_timestamp
        }


@dataclass 
class RangeDescriptor:
    """Describes a data range in CockroachDB"""
    range_id: str
    start_key: str
    end_key: str
    replicas: List[str]
    leader_replica: Optional[str]
    created_at: float
    
    def contains_key(self, key: str) -> bool:
        """Check if this range contains the given key"""
        return self.start_key <= key < self.end_key


class CockroachDBSimulator:
    """
    Educational simulation of CockroachDB distributed SQL database.
    
    Demonstrates Multi-Raft architecture where each range (key span) has its own
    Raft group for replication. Features:
    - Range-based data partitioning
    - Distributed ACID transactions
    - Multi-version concurrency control (simplified)
    - Node failure handling with automatic rebalancing
    - Strong consistency guarantees
    """
    
    def __init__(self, num_nodes: int = 6, ranges_per_node: int = 2):
        """
        Initialize CockroachDB cluster.
        
        Args:
            num_nodes: Number of CockroachDB nodes
            ranges_per_node: Average number of ranges per node
        """
        if num_nodes < 3:
            raise ValueError("CockroachDB cluster needs at least 3 nodes")
        
        self.num_nodes = num_nodes
        self.node_ids = [f"node_{i}" for i in range(num_nodes)]
        self.ranges: Dict[str, RaftCluster] = {}  # range_id -> Raft cluster
        self.range_descriptors: Dict[str, RangeDescriptor] = {}
        self.txn_counter = 1000
        
        self.logger = logging.getLogger("cockroachdb.simulator")
        
        # Create initial ranges with key space partitioning
        num_ranges = max(3, ranges_per_node)  # At least 3 ranges for demo
        self._create_initial_ranges(num_ranges)
        
        # Initialize system tables
        self._initialize_system_tables()
        
        self.logger.info(f"CockroachDB cluster initialized with {num_nodes} nodes and {num_ranges} ranges")
    
    def _create_initial_ranges(self, num_ranges: int) -> None:
        """Create initial range partitioning"""
        # Create key space partitions (simplified alphabetical)
        key_space = "abcdefghijklmnopqrstuvwxyz"
        partition_size = len(key_space) // num_ranges
        
        for i in range(num_ranges):
            start_idx = i * partition_size
            end_idx = start_idx + partition_size if i < num_ranges - 1 else len(key_space)
            
            start_key = key_space[start_idx] if start_idx < len(key_space) else "z"
            end_key = key_space[end_idx] if end_idx < len(key_space) else "~"
            
            range_id = f"range_{i}"
            
            self._create_range(range_id, start_key, end_key)
    
    def _create_range(self, range_id: str, start_key: str, end_key: str) -> bool:
        """
        Create a new range with Raft replication.
        
        Args:
            range_id: Unique range identifier
            start_key: Starting key for this range
            end_key: Ending key for this range (exclusive)
            
        Returns:
            bool: True if range was successfully created
        """
        # Determine replica placement (typically 3 replicas)
        replica_count = min(3, self.num_nodes)
        replica_nodes = random.sample(self.node_ids, replica_count)
        
        # Create Raft cluster for this range
        range_cluster = RaftCluster(num_nodes=replica_count)
        
        if not range_cluster.wait_for_convergence(timeout=3.0):
            self.logger.error(f"Failed to initialize range {range_id}")
            return False
        
        self.ranges[range_id] = range_cluster
        
        # Create range descriptor
        leader = range_cluster.get_leader()
        descriptor = RangeDescriptor(
            range_id=range_id,
            start_key=start_key,
            end_key=end_key,
            replicas=replica_nodes,
            leader_replica=leader,
            created_at=time.time()
        )
        
        self.range_descriptors[range_id] = descriptor
        
        # Store range metadata in the range itself
        range_metadata = {
            "range_id": range_id,
            "start_key": start_key,
            "end_key": end_key,
            "replicas": replica_nodes,
            "created_at": descriptor.created_at,
            "version": 1
        }
        
        range_cluster.submit_request("SET", {
            "key": f"_meta/range/{range_id}",
            "value": json.dumps(range_metadata)
        })
        
        self.logger.info(f"Created range {range_id}: [{start_key}, {end_key}) with replicas {replica_nodes}")
        return True
    
    def _initialize_system_tables(self) -> None:
        """Initialize system tables in ranges"""
        # Store cluster metadata
        cluster_info = {
            "cluster_id": "crdb-educational",
            "version": "v23.2.0-educational", 
            "nodes": self.node_ids,
            "created_at": time.time()
        }
        
        # Find range containing system keys (typically starts with underscore)
        system_range = self._find_range_for_key("_system")
        if system_range:
            self.ranges[system_range].submit_request("SET", {
                "key": "_system/cluster_info",
                "value": json.dumps(cluster_info)
            })
    
    def _find_range_for_key(self, key: str) -> Optional[str]:
        """Find which range contains the given key"""
        for range_id, descriptor in self.range_descriptors.items():
            if descriptor.contains_key(key):
                return range_id
        return None
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute distributed transaction across ranges.
        
        Args:
            operations: List of operations, each with 'type', 'key', and optional 'value'
            
        Returns:
            dict: Transaction result with status and details
        """
        txn_id = f"txn_{self.txn_counter}"
        self.txn_counter += 1
        
        self.logger.info(f"Starting transaction {txn_id} with {len(operations)} operations")
        
        # Group operations by range
        range_operations = {}
        for op in operations:
            key = op.get("key")
            if not key:
                continue
            
            range_id = self._find_range_for_key(key)
            if not range_id:
                return {
                    "txn_id": txn_id,
                    "status": "ABORTED",
                    "error": f"No range found for key: {key}"
                }
            
            if range_id not in range_operations:
                range_operations[range_id] = []
            range_operations[range_id].append(op)
        
        # Create transaction record
        txn_record = TransactionRecord(
            txn_id=txn_id,
            timestamp=time.time(),
            operations=operations,
            status="PENDING"
        )
        
        # Execute operations in each range (simplified - no 2PC for education)
        results = {}
        failed_ranges = []
        
        for range_id, ops in range_operations.items():
            success = self._execute_range_operations(range_id, txn_id, ops)
            results[range_id] = success
            if not success:
                failed_ranges.append(range_id)
        
        # Determine transaction outcome
        if failed_ranges:
            # Abort transaction - rollback successful ranges
            for range_id, success in results.items():
                if success:
                    self._rollback_range_transaction(range_id, txn_id)
            
            txn_record.status = "ABORTED"
            result = {
                "txn_id": txn_id,
                "status": "ABORTED",
                "error": f"Failed in ranges: {failed_ranges}",
                "affected_ranges": list(range_operations.keys())
            }
        else:
            # Commit transaction
            txn_record.status = "COMMITTED" 
            txn_record.commit_timestamp = time.time()
            
            # Mark ranges as committed
            for range_id in range_operations.keys():
                self._commit_range_transaction(range_id, txn_id)
            
            result = {
                "txn_id": txn_id,
                "status": "COMMITTED",
                "commit_timestamp": txn_record.commit_timestamp,
                "affected_ranges": list(range_operations.keys()),
                "operations_count": len(operations)
            }
        
        # Store transaction record in system range
        system_range = self._find_range_for_key("_system")
        if system_range:
            self.ranges[system_range].submit_request("SET", {
                "key": f"_system/transactions/{txn_id}",
                "value": json.dumps(txn_record.to_dict())
            })
        
        self.logger.info(f"Transaction {txn_id}: {result['status']}")
        return result
    
    def _execute_range_operations(self, range_id: str, txn_id: str, 
                                 operations: List[Dict[str, Any]]) -> bool:
        """Execute operations within a single range"""
        if range_id not in self.ranges:
            return False
        
        range_cluster = self.ranges[range_id]
        
        # Create transaction intent record
        intent_key = f"_intents/{txn_id}"
        intent_record = {
            "txn_id": txn_id,
            "timestamp": time.time(),
            "operations": operations,
            "status": "PENDING"
        }
        
        success = range_cluster.submit_request("SET", {
            "key": intent_key,
            "value": json.dumps(intent_record)
        })
        
        if not success:
            return False
        
        # Execute each operation
        for op in operations:
            op_type = op.get("type")
            key = op.get("key")
            
            if op_type == "READ":
                # For reads, just verify key exists (simplified)
                leader_id = range_cluster.get_leader()
                if leader_id:
                    leader = range_cluster.nodes[leader_id]
                    state_machine = leader.get_state_machine()
                    if key not in state_machine:
                        self.logger.debug(f"Read failed: key {key} not found in {range_id}")
                        return False
                
            elif op_type == "write":
                value = op.get("value", "")
                write_record = {
                    "value": value,
                    "txn_id": txn_id,
                    "timestamp": time.time(),
                    "committed": False
                }
                
                success = range_cluster.submit_request("SET", {
                    "key": key,
                    "value": json.dumps(write_record)
                })
                
                if not success:
                    return False
                
            elif op_type == "delete":
                success = range_cluster.submit_request("DELETE", {"key": key})
                if not success:
                    return False
        
        return True
    
    def _commit_range_transaction(self, range_id: str, txn_id: str) -> None:
        """Mark transaction as committed in range"""
        if range_id not in self.ranges:
            return
        
        range_cluster = self.ranges[range_id]
        
        # Update intent status
        intent_key = f"_intents/{txn_id}"
        leader_id = range_cluster.get_leader()
        if leader_id:
            leader = range_cluster.nodes[leader_id]
            state_machine = leader.get_state_machine()
            
            if intent_key in state_machine:
                try:
                    intent_record = json.loads(state_machine[intent_key])
                    intent_record["status"] = "COMMITTED"
                    intent_record["commit_timestamp"] = time.time()
                    
                    range_cluster.submit_request("SET", {
                        "key": intent_key,
                        "value": json.dumps(intent_record)
                    })
                except json.JSONDecodeError:
                    pass
    
    def _rollback_range_transaction(self, range_id: str, txn_id: str) -> None:
        """Rollback transaction in range"""
        if range_id not in self.ranges:
            return
        
        range_cluster = self.ranges[range_id]
        
        # Remove intent record
        intent_key = f"_intents/{txn_id}"
        range_cluster.submit_request("DELETE", {"key": intent_key})
        
        # Remove any uncommitted writes (simplified)
        leader_id = range_cluster.get_leader()
        if leader_id:
            leader = range_cluster.nodes[leader_id]
            state_machine = leader.get_state_machine()
            
            keys_to_remove = []
            for key, value in state_machine.items():
                if not key.startswith("_"):  # Skip system keys
                    try:
                        record = json.loads(value)
                        if (isinstance(record, dict) and 
                            record.get("txn_id") == txn_id and 
                            not record.get("committed", True)):
                            keys_to_remove.append(key)
                    except (json.JSONDecodeError, TypeError):
                        continue
            
            for key in keys_to_remove:
                range_cluster.submit_request("DELETE", {"key": key})
    
    def read_data(self, key: str, consistency: str = "strong") -> Optional[Any]:
        """
        Read data with specified consistency level.
        
        Args:
            key: Key to read
            consistency: "strong" (from leader) or "eventual" (from any replica)
            
        Returns:
            Data value or None if not found
        """
        range_id = self._find_range_for_key(key)
        if not range_id or range_id not in self.ranges:
            return None
        
        range_cluster = self.ranges[range_id]
        
        if consistency == "strong":
            # Strong consistency: read from Raft leader
            leader_id = range_cluster.get_leader()
            if leader_id:
                leader = range_cluster.nodes[leader_id]
                state_machine = leader.get_state_machine()
                
                if key in state_machine:
                    try:
                        record = json.loads(state_machine[key])
                        if isinstance(record, dict) and "value" in record:
                            return record["value"]
                        return record
                    except json.JSONDecodeError:
                        return state_machine[key]
        
        elif consistency == "eventual":
            # Eventual consistency: read from any available replica
            for node in range_cluster.nodes.values():
                if node.running:
                    state_machine = node.get_state_machine()
                    if key in state_machine:
                        try:
                            record = json.loads(state_machine[key])
                            if isinstance(record, dict) and "value" in record:
                                return record["value"]
                            return record
                        except json.JSONDecodeError:
                            return state_machine[key]
        
        return None
    
    def simulate_node_failure(self, node_id: str) -> Dict[str, Any]:
        """
        Simulate failure of a CockroachDB node.
        
        Args:
            node_id: ID of node to fail
            
        Returns:
            dict: Failure impact summary
        """
        if node_id not in self.node_ids:
            return {"error": f"Unknown node: {node_id}"}
        
        self.logger.info(f"Simulating failure of node: {node_id}")
        
        affected_ranges = []
        leader_changes = []
        
        # Identify affected ranges and simulate failures
        for range_id, range_cluster in self.ranges.items():
            # Check if this node is in the range's replica set
            descriptor = self.range_descriptors[range_id]
            if node_id in descriptor.replicas:
                affected_ranges.append(range_id)
                
                # If this node was the leader, record the change
                if descriptor.leader_replica == node_id:
                    old_leader = descriptor.leader_replica
                    
                    # Simulate some node failures in the range cluster
                    # In reality, this would be more complex with replica rebalancing
                    time.sleep(1.0)  # Allow time for leader election
                    
                    new_leader = range_cluster.get_leader()
                    descriptor.leader_replica = new_leader
                    
                    leader_changes.append({
                        "range_id": range_id,
                        "old_leader": old_leader,
                        "new_leader": new_leader
                    })
        
        result = {
            "failed_node": node_id,
            "affected_ranges": affected_ranges,
            "leader_changes": leader_changes,
            "cluster_operational": len(affected_ranges) < len(self.ranges),  # Simplified check
            "impact_summary": f"Node failure affected {len(affected_ranges)} ranges"
        }
        
        if leader_changes:
            self.logger.info(f"Leader elections completed: {len(leader_changes)} ranges elected new leaders")
        
        self.logger.info(f"Node failure impact: {result['impact_summary']}")
        return result
    
    def get_cluster_stats(self) -> Dict[str, Any]:
        """Get comprehensive cluster statistics"""
        total_ranges = len(self.ranges)
        active_ranges = 0
        total_keys = 0
        leader_distribution = {}
        
        range_details = {}
        
        for range_id, range_cluster in self.ranges.items():
            leader_id = range_cluster.get_leader()
            if leader_id:
                active_ranges += 1
                
                # Count leader distribution
                leader_distribution[leader_id] = leader_distribution.get(leader_id, 0) + 1
                
                # Count keys in range (excluding system keys)
                leader_node = range_cluster.nodes[leader_id]
                state_machine = leader_node.get_state_machine()
                range_keys = len([k for k in state_machine.keys() 
                                if not k.startswith("_")])
                total_keys += range_keys
                
                descriptor = self.range_descriptors[range_id]
                range_details[range_id] = {
                    "start_key": descriptor.start_key,
                    "end_key": descriptor.end_key,
                    "leader": leader_id,
                    "replicas": descriptor.replicas,
                    "keys": range_keys,
                    "healthy": True
                }
        
        return {
            "cluster_overview": {
                "total_nodes": len(self.node_ids),
                "total_ranges": total_ranges,
                "active_ranges": active_ranges,
                "total_keys": total_keys
            },
            "leader_distribution": leader_distribution,
            "range_details": range_details,
            "health": {
                "operational_percentage": (active_ranges / total_ranges * 100) if total_ranges > 0 else 0,
                "all_ranges_healthy": active_ranges == total_ranges
            }
        }
    
    def get_transaction_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent transaction history"""
        # Get transactions from system range
        system_range = self._find_range_for_key("_system")
        if not system_range:
            return []
        
        system_cluster = self.ranges[system_range]
        leader_id = system_cluster.get_leader()
        if not leader_id:
            return []
        
        leader = system_cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        transactions = []
        txn_prefix = "_system/transactions/"
        
        for key, value in state_machine.items():
            if key.startswith(txn_prefix):
                try:
                    txn_record = json.loads(value)
                    transactions.append(txn_record)
                except json.JSONDecodeError:
                    continue
        
        # Sort by timestamp and limit
        transactions.sort(key=lambda t: t.get("timestamp", 0), reverse=True)
        return transactions[:limit]
    
    def stop(self) -> None:
        """Stop all ranges in the cluster"""
        for range_cluster in self.ranges.values():
            range_cluster.stop()
        
        self.logger.info("CockroachDB cluster stopped")
