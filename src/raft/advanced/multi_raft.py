"""
Multi-Raft System for Horizontal Scaling

Implementation of Multi-Raft architecture where multiple Raft groups
operate independently to achieve horizontal scalability.

This approach is used by systems like:
- CockroachDB: Each range has its own Raft group
- TiKV: Key ranges are replicated independently  
- etcd: Can shard data across multiple etcd clusters

Multi-Raft enables:
- Horizontal scaling beyond single Raft group limits
- Parallel processing of requests across shards
- Independent failure domains per shard
- Load distribution across the system
"""

import time
import hashlib
import logging
import threading
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass
from ..cluster import RaftCluster


@dataclass
class ShardDescriptor:
    """Describes a shard in the Multi-Raft system"""
    shard_id: str
    start_key: Optional[str]    # Inclusive start key (None for hash-based)
    end_key: Optional[str]      # Exclusive end key (None for hash-based)
    hash_range: Optional[Tuple[int, int]]  # (start_hash, end_hash) for hash-based
    replica_count: int
    created_at: float
    
    def contains_key(self, key: str) -> bool:
        """Check if this shard contains the given key"""
        if self.start_key is not None and self.end_key is not None:
            # Range-based sharding
            return self.start_key <= key < self.end_key
        elif self.hash_range is not None:
            # Hash-based sharding
            key_hash = hash(key) % (2**32)  # 32-bit hash space
            start_hash, end_hash = self.hash_range
            if start_hash <= end_hash:
                return start_hash <= key_hash < end_hash
            else:
                # Wrap-around case
                return key_hash >= start_hash or key_hash < end_hash
        return False


class ShardManager:
    """
    Manages shard assignment and routing for Multi-Raft system.
    
    Handles:
    - Shard creation and configuration
    - Key-to-shard routing
    - Shard rebalancing (educational)
    - Metadata management
    """
    
    def __init__(self, sharding_strategy: str = "hash"):
        """
        Initialize shard manager.
        
        Args:
            sharding_strategy: "hash" or "range" based sharding
        """
        self.sharding_strategy = sharding_strategy
        self.shards: Dict[str, ShardDescriptor] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger("multiraft.shardmanager")
    
    def create_hash_based_shards(self, num_shards: int, replica_count: int = 3) -> List[str]:
        """
        Create hash-based shards with even distribution.
        
        Args:
            num_shards: Number of shards to create
            replica_count: Replicas per shard
            
        Returns:
            list: List of created shard IDs
        """
        with self.lock:
            hash_space = 2**32
            shard_size = hash_space // num_shards
            
            created_shards = []
            
            for i in range(num_shards):
                shard_id = f"shard_{i:03d}"
                start_hash = i * shard_size
                end_hash = start_hash + shard_size if i < num_shards - 1 else hash_space
                
                descriptor = ShardDescriptor(
                    shard_id=shard_id,
                    start_key=None,
                    end_key=None,
                    hash_range=(start_hash, end_hash),
                    replica_count=replica_count,
                    created_at=time.time()
                )
                
                self.shards[shard_id] = descriptor
                created_shards.append(shard_id)
                
                self.logger.debug(f"Created hash shard {shard_id}: [{start_hash}, {end_hash})")
            
            self.logger.info(f"Created {num_shards} hash-based shards")
            return created_shards
    
    def create_range_based_shards(self, key_ranges: List[Tuple[str, str]], 
                                 replica_count: int = 3) -> List[str]:
        """
        Create range-based shards with specified key ranges.
        
        Args:
            key_ranges: List of (start_key, end_key) tuples
            replica_count: Replicas per shard
            
        Returns:
            list: List of created shard IDs
        """
        with self.lock:
            created_shards = []
            
            for i, (start_key, end_key) in enumerate(key_ranges):
                shard_id = f"range_shard_{i:03d}"
                
                descriptor = ShardDescriptor(
                    shard_id=shard_id,
                    start_key=start_key,
                    end_key=end_key,
                    hash_range=None,
                    replica_count=replica_count,
                    created_at=time.time()
                )
                
                self.shards[shard_id] = descriptor
                created_shards.append(shard_id)
                
                self.logger.debug(f"Created range shard {shard_id}: [{start_key}, {end_key})")
            
            self.logger.info(f"Created {len(key_ranges)} range-based shards")
            return created_shards
    
    def find_shard_for_key(self, key: str) -> Optional[str]:
        """
        Find which shard contains the given key.
        
        Args:
            key: Key to look up
            
        Returns:
            str: Shard ID or None if no shard found
        """
        with self.lock:
            for shard_id, descriptor in self.shards.items():
                if descriptor.contains_key(key):
                    return shard_id
            return None
    
    def get_shard_descriptor(self, shard_id: str) -> Optional[ShardDescriptor]:
        """Get descriptor for a specific shard"""
        return self.shards.get(shard_id)
    
    def list_shards(self) -> List[str]:
        """Get list of all shard IDs"""
        return list(self.shards.keys())
    
    def get_shard_stats(self) -> Dict[str, Any]:
        """Get statistics about sharding"""
        with self.lock:
            return {
                "total_shards": len(self.shards),
                "sharding_strategy": self.sharding_strategy,
                "shards": {
                    shard_id: {
                        "replica_count": desc.replica_count,
                        "created_at": desc.created_at,
                        "age_seconds": time.time() - desc.created_at
                    }
                    for shard_id, desc in self.shards.items()
                }
            }


class MultiRaftSystem:
    """
    Multi-Raft system implementing horizontal scalability through sharding.
    
    Each shard is an independent Raft group, enabling:
    - Parallel request processing
    - Independent scaling per shard
    - Fault isolation between shards
    - Linear scalability with number of shards
    
    Educational implementation demonstrating concepts used in
    distributed databases like CockroachDB and TiKV.
    """
    
    def __init__(self, num_shards: int = 3, nodes_per_shard: int = 3,
                 sharding_strategy: str = "hash"):
        """
        Initialize Multi-Raft system.
        
        Args:
            num_shards: Number of independent Raft groups
            nodes_per_shard: Number of nodes per Raft group
            sharding_strategy: "hash" or "range" based sharding
        """
        self.num_shards = num_shards
        self.nodes_per_shard = nodes_per_shard
        self.shard_manager = ShardManager(sharding_strategy)
        self.raft_groups: Dict[str, RaftCluster] = {}
        
        self.request_counter = 0
        self.lock = threading.Lock()
        
        self.logger = logging.getLogger("multiraft.system")
        
        # Initialize shards
        self._initialize_shards()
        
        self.logger.info(f"Multi-Raft system initialized: {num_shards} shards, "
                        f"{nodes_per_shard} nodes each ({sharding_strategy} sharding)")
    
    def _initialize_shards(self) -> None:
        """Initialize all shards and their Raft groups"""
        # Create shard descriptors
        if self.shard_manager.sharding_strategy == "hash":
            shard_ids = self.shard_manager.create_hash_based_shards(
                self.num_shards, self.nodes_per_shard
            )
        else:
            # Create alphabetical ranges for educational demo
            alphabet = "abcdefghijklmnopqrstuvwxyz"
            range_size = len(alphabet) // self.num_shards
            key_ranges = []
            
            for i in range(self.num_shards):
                start_idx = i * range_size
                end_idx = start_idx + range_size if i < self.num_shards - 1 else len(alphabet)
                start_key = alphabet[start_idx] if start_idx < len(alphabet) else "z"
                end_key = alphabet[end_idx] if end_idx < len(alphabet) else "~"
                key_ranges.append((start_key, end_key))
            
            shard_ids = self.shard_manager.create_range_based_shards(
                key_ranges, self.nodes_per_shard
            )
        
        # Create Raft cluster for each shard
        for shard_id in shard_ids:
            cluster = RaftCluster(num_nodes=self.nodes_per_shard)
            if cluster.wait_for_convergence(timeout=3.0):
                self.raft_groups[shard_id] = cluster
                self.logger.debug(f"Initialized Raft group for shard {shard_id}")
            else:
                self.logger.error(f"Failed to initialize Raft group for shard {shard_id}")
    
    def put(self, key: str, value: str) -> bool:
        """
        Store key-value pair in appropriate shard.
        
        Args:
            key: Key to store
            value: Value to store
            
        Returns:
            bool: True if successfully stored
        """
        shard_id = self.shard_manager.find_shard_for_key(key)
        
        if not shard_id or shard_id not in self.raft_groups:
            self.logger.error(f"No shard found for key: {key}")
            return False
        
        with self.lock:
            self.request_counter += 1
            request_id = self.request_counter
        
        raft_group = self.raft_groups[shard_id]
        
        success = raft_group.submit_request("SET", {
            "key": key,
            "value": value,
            "shard_id": shard_id,
            "request_id": request_id
        })
        
        if success:
            self.logger.debug(f"PUT {key}={value} -> shard {shard_id}")
        else:
            self.logger.warning(f"Failed PUT {key}={value} -> shard {shard_id}")
        
        return success
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value for key from appropriate shard.
        
        Args:
            key: Key to retrieve
            
        Returns:
            str: Value if found, None otherwise
        """
        shard_id = self.shard_manager.find_shard_for_key(key)
        
        if not shard_id or shard_id not in self.raft_groups:
            return None
        
        raft_group = self.raft_groups[shard_id]
        leader_id = raft_group.get_leader()
        
        if not leader_id:
            return None
        
        leader = raft_group.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        self.logger.debug(f"GET {key} -> shard {shard_id}")
        return state_machine.get(key)
    
    def delete(self, key: str) -> bool:
        """
        Delete key from appropriate shard.
        
        Args:
            key: Key to delete
            
        Returns:
            bool: True if successfully deleted
        """
        shard_id = self.shard_manager.find_shard_for_key(key)
        
        if not shard_id or shard_id not in self.raft_groups:
            return False
        
        raft_group = self.raft_groups[shard_id]
        
        success = raft_group.submit_request("DELETE", {"key": key})
        
        if success:
            self.logger.debug(f"DELETE {key} -> shard {shard_id}")
        
        return success
    
    def batch_put(self, key_value_pairs: List[Tuple[str, str]]) -> Dict[str, bool]:
        """
        Store multiple key-value pairs efficiently.
        
        Groups requests by shard to minimize cross-shard operations.
        
        Args:
            key_value_pairs: List of (key, value) tuples
            
        Returns:
            dict: Key -> success mapping
        """
        # Group by shard
        shard_requests = {}
        
        for key, value in key_value_pairs:
            shard_id = self.shard_manager.find_shard_for_key(key)
            if shard_id and shard_id in self.raft_groups:
                if shard_id not in shard_requests:
                    shard_requests[shard_id] = []
                shard_requests[shard_id].append((key, value))
        
        # Execute requests per shard
        results = {}
        
        for shard_id, requests in shard_requests.items():
            raft_group = self.raft_groups[shard_id]
            
            for key, value in requests:
                success = raft_group.submit_request("SET", {
                    "key": key,
                    "value": value,
                    "shard_id": shard_id
                })
                results[key] = success
        
        self.logger.info(f"Batch PUT: {len(key_value_pairs)} operations across "
                        f"{len(shard_requests)} shards")
        
        return results
    
    def simulate_shard_failure(self, shard_id: str) -> Dict[str, Any]:
        """
        Simulate failure of an entire shard.
        
        Args:
            shard_id: Shard to fail
            
        Returns:
            dict: Failure impact summary
        """
        if shard_id not in self.raft_groups:
            return {"error": f"Unknown shard: {shard_id}"}
        
        self.logger.info(f"Simulating failure of shard {shard_id}")
        
        # Stop the shard's Raft group
        self.raft_groups[shard_id].stop()
        del self.raft_groups[shard_id]
        
        # Calculate impact
        remaining_shards = len(self.raft_groups)
        impact_percentage = (1 / self.num_shards) * 100
        
        result = {
            "failed_shard": shard_id,
            "remaining_shards": remaining_shards,
            "total_shards": self.num_shards,
            "availability_impact": f"{impact_percentage:.1f}%",
            "system_operational": remaining_shards > 0
        }
        
        self.logger.info(f"Shard failure impact: {impact_percentage:.1f}% of capacity lost")
        return result
    
    def simulate_node_failure_in_shard(self, shard_id: str, node_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Simulate failure of a specific node within a shard.
        
        Args:
            shard_id: Shard containing the node
            node_id: Specific node to fail (leader if None)
            
        Returns:
            dict: Failure impact summary
        """
        if shard_id not in self.raft_groups:
            return {"error": f"Unknown shard: {shard_id}"}
        
        raft_group = self.raft_groups[shard_id]
        target_node = node_id or raft_group.get_leader()
        
        if not target_node:
            return {"error": f"No target node in shard {shard_id}"}
        
        self.logger.info(f"Simulating failure of node {target_node} in shard {shard_id}")
        
        # Simulate node failure
        original_leader = raft_group.get_leader()
        success = raft_group.simulate_node_failure(target_node)
        
        # Wait for recovery
        time.sleep(1.0)
        new_leader = raft_group.get_leader()
        
        result = {
            "failed_node": target_node,
            "shard_id": shard_id,
            "was_leader": target_node == original_leader,
            "new_leader": new_leader,
            "shard_operational": new_leader is not None,
            "recovery_successful": success and new_leader is not None
        }
        
        if new_leader:
            self.logger.info(f"Shard {shard_id} recovered with new leader: {new_leader}")
        else:
            self.logger.error(f"Shard {shard_id} failed to recover")
        
        return result
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        shard_stats = {}
        total_keys = 0
        active_shards = 0
        total_leaders = 0
        
        for shard_id, raft_group in self.raft_groups.items():
            leader_id = raft_group.get_leader()
            cluster_stats = raft_group.get_cluster_stats()
            
            if leader_id:
                active_shards += 1
                total_leaders += 1
                
                leader_node = raft_group.nodes[leader_id]
                shard_keys = len(leader_node.get_state_machine())
                total_keys += shard_keys
                
                shard_stats[shard_id] = {
                    "leader": leader_id,
                    "active_nodes": cluster_stats.active_nodes,
                    "total_nodes": cluster_stats.total_nodes,
                    "term": cluster_stats.current_term,
                    "keys": shard_keys,
                    "healthy": True
                }
            else:
                shard_stats[shard_id] = {
                    "leader": None,
                    "healthy": False,
                    "keys": 0
                }
        
        system_health = (active_shards / len(self.shard_manager.shards)) * 100
        
        return {
            "system_overview": {
                "total_shards": len(self.shard_manager.shards),
                "active_shards": active_shards,
                "nodes_per_shard": self.nodes_per_shard,
                "total_keys": total_keys,
                "sharding_strategy": self.shard_manager.sharding_strategy
            },
            "health": {
                "system_health_percentage": system_health,
                "fully_operational": system_health == 100,
                "degraded": 50 <= system_health < 100,
                "critical": system_health < 50
            },
            "shard_details": shard_stats,
            "performance": {
                "total_requests_processed": self.request_counter,
                "parallel_processing_capacity": active_shards
            }
        }
    
    def rebalance_load(self) -> Dict[str, Any]:
        """
        Simulate load rebalancing across shards.
        
        In production systems, this would involve:
        - Moving data between shards
        - Updating shard boundaries
        - Coordinating with clients
        
        Returns:
            dict: Rebalancing operation summary
        """
        self.logger.info("Starting load rebalancing across shards")
        
        # Calculate current load distribution
        shard_loads = {}
        total_load = 0
        
        for shard_id, raft_group in self.raft_groups.items():
            leader_id = raft_group.get_leader()
            if leader_id:
                leader_node = raft_group.nodes[leader_id]
                load = len(leader_node.get_state_machine())
                shard_loads[shard_id] = load
                total_load += load
        
        if not shard_loads:
            return {"error": "No active shards for rebalancing"}
        
        # Calculate target load per shard
        target_load_per_shard = total_load / len(shard_loads)
        
        # Identify imbalanced shards
        overloaded_shards = []
        underloaded_shards = []
        
        for shard_id, load in shard_loads.items():
            if load > target_load_per_shard * 1.2:  # 20% threshold
                overloaded_shards.append((shard_id, load))
            elif load < target_load_per_shard * 0.8:
                underloaded_shards.append((shard_id, load))
        
        # Educational implementation - just log the analysis
        result = {
            "total_load": total_load,
            "target_load_per_shard": target_load_per_shard,
            "overloaded_shards": len(overloaded_shards),
            "underloaded_shards": len(underloaded_shards),
            "rebalancing_needed": len(overloaded_shards) > 0 or len(underloaded_shards) > 0,
            "current_distribution": shard_loads
        }
        
        if result["rebalancing_needed"]:
            self.logger.info(f"Load rebalancing recommended: {len(overloaded_shards)} overloaded, "
                           f"{len(underloaded_shards)} underloaded shards")
        else:
            self.logger.info("System load is well balanced")
        
        return result
    
    def stop(self) -> None:
        """Stop all shards in the system"""
        for shard_id, raft_group in self.raft_groups.items():
            raft_group.stop()
            self.logger.debug(f"Stopped shard {shard_id}")
        
        self.logger.info("Multi-Raft system stopped")
