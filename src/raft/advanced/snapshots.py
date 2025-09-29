"""
Raft Snapshots for Log Compaction

Implementation of Raft snapshots to prevent unbounded log growth.
Snapshots capture the state machine at a point in time, allowing
older log entries to be discarded while maintaining consistency.

This is essential for production Raft systems to prevent:
- Unbounded memory usage from growing logs
- Slow recovery times for new nodes joining
- Excessive disk space consumption
"""

import json
import time
import threading
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from ..node import RaftNode
from ..messages import LogEntry


@dataclass
class Snapshot:
    """
    Represents a snapshot of the state machine at a specific point.
    """
    index: int                    # Last log index included in snapshot
    term: int                     # Term of the last included log entry
    timestamp: float              # When snapshot was created
    state_machine_data: Dict[str, Any]  # State machine contents
    metadata: Dict[str, Any]      # Additional metadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary for serialization"""
        return {
            "index": self.index,
            "term": self.term, 
            "timestamp": self.timestamp,
            "state_machine_data": self.state_machine_data,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Snapshot':
        """Create snapshot from dictionary"""
        return cls(
            index=data["index"],
            term=data["term"],
            timestamp=data["timestamp"],
            state_machine_data=data["state_machine_data"],
            metadata=data["metadata"]
        )


class SnapshotManager:
    """
    Manages snapshot creation, storage, and restoration.
    
    In production systems, snapshots would be stored to persistent disk
    and potentially backed up to remote storage for disaster recovery.
    """
    
    def __init__(self, snapshot_dir: str = "/tmp/raft_snapshots"):
        self.snapshot_dir = snapshot_dir
        self.snapshots: Dict[str, Snapshot] = {}  # node_id -> latest snapshot
        self.logger = logging.getLogger("raft.snapshots")
        
        # In real implementation, would create directory and load existing snapshots
        
    def create_snapshot(self, node_id: str, last_index: int, last_term: int,
                       state_machine: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> Snapshot:
        """
        Create a new snapshot.
        
        Args:
            node_id: Node creating the snapshot
            last_index: Last log index included in snapshot
            last_term: Term of last log entry included
            state_machine: Complete state machine data
            metadata: Additional snapshot metadata
            
        Returns:
            Snapshot: The created snapshot
        """
        snapshot = Snapshot(
            index=last_index,
            term=last_term,
            timestamp=time.time(),
            state_machine_data=state_machine.copy(),
            metadata=metadata or {}
        )
        
        self.snapshots[node_id] = snapshot
        
        # In production, would persist to disk
        self._persist_snapshot(node_id, snapshot)
        
        self.logger.info(f"Created snapshot for {node_id} at index {last_index}, term {last_term}")
        return snapshot
    
    def get_latest_snapshot(self, node_id: str) -> Optional[Snapshot]:
        """Get the latest snapshot for a node"""
        return self.snapshots.get(node_id)
    
    def _persist_snapshot(self, node_id: str, snapshot: Snapshot) -> None:
        """Persist snapshot to storage (educational - would use disk in production)"""
        # Educational implementation - in production would write to disk
        self.logger.debug(f"Persisting snapshot for {node_id} (index {snapshot.index})")
    
    def _load_snapshot(self, node_id: str) -> Optional[Snapshot]:
        """Load snapshot from storage (educational)"""
        # Educational implementation - in production would read from disk
        return self.snapshots.get(node_id)


class SnapshotRaftNode(RaftNode):
    """
    Enhanced Raft node with snapshotting capability.
    
    Extends the base RaftNode to support:
    - Automatic snapshot creation when log grows too large
    - Log truncation after successful snapshots
    - State machine restoration from snapshots
    - Snapshot transfer to new/recovering nodes
    """
    
    def __init__(self, node_id: str, cluster_nodes: List[str], 
                 snapshot_interval: int = 100,
                 snapshot_manager: Optional[SnapshotManager] = None,
                 **kwargs):
        """
        Initialize snapshot-enabled Raft node.
        
        Args:
            node_id: Node identifier
            cluster_nodes: List of all cluster node IDs
            snapshot_interval: Create snapshot every N log entries
            snapshot_manager: Manager for snapshot storage
            **kwargs: Additional arguments for base RaftNode
        """
        super().__init__(node_id, cluster_nodes, **kwargs)
        
        self.snapshot_interval = snapshot_interval
        self.snapshot_manager = snapshot_manager or SnapshotManager()
        self.last_snapshot_index = -1
        self.last_snapshot_term = 0
        
        # Restore from latest snapshot if available
        self._restore_from_snapshot()
        
        self.logger.info(f"Snapshot-enabled Raft node {node_id} initialized (interval: {snapshot_interval})")
    
    def _restore_from_snapshot(self) -> None:
        """Restore state machine and indices from latest snapshot"""
        snapshot = self.snapshot_manager.get_latest_snapshot(self.node_id)
        
        if snapshot:
            # Restore state machine
            self.state_machine = snapshot.state_machine_data.copy()
            self.last_snapshot_index = snapshot.index
            self.last_snapshot_term = snapshot.term
            
            # Update indices
            self.last_applied = snapshot.index
            if snapshot.index >= 0:
                self.commit_index = max(self.commit_index, snapshot.index)
            
            self.logger.info(f"Restored from snapshot: index {snapshot.index}, "
                           f"{len(self.state_machine)} state machine entries")
    
    def _apply_committed_entries(self) -> None:
        """
        Apply committed log entries and check for snapshot creation.
        
        Overrides base method to add snapshot creation logic.
        """
        # Apply entries as normal
        super()._apply_committed_entries()
        
        # Check if we should create a snapshot
        if self._should_create_snapshot():
            self._create_snapshot()
    
    def _should_create_snapshot(self) -> bool:
        """Determine if a snapshot should be created"""
        if not self.log:
            return False
        
        # Create snapshot if we've applied enough entries since last snapshot
        entries_since_snapshot = self.last_applied - self.last_snapshot_index
        return entries_since_snapshot >= self.snapshot_interval
    
    def _create_snapshot(self) -> Optional[Snapshot]:
        """
        Create a snapshot of the current state machine.
        
        Returns:
            Snapshot: The created snapshot, or None if creation failed
        """
        with self.lock:
            if self.last_applied < 0:
                return None
            
            # Determine snapshot bounds
            snapshot_index = self.last_applied
            snapshot_term = self.log[snapshot_index].term if snapshot_index < len(self.log) else self.current_term
            
            # Create snapshot
            metadata = {
                "node_id": self.node_id,
                "cluster_size": len(self.cluster_nodes),
                "log_length_before": len(self.log),
                "entries_applied": len(self.state_machine)
            }
            
            snapshot = self.snapshot_manager.create_snapshot(
                node_id=self.node_id,
                last_index=snapshot_index,
                last_term=snapshot_term,
                state_machine=self.state_machine,
                metadata=metadata
            )
            
            # Update snapshot tracking
            self.last_snapshot_index = snapshot.index
            self.last_snapshot_term = snapshot.term
            
            # Truncate log (keep some entries for safety)
            self._truncate_log_after_snapshot(snapshot)
            
            self.logger.info(f"Created snapshot at index {snapshot.index}, "
                           f"state machine size: {len(self.state_machine)}")
            
            return snapshot
    
    def _truncate_log_after_snapshot(self, snapshot: Snapshot) -> None:
        """
        Truncate log entries that are included in the snapshot.
        
        Args:
            snapshot: The snapshot that was just created
        """
        # Keep a few entries before snapshot for safety during replication
        keep_entries = 10
        truncate_before_index = max(0, snapshot.index - keep_entries)
        
        if truncate_before_index > 0 and truncate_before_index < len(self.log):
            # Store original log length for metrics
            original_length = len(self.log)
            
            # Truncate log
            self.log = self.log[truncate_before_index:]
            
            # Adjust indices to account for truncated entries
            # Note: In production, this requires careful index management
            
            truncated_entries = original_length - len(self.log)
            self.logger.debug(f"Truncated {truncated_entries} log entries "
                            f"(kept {len(self.log)} entries)")
    
    def get_snapshot_stats(self) -> Dict[str, Any]:
        """Get snapshot-related statistics"""
        with self.lock:
            latest_snapshot = self.snapshot_manager.get_latest_snapshot(self.node_id)
            
            stats = {
                "snapshot_enabled": True,
                "snapshot_interval": self.snapshot_interval,
                "last_snapshot_index": self.last_snapshot_index,
                "last_snapshot_term": self.last_snapshot_term,
                "entries_since_snapshot": self.last_applied - self.last_snapshot_index,
                "should_snapshot": self._should_create_snapshot()
            }
            
            if latest_snapshot:
                stats.update({
                    "latest_snapshot": {
                        "index": latest_snapshot.index,
                        "term": latest_snapshot.term,
                        "timestamp": latest_snapshot.timestamp,
                        "state_machine_entries": len(latest_snapshot.state_machine_data),
                        "age_seconds": time.time() - latest_snapshot.timestamp
                    }
                })
            
            return stats
    
    def force_create_snapshot(self) -> Optional[Snapshot]:
        """Force creation of a snapshot regardless of interval"""
        self.logger.info(f"Force creating snapshot for node {self.node_id}")
        return self._create_snapshot()
    
    def install_snapshot(self, snapshot: Snapshot) -> bool:
        """
        Install a snapshot received from another node.
        
        This would be used during node recovery or when a node
        falls too far behind and needs to catch up via snapshot.
        
        Args:
            snapshot: Snapshot to install
            
        Returns:
            bool: True if snapshot was successfully installed
        """
        with self.lock:
            try:
                # Validate snapshot
                if snapshot.index < self.last_snapshot_index:
                    self.logger.warning(f"Rejecting older snapshot (index {snapshot.index} < {self.last_snapshot_index})")
                    return False
                
                # Install snapshot
                self.state_machine = snapshot.state_machine_data.copy()
                self.last_snapshot_index = snapshot.index
                self.last_snapshot_term = snapshot.term
                self.last_applied = snapshot.index
                self.commit_index = max(self.commit_index, snapshot.index)
                
                # Truncate log entries included in snapshot
                if snapshot.index >= 0 and len(self.log) > snapshot.index:
                    self.log = self.log[snapshot.index + 1:]
                
                # Store snapshot
                self.snapshot_manager.snapshots[self.node_id] = snapshot
                
                self.logger.info(f"Installed snapshot at index {snapshot.index} "
                               f"with {len(self.state_machine)} state machine entries")
                
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to install snapshot: {e}")
                return False
    
    def get_snapshot_for_node(self, requesting_node_id: str) -> Optional[Snapshot]:
        """
        Get snapshot to send to another node.
        
        This would be used when a follower is too far behind
        and needs to catch up via snapshot instead of log replication.
        
        Args:
            requesting_node_id: Node requesting the snapshot
            
        Returns:
            Snapshot: Latest snapshot if available
        """
        latest_snapshot = self.snapshot_manager.get_latest_snapshot(self.node_id)
        
        if latest_snapshot:
            self.logger.info(f"Providing snapshot (index {latest_snapshot.index}) to {requesting_node_id}")
        else:
            self.logger.warning(f"No snapshot available for {requesting_node_id}")
        
        return latest_snapshot
