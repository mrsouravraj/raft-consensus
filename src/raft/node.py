"""
Core Raft Node Implementation

This module implements the core Raft consensus algorithm including:
- Leader election with randomized timeouts
- Log replication with consistency guarantees  
- Safety properties and liveness guarantees
- State machine management

Educational implementation demonstrating concepts used in etcd, Consul, and CockroachDB.
"""

import random
import time
import threading
import logging
from typing import List, Dict, Optional, Set, Any
from .messages import (
    NodeState, LogEntry, VoteRequest, VoteResponse,
    AppendEntriesRequest, AppendEntriesResponse, NodeStats
)


class RaftNode:
    """
    Educational Raft consensus node implementation.
    
    Implements the complete Raft consensus algorithm with:
    - Randomized leader election to prevent split votes
    - Log replication with strong consistency guarantees
    - Automatic failover and recovery
    - State machine for client operations
    
    This implementation demonstrates the concepts used in production systems
    like etcd (Kubernetes), Consul (HashiCorp), and CockroachDB.
    """
    
    def __init__(self, node_id: str, cluster_nodes: List[str], 
                 election_timeout_range: tuple = (0.15, 0.30),
                 heartbeat_interval: float = 0.05):
        """
        Initialize a Raft node.
        
        Args:
            node_id: Unique identifier for this node
            cluster_nodes: List of all node IDs in the cluster
            election_timeout_range: Min/max election timeout in seconds
            heartbeat_interval: Leader heartbeat interval in seconds
        """
        # Persistent state (must survive restarts in production)
        self.node_id = node_id
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Volatile state (all nodes)
        self.commit_index = -1  # Index of highest log entry known to be committed
        self.last_applied = -1  # Index of highest log entry applied to state machine
        self.state = NodeState.FOLLOWER
        
        # Volatile state (leaders only)
        self.next_index: Dict[str, int] = {}   # Next log entry to send to each server
        self.match_index: Dict[str, int] = {}  # Highest log entry known to be replicated
        
        # Cluster configuration
        self.cluster_nodes = cluster_nodes.copy()
        self.majority = len(cluster_nodes) // 2 + 1
        
        # Timing and communication
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout = self._get_election_timeout()
        self.last_heartbeat = time.time()
        self.votes_received: Set[str] = set()
        
        # State machine (key-value store for demonstration)
        self.state_machine: Dict[str, str] = {}
        
        # Thread safety and control
        self.lock = threading.Lock()
        self.running = True
        self.rpc_handlers: Dict[str, RaftNode] = {}  # For inter-node communication
        
        # Logging
        self.logger = logging.getLogger(f"raft.{node_id}")
        
        # Start the main node loop
        self.thread = threading.Thread(target=self._run_node, daemon=True)
        self.thread.start()
        
        self.logger.info(f"Raft node {self.node_id} initialized as {self.state.value}")
    
    def _get_election_timeout(self) -> float:
        """Generate randomized election timeout to prevent split votes"""
        return random.uniform(*self.election_timeout_range)
    
    def _run_node(self) -> None:
        """Main node event loop - handles state transitions and timing"""
        while self.running:
            try:
                with self.lock:
                    if self.state == NodeState.FOLLOWER:
                        self._handle_follower_state()
                    elif self.state == NodeState.CANDIDATE:
                        self._handle_candidate_state()
                    elif self.state == NodeState.LEADER:
                        self._handle_leader_state()
                
                time.sleep(0.01)  # Small sleep to prevent busy waiting
                
            except Exception as e:
                self.logger.error(f"Error in node loop: {e}")
                time.sleep(0.1)
    
    def _handle_follower_state(self) -> None:
        """Handle follower state - wait for heartbeats or start election"""
        if time.time() - self.last_heartbeat > self.election_timeout:
            self.logger.info(f"Election timeout ({self.election_timeout:.3f}s), becoming candidate")
            self._become_candidate()
    
    def _handle_candidate_state(self) -> None:
        """Handle candidate state - request votes from other nodes"""
        # Election timeout - start new election
        if time.time() - self.last_heartbeat > self.election_timeout:
            self._start_election()
    
    def _handle_leader_state(self) -> None:
        """Handle leader state - send heartbeats and replicate log"""
        current_time = time.time()
        if current_time - self.last_heartbeat > self.heartbeat_interval:
            self._send_heartbeats()
            self.last_heartbeat = current_time
    
    def _become_candidate(self) -> None:
        """Transition to candidate state and increment term"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}  # Vote for self
        self.election_timeout = self._get_election_timeout()
        self.last_heartbeat = time.time()
        
        self.logger.info(f"Became CANDIDATE for term {self.current_term}")
        
        # Immediately start election
        self._start_election()
    
    def _start_election(self) -> None:
        """Send vote requests to all other nodes"""
        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1].term if self.log else 0
        
        vote_request = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
        
        self.logger.debug(f"Requesting votes for term {self.current_term}")
        
        # Send vote requests to all other nodes
        for node_id in self.cluster_nodes:
            if node_id != self.node_id and node_id in self.rpc_handlers:
                try:
                    response = self.rpc_handlers[node_id].handle_vote_request(vote_request)
                    if response.vote_granted and response.term == self.current_term:
                        self.votes_received.add(node_id)
                        self.logger.debug(f"Received vote from {node_id}")
                    elif response.term > self.current_term:
                        # Discovered higher term
                        self._update_term(response.term)
                        return
                except Exception as e:
                    self.logger.debug(f"Failed to get vote from {node_id}: {e}")
        
        # Check if we have majority votes
        if len(self.votes_received) >= self.majority:
            self._become_leader()
        else:
            # If election timeout elapsed without becoming leader, try again
            if time.time() - self.last_heartbeat > self.election_timeout:
                self.logger.debug(f"Election failed, got {len(self.votes_received)}/{self.majority} votes")
                self._become_follower()
    
    def _become_leader(self) -> None:
        """Transition to leader state and initialize leader state"""
        if self.state != NodeState.CANDIDATE:
            return
            
        self.state = NodeState.LEADER
        
        # Initialize leader volatile state
        last_log_index = len(self.log)
        for node_id in self.cluster_nodes:
            if node_id != self.node_id:
                self.next_index[node_id] = last_log_index
                self.match_index[node_id] = -1
        
        self.logger.info(f"Became LEADER for term {self.current_term}")
        
        # Send immediate heartbeat to establish leadership
        self._send_heartbeats()
    
    def _become_follower(self, new_term: Optional[int] = None) -> None:
        """Transition to follower state"""
        self.state = NodeState.FOLLOWER
        if new_term is not None:
            self.current_term = new_term
        self.voted_for = None
        self.votes_received.clear()
        self.last_heartbeat = time.time()
        self.election_timeout = self._get_election_timeout()
        
        self.logger.debug(f"Became FOLLOWER for term {self.current_term}")
    
    def _update_term(self, new_term: int) -> None:
        """Update current term and become follower if term is higher"""
        if new_term > self.current_term:
            self.current_term = new_term
            self.voted_for = None
            self._become_follower()
    
    def _send_heartbeats(self) -> None:
        """Send heartbeat (empty AppendEntries) to all followers"""
        if self.state != NodeState.LEADER:
            return
        
        for node_id in self.cluster_nodes:
            if node_id != self.node_id and node_id in self.rpc_handlers:
                self._send_append_entries(node_id, heartbeat=True)
    
    def _send_append_entries(self, node_id: str, heartbeat: bool = False) -> None:
        """Send AppendEntries RPC to a specific node"""
        try:
            next_index = self.next_index.get(node_id, len(self.log))
            prev_log_index = next_index - 1
            prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
            
            # Determine entries to send
            entries = [] if heartbeat else self.log[next_index:]
            
            request = AppendEntriesRequest(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self.commit_index
            )
            
            response = self.rpc_handlers[node_id].handle_append_entries(request)
            
            if response.term > self.current_term:
                # Discovered higher term - step down
                self._update_term(response.term)
                return
            
            if response.success:
                # Update follower indices
                if entries:
                    self.match_index[node_id] = prev_log_index + len(entries)
                    self.next_index[node_id] = self.match_index[node_id] + 1
                    self.logger.debug(f"Successfully replicated to {node_id}")
                    
                    # Check if we can advance commit index
                    self._advance_commit_index()
            else:
                # AppendEntries failed - decrement next_index and retry
                self.next_index[node_id] = max(0, self.next_index[node_id] - 1)
                self.logger.debug(f"AppendEntries failed for {node_id}, retry with index {self.next_index[node_id]}")
                
        except Exception as e:
            self.logger.debug(f"Failed to send AppendEntries to {node_id}: {e}")
    
    def _advance_commit_index(self) -> None:
        """Advance commit index if majority of followers have replicated"""
        if self.state != NodeState.LEADER:
            return
        
        # Find highest index replicated by majority
        for index in range(self.commit_index + 1, len(self.log)):
            if self.log[index].term != self.current_term:
                continue
                
            # Count nodes that have this entry (including leader)
            replicated_count = 1  # Leader has it
            for node_id in self.cluster_nodes:
                if node_id != self.node_id:
                    if self.match_index.get(node_id, -1) >= index:
                        replicated_count += 1
            
            if replicated_count >= self.majority:
                self.commit_index = index
                self.logger.debug(f"Advanced commit index to {index}")
                self._apply_committed_entries()
            else:
                break  # Can't commit this or higher entries yet
    
    def _apply_committed_entries(self) -> None:
        """Apply committed log entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                self._apply_entry_to_state_machine(entry)
    
    def _apply_entry_to_state_machine(self, entry: LogEntry) -> None:
        """Apply a single log entry to the state machine"""
        try:
            if entry.command == "SET":
                key = entry.data.get("key")
                value = entry.data.get("value")
                if key is not None and value is not None:
                    self.state_machine[key] = value
                    self.logger.debug(f"Applied: SET {key}={value}")
            elif entry.command == "DELETE":
                key = entry.data.get("key")
                if key is not None and key in self.state_machine:
                    del self.state_machine[key]
                    self.logger.debug(f"Applied: DELETE {key}")
        except Exception as e:
            self.logger.error(f"Failed to apply entry {entry}: {e}")
    
    # Public API Methods
    
    def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Handle incoming RequestVote RPC"""
        with self.lock:
            self.logger.debug(f"Received vote request: {request}")
            
            # Update term if request has higher term
            if request.term > self.current_term:
                self._update_term(request.term)
            
            vote_granted = False
            
            if request.term == self.current_term:
                # Grant vote if:
                # 1. Haven't voted in this term OR already voted for this candidate
                # 2. Candidate's log is at least as up-to-date as ours
                if (self.voted_for is None or self.voted_for == request.candidate_id):
                    candidate_log_ok = self._is_candidate_log_up_to_date(
                        request.last_log_term, request.last_log_index
                    )
                    
                    if candidate_log_ok:
                        vote_granted = True
                        self.voted_for = request.candidate_id
                        self.last_heartbeat = time.time()  # Reset election timer
                        self.logger.debug(f"Granted vote to {request.candidate_id}")
            
            response = VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )
            
            if not vote_granted:
                self.logger.debug(f"Denied vote to {request.candidate_id}")
            
            return response
    
    def _is_candidate_log_up_to_date(self, candidate_last_term: int, candidate_last_index: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours"""
        if not self.log:
            return True  # Empty log is always ok
        
        our_last_term = self.log[-1].term
        our_last_index = len(self.log) - 1
        
        # Candidate is more up-to-date if:
        # 1. Last log term is higher, OR
        # 2. Last log term is same but index is >= ours
        return (candidate_last_term > our_last_term or 
                (candidate_last_term == our_last_term and candidate_last_index >= our_last_index))
    
    def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPC"""
        with self.lock:
            self.logger.debug(f"Received append entries: {request}")
            
            # Update term if request has higher term
            if request.term > self.current_term:
                self._update_term(request.term)
            
            success = False
            
            if request.term == self.current_term:
                # Valid leader for this term
                self._become_follower()
                self.last_heartbeat = time.time()
                
                # Check if our log contains an entry at prev_log_index with matching term
                prev_log_ok = (
                    request.prev_log_index == -1 or  # No previous entry
                    (request.prev_log_index < len(self.log) and
                     self.log[request.prev_log_index].term == request.prev_log_term)
                )
                
                if prev_log_ok:
                    success = True
                    
                    # Remove any conflicting entries and append new ones
                    if request.entries:
                        # Find first conflicting entry
                        new_entries_start = request.prev_log_index + 1
                        
                        # Remove conflicting entries
                        if new_entries_start < len(self.log):
                            for i, new_entry in enumerate(request.entries):
                                log_index = new_entries_start + i
                                if (log_index < len(self.log) and 
                                    self.log[log_index].term != new_entry.term):
                                    # Remove this and all following entries
                                    self.log = self.log[:log_index]
                                    break
                        
                        # Append any new entries
                        for i, new_entry in enumerate(request.entries):
                            log_index = new_entries_start + i
                            if log_index >= len(self.log):
                                self.log.append(new_entry)
                        
                        self.logger.debug(f"Appended {len(request.entries)} entries")
                    
                    # Update commit index
                    if request.leader_commit > self.commit_index:
                        self.commit_index = min(request.leader_commit, len(self.log) - 1)
                        self._apply_committed_entries()
                        self.logger.debug(f"Updated commit index to {self.commit_index}")
            
            response = AppendEntriesResponse(
                term=self.current_term,
                success=success
            )
            
            return response
    
    def client_request(self, command: str, data: Dict[str, Any]) -> bool:
        """
        Handle client request - only leader can process writes.
        
        Args:
            command: Command to execute (e.g., "SET", "DELETE")
            data: Command data (e.g., {"key": "foo", "value": "bar"})
            
        Returns:
            bool: True if request was successfully processed
        """
        with self.lock:
            if self.state != NodeState.LEADER:
                self.logger.debug("Rejecting client request - not leader")
                return False
            
            # Create new log entry
            new_entry = LogEntry(
                term=self.current_term,
                index=len(self.log),
                command=command,
                data=data.copy()
            )
            
            self.log.append(new_entry)
            self.logger.info(f"Added log entry {new_entry.index}: {command}")
            
            # In a real implementation, we would wait for majority replication
            # For this educational version, we replicate synchronously in the background
            # and apply immediately for simplicity
            self._replicate_to_followers()
            
            return True
    
    def _replicate_to_followers(self) -> None:
        """Replicate log entries to all followers (simplified for education)"""
        if self.state != NodeState.LEADER:
            return
        
        # Send append entries to all followers
        for node_id in self.cluster_nodes:
            if node_id != self.node_id and node_id in self.rpc_handlers:
                self._send_append_entries(node_id)
    
    def get_stats(self) -> NodeStats:
        """Get current node statistics"""
        with self.lock:
            # Find current leader
            leader_id = None
            if self.state == NodeState.LEADER:
                leader_id = self.node_id
            # In a real system, followers would track the current leader
            
            return NodeStats(
                node_id=self.node_id,
                state=self.state,
                term=self.current_term,
                log_length=len(self.log),
                commit_index=self.commit_index,
                last_applied=self.last_applied,
                voted_for=self.voted_for,
                leader_id=leader_id,
                state_machine_size=len(self.state_machine)
            )
    
    def get_state_machine(self) -> Dict[str, str]:
        """Get current state machine contents (thread-safe)"""
        with self.lock:
            return self.state_machine.copy()
    
    def add_rpc_handler(self, node_id: str, handler: 'RaftNode') -> None:
        """Add RPC handler for communication with another node"""
        self.rpc_handlers[node_id] = handler
    
    def stop(self) -> None:
        """Stop the node gracefully"""
        self.running = False
        if self.thread.is_alive():
            self.thread.join(timeout=1.0)
        self.logger.info(f"Node {self.node_id} stopped")
    
    def is_leader(self) -> bool:
        """Check if this node is currently the leader"""
        with self.lock:
            return self.state == NodeState.LEADER
    
    def __str__(self) -> str:
        return f"RaftNode({self.node_id}, {self.state.value}, term={self.current_term})"
