"""
Raft Protocol Messages and Data Structures

This module defines all the data structures and messages used in the Raft consensus protocol.
These structures follow the original Raft paper specification and are used for communication
between nodes in the cluster.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from enum import Enum


class NodeState(Enum):
    """Possible states for a Raft node"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"  
    LEADER = "leader"


@dataclass
class LogEntry:
    """
    Represents a single log entry in the Raft log.
    
    Each log entry contains:
    - term: The term when the entry was created by the leader
    - index: The position of this entry in the log
    - command: The command to be applied to the state machine
    - data: The data associated with the command
    """
    term: int
    index: int
    command: str
    data: Dict[str, Any]
    
    def __str__(self) -> str:
        return f"LogEntry(term={self.term}, index={self.index}, command={self.command})"


@dataclass
class VoteRequest:
    """
    RequestVote RPC message.
    
    Sent by candidates to gather votes during leader election.
    Contains information about the candidate's log to ensure
    only up-to-date candidates can become leader.
    """
    term: int              # Candidate's term
    candidate_id: str      # Candidate requesting vote
    last_log_index: int    # Index of candidate's last log entry
    last_log_term: int     # Term of candidate's last log entry
    
    def __str__(self) -> str:
        return f"VoteRequest(term={self.term}, candidate={self.candidate_id})"


@dataclass 
class VoteResponse:
    """
    RequestVote RPC response.
    
    Sent in response to VoteRequest to grant or deny vote.
    """
    term: int           # Current term, for candidate to update itself
    vote_granted: bool  # True means candidate received vote
    
    def __str__(self) -> str:
        return f"VoteResponse(term={self.term}, granted={self.vote_granted})"


@dataclass
class AppendEntriesRequest:
    """
    AppendEntries RPC message.
    
    Sent by leader to replicate log entries to followers.
    Also serves as heartbeat when entries list is empty.
    """
    term: int                    # Leader's term
    leader_id: str              # So follower can redirect clients
    prev_log_index: int         # Index of log entry immediately preceding new ones
    prev_log_term: int          # Term of prev_log_index entry
    entries: List[LogEntry]     # Log entries to store (empty for heartbeat)
    leader_commit: int          # Leader's commit_index
    
    def __str__(self) -> str:
        entry_count = len(self.entries)
        return f"AppendEntries(term={self.term}, leader={self.leader_id}, entries={entry_count})"


@dataclass
class AppendEntriesResponse:
    """
    AppendEntries RPC response.
    
    Sent in response to AppendEntriesRequest to indicate success/failure.
    """
    term: int      # Current term, for leader to update itself
    success: bool  # True if follower contained entry matching prev_log_index and prev_log_term
    
    def __str__(self) -> str:
        return f"AppendEntriesResponse(term={self.term}, success={self.success})"


@dataclass
class NodeStats:
    """Statistics and state information for a Raft node"""
    node_id: str
    state: NodeState
    term: int
    log_length: int
    commit_index: int
    last_applied: int
    voted_for: Optional[str]
    leader_id: Optional[str]
    state_machine_size: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.term,
            "log_length": self.log_length,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "voted_for": self.voted_for,
            "leader_id": self.leader_id,
            "state_machine_size": self.state_machine_size
        }


@dataclass
class ClusterStats:
    """Statistics for the entire Raft cluster"""
    total_nodes: int
    active_nodes: int
    leader_id: Optional[str]
    current_term: int
    nodes: Dict[str, NodeStats]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "total_nodes": self.total_nodes,
            "active_nodes": self.active_nodes, 
            "leader_id": self.leader_id,
            "current_term": self.current_term,
            "nodes": {node_id: stats.to_dict() for node_id, stats in self.nodes.items()}
        }
