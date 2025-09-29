"""
Raft Consensus Algorithm Implementation

This package provides a comprehensive implementation of the Raft consensus algorithm,
including core consensus functionality, cluster management, and real-world applications.

Educational implementation demonstrating concepts used in production systems like:
- etcd (Kubernetes)
- Consul (HashiCorp)
- CockroachDB

Author: Advanced Algorithms Series
"""

from .node import RaftNode, NodeState
from .cluster import RaftCluster
from .messages import (
    LogEntry,
    VoteRequest, 
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse
)

__version__ = "1.0.0"
__all__ = [
    "RaftNode",
    "NodeState", 
    "RaftCluster",
    "LogEntry",
    "VoteRequest",
    "VoteResponse", 
    "AppendEntriesRequest",
    "AppendEntriesResponse"
]
