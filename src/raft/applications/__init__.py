"""
Real-World Raft Applications

This package contains educational simulations of how Raft is used in production systems:
- EtcdSimulator: Kubernetes cluster state management
- ConsulSimulator: Service discovery and configuration
- CockroachDBSimulator: Distributed database transactions
"""

from .etcd_simulator import EtcdSimulator
from .consul_simulator import ConsulSimulator
from .cockroachdb_simulator import CockroachDBSimulator

__all__ = [
    "EtcdSimulator",
    "ConsulSimulator", 
    "CockroachDBSimulator"
]
