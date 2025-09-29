# Raft Consensus Algorithm - Educational Implementation

🚀 **A comprehensive educational implementation of the Raft Consensus Algorithm in Python**

This repository provides a complete, well-documented implementation of the Raft consensus algorithm, designed for learning and understanding how distributed consensus works in production systems like etcd, Consul, and CockroachDB.

## 🎯 What You'll Learn

- **Raft Consensus Fundamentals**: Leader election, log replication, and safety guarantees
- **Real-World Applications**: How etcd, Consul, and CockroachDB use Raft in production
- **Advanced Features**: Snapshots, Multi-Raft scaling, monitoring, and performance analysis
- **Distributed Systems Concepts**: Network partitions, failure scenarios, and consistency models

## 🌟 Features

### Core Raft Implementation
- ✅ **Leader Election** with randomized timeouts
- ✅ **Log Replication** with strong consistency guarantees
- ✅ **Safety Properties** as defined in the Raft paper
- ✅ **Network Partition Handling** and failure recovery
- ✅ **Comprehensive Testing** covering edge cases

### Real-World Application Simulators
- 🔧 **etcd Simulator**: Kubernetes cluster state management
- 🌐 **Consul Simulator**: Service discovery and configuration
- 🗄️ **CockroachDB Simulator**: Distributed SQL transactions

### Advanced Features
- 📸 **Snapshots**: Log compaction for production deployments
- 🚀 **Multi-Raft**: Horizontal scaling with multiple Raft groups
- 📊 **Monitoring**: Comprehensive observability and health checks
- ⚡ **Performance Analysis**: Load testing and benchmarking tools

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/mrsouravraj/raft-consensus.git
cd raft-consensus

# No external dependencies required! Uses only Python standard library
python --version  # Requires Python 3.7+
```

### Run Basic Demo

```bash
# Interactive Raft consensus demo
python examples/basic_raft_demo.py

# Real-world applications demo
python examples/real_world_applications_demo.py
```

### Run Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Or use unittest
python -m unittest discover tests/ -v

# Run specific test modules
python -m unittest tests.test_core_raft -v
python -m unittest tests.test_applications -v
python -m unittest tests.test_advanced_features -v
```

## 📚 Project Structure

```
raft-consensus/
├── src/raft/                    # Core Raft implementation
│   ├── __init__.py
│   ├── node.py                  # RaftNode with consensus logic
│   ├── cluster.py               # RaftCluster management
│   ├── messages.py              # Raft protocol messages
│   ├── applications/            # Real-world simulators
│   │   ├── etcd_simulator.py
│   │   ├── consul_simulator.py
│   │   └── cockroachdb_simulator.py
│   └── advanced/                # Advanced features
│       ├── snapshots.py         # Log compaction
│       ├── multi_raft.py        # Horizontal scaling
│       ├── monitoring.py        # Observability
│       └── performance.py       # Load testing
├── tests/                       # Comprehensive test suite
│   ├── test_core_raft.py        # Core algorithm tests
│   ├── test_applications.py     # Application simulator tests
│   └── test_advanced_features.py # Advanced features tests
├── examples/                    # Educational demos
│   ├── basic_raft_demo.py       # Basic consensus demo
│   └── real_world_applications_demo.py
└── docs/                        # Documentation
```

## 🎮 Interactive Demos

### Basic Raft Demo
Experience Raft consensus in action:

```bash
python examples/basic_raft_demo.py
```

**Features:**
- Leader election visualization
- Log replication demonstration
- Network partition handling
- Concurrent operations testing
- Interactive failure scenarios

### Real-World Applications Demo
See how production systems use Raft:

```bash
python examples/real_world_applications_demo.py
```

**Includes:**
- **etcd**: Store and retrieve Kubernetes resources
- **Consul**: Service discovery and configuration management
- **CockroachDB**: Distributed ACID transactions

## 🧪 Core Usage Examples

### Basic Raft Cluster

```python
from raft import RaftCluster

# Create a 5-node Raft cluster
cluster = RaftCluster(num_nodes=5)

# Wait for leader election
cluster.wait_for_convergence(timeout=5.0)

# Submit client requests
success = cluster.submit_request("SET", {
    "key": "user:alice", 
    "value": "Alice Johnson"
})

# Get cluster statistics
stats = cluster.get_cluster_stats()
print(f"Leader: {stats.leader_id}")
print(f"Active nodes: {stats.active_nodes}/{stats.total_nodes}")
```

### etcd for Kubernetes

```python
from raft.applications import EtcdSimulator

# Create etcd cluster
etcd = EtcdSimulator(num_masters=3)

# Store Kubernetes resource
etcd.store_kubernetes_resource(
    resource_type="Pod",
    namespace="default", 
    name="nginx-pod",
    spec={"containers": [{"name": "nginx", "image": "nginx:1.21"}]}
)

# Simulate master failure
result = etcd.simulate_master_failure()
print(f"New leader: {result['new_leader']}")
```

### Consul for Service Discovery

```python
from raft.applications import ConsulSimulator

# Create Consul cluster
consul = ConsulSimulator(num_servers=3, datacenter="dc1")

# Register service
consul.register_service(
    service_name="web-api",
    node_id="server-1", 
    address="10.0.1.10",
    port=8080,
    tags=["production", "v2.1"]
)

# Discover services
instances = consul.discover_service("web-api", tag_filter="production")
print(f"Found {len(instances)} production instances")
```

### CockroachDB Distributed SQL

```python
from raft.applications import CockroachDBSimulator

# Create CockroachDB cluster
cockroach = CockroachDBSimulator(num_nodes=6)

# Execute distributed transaction
result = cockroach.execute_transaction([
    {"type": "write", "key": "user:alice", "value": "Alice Johnson"},
    {"type": "write", "key": "user:bob", "value": "Bob Smith"}
])

print(f"Transaction: {result['status']}")
print(f"Affected ranges: {len(result['affected_ranges'])}")
```

## 🔬 Advanced Features

### Snapshots and Log Compaction

```python
from raft.advanced import SnapshotRaftNode, SnapshotManager

# Create snapshot-enabled node
node = SnapshotRaftNode(
    "node_1", 
    ["node_1", "node_2", "node_3"],
    snapshot_interval=100  # Snapshot every 100 entries
)

# Force snapshot creation
snapshot = node.force_create_snapshot()
print(f"Snapshot created: index {snapshot.index}")
```

### Multi-Raft for Horizontal Scaling

```python
from raft.advanced import MultiRaftSystem

# Create Multi-Raft system
multiraft = MultiRaftSystem(
    num_shards=5, 
    nodes_per_shard=3,
    sharding_strategy="hash"
)

# Store data across shards
multiraft.put("user:alice", "Alice Johnson")
multiraft.put("user:bob", "Bob Smith")

# Data is automatically distributed across shards
value = multiraft.get("user:alice")
print(f"Retrieved: {value}")
```

### Monitoring and Observability

```python
from raft.advanced import RaftMonitor

# Create monitored cluster
cluster = RaftCluster(num_nodes=5)
monitor = RaftMonitor(cluster, collection_interval=1.0)

# Get health summary
health = monitor.get_cluster_health_summary()
print(f"Health: {health['overall_health']}")
print(f"Active alerts: {health['summary']['active_alerts']}")

# Performance dashboard
dashboard = monitor.get_performance_dashboard()
print(f"Current leader: {dashboard['cluster_overview']['current_leader']}")
```

### Performance Analysis

```python
from raft.advanced import PerformanceAnalyzer, LoadGenerator

# Create performance analyzer
analyzer = PerformanceAnalyzer()
load_gen = LoadGenerator(cluster)

# Generate load and analyze
results = load_gen.generate_constant_load(
    duration_seconds=10,
    requests_per_second=100
)

# Analyze performance
perf_result = analyzer.analyze_request_results(
    results, "load_test", {"cluster_size": 5}
)

print(f"Throughput: {perf_result.requests_per_second:.1f} req/s")
print(f"P95 Latency: {perf_result.p95_latency_ms:.1f}ms")
```

## 🧪 Testing

### Run All Tests

```bash
# Using pytest (recommended)
python -m pytest tests/ -v

# Using unittest
python -m unittest discover tests/ -v
```

### Test Coverage

```bash
# Install pytest-cov
pip install pytest pytest-cov

# Run with coverage
python -m pytest tests/ --cov=src/raft --cov-report=html
```

### Test Categories

- **Core Raft Tests** (`tests/test_core_raft.py`): Algorithm correctness
- **Application Tests** (`tests/test_applications.py`): Real-world simulators  
- **Advanced Features** (`tests/test_advanced_features.py`): Snapshots, Multi-Raft, monitoring

## 🎓 Educational Value

### Learning Objectives

After working with this implementation, you'll understand:

1. **Distributed Consensus**: How multiple nodes agree on shared state
2. **Leader Election**: Randomized timeouts and split-brain prevention
3. **Log Replication**: Strong consistency and conflict resolution
4. **Failure Handling**: Network partitions and node failures
5. **Production Systems**: How etcd, Consul, and CockroachDB use Raft

### Key Concepts Demonstrated

- **Safety Properties**: Election safety, leader append-only, log matching
- **Liveness Properties**: Leader election and progress guarantees
- **CAP Theorem**: Consistency and partition tolerance tradeoffs
- **Consensus Algorithms**: Comparison with Paxos and other algorithms

## 📖 References and Further Reading

- **Original Raft Paper**: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- **Raft Visualization**: [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
- **etcd**: [etcd Raft Documentation](https://etcd.io/docs/)
- **Consul**: [Consul Consensus Protocol](https://www.consul.io/docs/architecture/consensus)
- **CockroachDB**: [CockroachDB Architecture](https://www.cockroachlabs.com/docs/stable/architecture/overview.html)

## 🤝 Contributing

This is an educational project! Contributions are welcome:

1. **Bug Reports**: Found an issue? Open an issue
2. **Feature Requests**: Ideas for new educational examples
3. **Documentation**: Improve explanations and examples
4. **Code Quality**: Better error handling, logging, or tests

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Diego Ongaro and John Ousterhout** for creating the Raft algorithm
- **The etcd, Consul, and CockroachDB teams** for production implementations
- **The distributed systems community** for advancing consensus algorithms

---

**⭐ Star this repository if it helped you understand Raft consensus!**

*"The best way to understand distributed systems is to implement them yourself."*