#!/usr/bin/env python3
"""
Quick verification script to ensure the Raft implementation works correctly.
Run this script after installation to verify everything is working.
"""

import sys
import os
import time
import traceback

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all modules can be imported"""
    print("🔄 Testing imports...")
    
    try:
        from raft import RaftNode, RaftCluster, NodeState
        from raft.messages import LogEntry, VoteRequest, AppendEntriesRequest
        from raft.applications import EtcdSimulator, ConsulSimulator, CockroachDBSimulator
        from raft.advanced import SnapshotRaftNode, MultiRaftSystem, RaftMonitor, PerformanceAnalyzer
        print("✅ All imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_basic_raft():
    """Test basic Raft functionality"""
    print("🔄 Testing basic Raft cluster...")
    
    try:
        from raft import RaftCluster
        
        # Create small cluster for quick test
        cluster = RaftCluster(num_nodes=3)
        
        # Wait for leader election
        if not cluster.wait_for_convergence(timeout=5.0):
            print("❌ Leader election failed")
            return False
        
        leader = cluster.get_leader()
        if not leader:
            print("❌ No leader elected")
            return False
        
        # Test client request
        success = cluster.submit_request("SET", {"key": "test", "value": "works"})
        if not success:
            print("❌ Client request failed")
            return False
        
        # Test cluster state
        stats = cluster.get_cluster_stats()
        if stats.total_nodes != 3 or stats.active_nodes < 2:
            print(f"❌ Cluster state invalid: {stats.total_nodes} total, {stats.active_nodes} active")
            return False
        
        cluster.stop()
        print("✅ Basic Raft cluster working")
        return True
        
    except Exception as e:
        print(f"❌ Basic Raft test failed: {e}")
        return False

def test_applications():
    """Test application simulators"""
    print("🔄 Testing application simulators...")
    
    try:
        from raft.applications import EtcdSimulator, ConsulSimulator, CockroachDBSimulator
        
        # Quick etcd test
        etcd = EtcdSimulator(num_masters=3)
        success = etcd.store_kubernetes_resource("Pod", "default", "test-pod", {"containers": []})
        etcd.stop()
        
        if not success:
            print("❌ etcd simulator failed")
            return False
        
        # Quick Consul test
        consul = ConsulSimulator(num_servers=3)
        success = consul.register_service("test", "node1", "10.0.1.10", 8080)
        consul.stop()
        
        if not success:
            print("❌ Consul simulator failed")
            return False
        
        # Quick CockroachDB test
        cockroach = CockroachDBSimulator(num_nodes=3)
        result = cockroach.execute_transaction([
            {"type": "write", "key": "test", "value": "data"}
        ])
        cockroach.stop()
        
        if result['status'] != 'COMMITTED':
            print("❌ CockroachDB simulator failed")
            return False
        
        print("✅ Application simulators working")
        return True
        
    except Exception as e:
        print(f"❌ Application simulators test failed: {e}")
        return False

def test_advanced_features():
    """Test advanced features"""
    print("🔄 Testing advanced features...")
    
    try:
        from raft.advanced import MultiRaftSystem, PerformanceAnalyzer
        from raft import RaftCluster
        
        # Quick Multi-Raft test
        multiraft = MultiRaftSystem(num_shards=2, nodes_per_shard=3)
        success = multiraft.put("test", "value")
        value = multiraft.get("test")
        multiraft.stop()
        
        if not success or value != "value":
            print("❌ Multi-Raft system failed")
            return False
        
        # Quick performance analyzer test
        cluster = RaftCluster(num_nodes=3)
        cluster.wait_for_convergence(timeout=3.0)
        
        from raft.advanced import LoadGenerator
        load_gen = LoadGenerator(cluster)
        results = load_gen.generate_constant_load(1, 5)  # 1 second, 5 req/s
        
        analyzer = PerformanceAnalyzer()
        perf_result = analyzer.analyze_request_results(results, "test", {"size": 3})
        
        cluster.stop()
        
        if perf_result.total_requests == 0:
            print("❌ Performance analysis failed")
            return False
        
        print("✅ Advanced features working")
        return True
        
    except Exception as e:
        print(f"❌ Advanced features test failed: {e}")
        return False

def run_quick_tests():
    """Run quick verification tests"""
    print("🚀 Raft Consensus Algorithm - Quick Verification")
    print("=" * 50)
    
    all_passed = True
    
    # Test imports
    if not test_imports():
        all_passed = False
    
    # Test basic Raft
    if not test_basic_raft():
        all_passed = False
    
    # Test applications
    if not test_applications():
        all_passed = False
    
    # Test advanced features
    if not test_advanced_features():
        all_passed = False
    
    print("=" * 50)
    
    if all_passed:
        print("🎉 All verification tests PASSED!")
        print("\nYour Raft implementation is working correctly.")
        print("\nNext steps:")
        print("- Run: python examples/basic_raft_demo.py")
        print("- Run: python examples/real_world_applications_demo.py")
        print("- Run tests: python -m unittest discover tests/ -v")
        return True
    else:
        print("❌ Some verification tests FAILED!")
        print("\nPlease check the error messages above.")
        return False

if __name__ == '__main__':
    try:
        success = run_quick_tests()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nVerification interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nVerification failed with unexpected error: {e}")
        traceback.print_exc()
        sys.exit(1)
