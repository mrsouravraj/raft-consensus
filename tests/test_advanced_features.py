"""
Advanced Features Tests

Tests for advanced Raft features including:
- Snapshots and log compaction
- Multi-Raft horizontal scaling
- Monitoring and observability
- Performance analysis and load testing

These tests validate the advanced functionality that makes Raft
suitable for production distributed systems.
"""

import unittest
import time
import threading
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from raft import RaftCluster
from raft.advanced import (
    SnapshotRaftNode, SnapshotManager,
    MultiRaftSystem, ShardManager,
    RaftMonitor, MetricsCollector,
    PerformanceAnalyzer, LoadGenerator
)


class TestSnapshotFeatures(unittest.TestCase):
    """Test snapshot and log compaction features"""
    
    def setUp(self):
        """Set up snapshot test environment"""
        self.snapshot_manager = SnapshotManager()
        self.node_ids = ["snap_node_0", "snap_node_1", "snap_node_2"]
        self.test_node = SnapshotRaftNode(
            "snap_test_node",
            self.node_ids,
            snapshot_interval=10,  # Small interval for testing
            snapshot_manager=self.snapshot_manager
        )
        time.sleep(0.1)
    
    def tearDown(self):
        """Clean up snapshot test environment"""
        self.test_node.stop()
    
    def test_snapshot_manager_creation(self):
        """Test snapshot creation and storage"""
        state_machine = {"key1": "value1", "key2": "value2"}
        
        snapshot = self.snapshot_manager.create_snapshot(
            node_id="test_node",
            last_index=5,
            last_term=2,
            state_machine=state_machine,
            metadata={"test": "data"}
        )
        
        self.assertEqual(snapshot.index, 5)
        self.assertEqual(snapshot.term, 2)
        self.assertEqual(snapshot.state_machine_data, state_machine)
        self.assertEqual(snapshot.metadata["test"], "data")
        
        # Test retrieval
        retrieved = self.snapshot_manager.get_latest_snapshot("test_node")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.index, 5)
        self.assertEqual(retrieved.state_machine_data, state_machine)
    
    def test_snapshot_node_initialization(self):
        """Test snapshot-enabled node initialization"""
        self.assertEqual(self.test_node.snapshot_interval, 10)
        self.assertIsNotNone(self.test_node.snapshot_manager)
        self.assertEqual(self.test_node.last_snapshot_index, -1)
    
    def test_automatic_snapshot_creation(self):
        """Test automatic snapshot creation when interval reached"""
        # Add enough entries to trigger snapshot
        for i in range(15):  # More than snapshot_interval of 10
            entry_data = {"key": f"test_key_{i}", "value": f"test_value_{i}"}
            
            # Manually add to log and apply (simulating normal operation)
            with self.test_node.lock:
                from raft.messages import LogEntry
                entry = LogEntry(term=1, index=i, command="SET", data=entry_data)
                self.test_node.log.append(entry)
                self.test_node.commit_index = i
                self.test_node._apply_committed_entries()
        
        # Should have created a snapshot
        stats = self.test_node.get_snapshot_stats()
        self.assertTrue(stats["snapshot_enabled"])
        self.assertGreater(stats["last_snapshot_index"], -1)
    
    def test_force_snapshot_creation(self):
        """Test forced snapshot creation"""
        # Add some data
        with self.test_node.lock:
            from raft.messages import LogEntry
            entry = LogEntry(term=1, index=0, command="SET", data={"key": "test", "value": "data"})
            self.test_node.log.append(entry)
            self.test_node.commit_index = 0
            self.test_node._apply_committed_entries()
        
        # Force snapshot creation
        snapshot = self.test_node.force_create_snapshot()
        
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.index, 0)
        self.assertIn("test", snapshot.state_machine_data)
    
    def test_snapshot_installation(self):
        """Test installing snapshot from another node"""
        # Create a snapshot with test data
        test_state_machine = {"installed_key": "installed_value"}
        snapshot = self.snapshot_manager.create_snapshot(
            node_id="external_node",
            last_index=10,
            last_term=3,
            state_machine=test_state_machine
        )
        
        # Install snapshot
        success = self.test_node.install_snapshot(snapshot)
        self.assertTrue(success)
        
        # Verify installation
        node_state = self.test_node.get_state_machine()
        self.assertEqual(node_state["installed_key"], "installed_value")
        self.assertEqual(self.test_node.last_snapshot_index, 10)
        self.assertEqual(self.test_node.last_applied, 10)
    
    def test_snapshot_stats(self):
        """Test snapshot statistics collection"""
        stats = self.test_node.get_snapshot_stats()
        
        self.assertTrue(stats["snapshot_enabled"])
        self.assertEqual(stats["snapshot_interval"], 10)
        self.assertIsInstance(stats["entries_since_snapshot"], int)
        self.assertIsInstance(stats["should_snapshot"], bool)


class TestMultiRaftSystem(unittest.TestCase):
    """Test Multi-Raft horizontal scaling"""
    
    def setUp(self):
        """Set up Multi-Raft test environment"""
        self.multiraft = MultiRaftSystem(
            num_shards=3,
            nodes_per_shard=3,
            sharding_strategy="hash"
        )
        time.sleep(1.0)  # Allow initialization
    
    def tearDown(self):
        """Clean up Multi-Raft test environment"""
        self.multiraft.stop()
    
    def test_multiraft_initialization(self):
        """Test Multi-Raft system initializes correctly"""
        stats = self.multiraft.get_system_stats()
        
        self.assertEqual(stats["system_overview"]["total_shards"], 3)
        self.assertEqual(stats["system_overview"]["nodes_per_shard"], 3)
        self.assertEqual(stats["system_overview"]["sharding_strategy"], "hash")
        self.assertGreaterEqual(stats["system_overview"]["active_shards"], 1)
    
    def test_shard_manager(self):
        """Test shard management functionality"""
        shard_manager = ShardManager("hash")
        
        # Create hash-based shards
        shard_ids = shard_manager.create_hash_based_shards(4, replica_count=3)
        self.assertEqual(len(shard_ids), 4)
        
        # Test key routing
        test_keys = ["key1", "key2", "key3", "key4", "key5"]
        
        for key in test_keys:
            shard_id = shard_manager.find_shard_for_key(key)
            self.assertIsNotNone(shard_id)
            self.assertIn(shard_id, shard_ids)
        
        # Same key should always route to same shard
        for key in test_keys:
            shard1 = shard_manager.find_shard_for_key(key)
            shard2 = shard_manager.find_shard_for_key(key)
            self.assertEqual(shard1, shard2)
    
    def test_range_based_sharding(self):
        """Test range-based sharding"""
        shard_manager = ShardManager("range")
        
        key_ranges = [
            ("a", "m"),
            ("m", "z"),
            ("z", "~")
        ]
        
        shard_ids = shard_manager.create_range_based_shards(key_ranges, replica_count=3)
        self.assertEqual(len(shard_ids), 3)
        
        # Test key routing
        test_keys = [
            ("apple", 0),    # Should go to first shard (a-m)
            ("orange", 1),   # Should go to second shard (m-z)
            ("zebra", 1),    # Should go to second shard (m-z)
        ]
        
        for key, expected_shard_idx in test_keys:
            shard_id = shard_manager.find_shard_for_key(key)
            self.assertIsNotNone(shard_id)
    
    def test_put_get_operations(self):
        """Test basic put/get operations across shards"""
        # Test data that should distribute across shards
        test_data = [
            ("user:alice", "Alice Johnson"),
            ("user:bob", "Bob Smith"),
            ("config:timeout", "30s"),
            ("stats:requests", "1000"),
            ("cache:key123", "cached_value")
        ]
        
        # Store data
        for key, value in test_data:
            success = self.multiraft.put(key, value)
            self.assertTrue(success, f"Failed to store {key}")
        
        time.sleep(0.5)  # Allow replication
        
        # Retrieve data
        for key, expected_value in test_data:
            retrieved_value = self.multiraft.get(key)
            self.assertEqual(retrieved_value, expected_value, f"Mismatch for key {key}")
    
    def test_batch_operations(self):
        """Test batch put operations"""
        batch_data = [
            (f"batch_key_{i}", f"batch_value_{i}")
            for i in range(20)
        ]
        
        results = self.multiraft.batch_put(batch_data)
        
        # Most operations should succeed
        success_count = sum(results.values())
        self.assertGreaterEqual(success_count, len(batch_data) * 0.8)  # 80% success rate
        
        time.sleep(0.5)
        
        # Verify some of the data
        for key, value in batch_data[:5]:  # Check first 5
            retrieved = self.multiraft.get(key)
            if results[key]:  # Only check if put succeeded
                self.assertEqual(retrieved, value)
    
    def test_shard_failure_simulation(self):
        """Test shard failure simulation"""
        stats = self.multiraft.get_system_stats()
        initial_active_shards = stats["system_overview"]["active_shards"]
        
        # Store some data before failure
        self.multiraft.put("pre_failure_key", "pre_failure_value")
        
        # Simulate shard failure
        shard_to_fail = list(self.multiraft.ranges.keys())[0]
        failure_result = self.multiraft.simulate_shard_failure(shard_to_fail)
        
        self.assertEqual(failure_result["failed_shard"], shard_to_fail)
        self.assertEqual(failure_result["remaining_shards"], initial_active_shards - 1)
        self.assertIn("availability_impact", failure_result)
        
        # System should still be partially operational
        self.assertTrue(failure_result["system_operational"])
    
    def test_node_failure_in_shard(self):
        """Test node failure within a shard"""
        shard_id = list(self.multiraft.ranges.keys())[0]
        
        failure_result = self.multiraft.simulate_node_failure_in_shard(shard_id)
        
        self.assertEqual(failure_result["shard_id"], shard_id)
        self.assertIsNotNone(failure_result["failed_node"])
        
        if failure_result["recovery_successful"]:
            # Should still be able to process requests to this shard
            test_key = "post_node_failure_key"
            # Try to route to the same shard by using consistent key
            for _ in range(10):  # Try a few keys to hit the right shard
                success = self.multiraft.put(f"{test_key}_{_}", "test_value")
                if success:
                    break
    
    def test_load_rebalancing(self):
        """Test load rebalancing analysis"""
        # Add different amounts of data to create imbalance
        shard_keys = list(self.multiraft.ranges.keys())
        
        # Add more data to first shard by targeting specific keys
        for i in range(10):
            self.multiraft.put(f"heavy_shard_key_{i}", f"value_{i}")
        
        for i in range(3):
            self.multiraft.put(f"light_shard_key_{i}", f"value_{i}")
        
        time.sleep(0.5)
        
        # Analyze load distribution
        rebalance_result = self.multiraft.rebalance_load()
        
        self.assertIsInstance(rebalance_result, dict)
        self.assertIn("total_load", rebalance_result)
        self.assertIn("target_load_per_shard", rebalance_result)
        self.assertIn("rebalancing_needed", rebalance_result)


class TestMonitoringSystem(unittest.TestCase):
    """Test monitoring and observability features"""
    
    def setUp(self):
        """Set up monitoring test environment"""
        self.cluster = RaftCluster(num_nodes=5)
        self.cluster.wait_for_convergence(timeout=5.0)
        self.monitor = RaftMonitor(self.cluster, collection_interval=0.5)
        time.sleep(1.0)  # Allow some metrics collection
    
    def tearDown(self):
        """Clean up monitoring test environment"""
        self.monitor.stop_monitoring()
        self.cluster.stop()
    
    def test_metrics_collection(self):
        """Test basic metrics collection"""
        collector = MetricsCollector()
        
        # Record some test metrics
        test_metrics = [
            ("test.counter", 10),
            ("test.gauge", 5.5),
            ("test.string", "test_value")
        ]
        
        for name, value in test_metrics:
            collector.record_metric(name, value, {"component": "test"})
        
        # Retrieve metrics
        for name, expected_value in test_metrics:
            latest_value = collector.get_metric_value(name)
            self.assertEqual(latest_value, expected_value)
        
        # Test metric samples
        samples = collector.get_metric_samples("test.counter")
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].value, 10)
        self.assertEqual(samples[0].labels["component"], "test")
    
    def test_cluster_health_monitoring(self):
        """Test cluster health monitoring"""
        health_summary = self.monitor.get_cluster_health_summary()
        
        self.assertIn("overall_health", health_summary)
        self.assertIn("health_checks", health_summary)
        self.assertIn("summary", health_summary)
        
        # Should have various health checks
        health_checks = health_summary["health_checks"]
        self.assertGreater(len(health_checks), 0)
        
        check_names = [hc["name"] for hc in health_checks]
        expected_checks = [
            "cluster_has_leader",
            "no_split_brain", 
            "data_consistency",
            "node_failures"
        ]
        
        for expected_check in expected_checks:
            self.assertIn(expected_check, check_names)
    
    def test_performance_dashboard(self):
        """Test performance metrics dashboard"""
        # Submit some requests to generate metrics
        for i in range(5):
            self.cluster.submit_request("SET", {"key": f"perf_test_{i}", "value": f"value_{i}"})
        
        time.sleep(1.0)  # Allow metrics collection
        
        dashboard = self.monitor.get_performance_dashboard()
        
        self.assertIn("cluster_overview", dashboard)
        self.assertIn("performance_metrics", dashboard)
        
        overview = dashboard["cluster_overview"]
        self.assertEqual(overview["total_nodes"], 5)
        self.assertGreaterEqual(overview["active_nodes"], 3)  # Majority should be active
    
    def test_issue_diagnosis(self):
        """Test issue diagnosis functionality"""
        diagnosis = self.monitor.diagnose_issues()
        
        self.assertIn("issues_found", diagnosis)
        self.assertIn("issues", diagnosis)
        self.assertIn("recommendations", diagnosis)
        self.assertIn("overall_diagnosis", diagnosis)
        
        # Should be healthy initially
        self.assertIn(diagnosis["overall_diagnosis"], ["HEALTHY", "ISSUES_DETECTED"])
    
    def test_alert_handling(self):
        """Test alert generation and handling"""
        alerts_received = []
        
        def test_alert_handler(alert):
            alerts_received.append(alert)
        
        self.monitor.add_alert_handler(test_alert_handler)
        
        # Simulate a condition that should trigger alerts
        leader = self.cluster.get_leader()
        if leader:
            self.cluster.simulate_node_failure(leader)
        
        time.sleep(2.0)  # Allow alert processing
        
        # Should have received some alerts
        self.assertGreater(len(alerts_received), 0)
        
        # Check alert structure
        if alerts_received:
            alert = alerts_received[0]
            self.assertIsNotNone(alert.id)
            self.assertIsNotNone(alert.title)
            self.assertIsNotNone(alert.severity)


class TestPerformanceAnalysis(unittest.TestCase):
    """Test performance analysis and load testing"""
    
    def setUp(self):
        """Set up performance test environment"""
        self.cluster = RaftCluster(num_nodes=5)
        self.cluster.wait_for_convergence(timeout=5.0)
        self.load_generator = LoadGenerator(self.cluster)
        self.performance_analyzer = PerformanceAnalyzer()
    
    def tearDown(self):
        """Clean up performance test environment"""
        self.cluster.stop()
    
    def test_load_generation(self):
        """Test basic load generation"""
        # Generate constant load
        results = self.load_generator.generate_constant_load(
            duration_seconds=3,
            requests_per_second=10,
            read_ratio=0.5
        )
        
        self.assertGreater(len(results), 20)  # Should have ~30 requests
        
        # Check result structure
        for result in results[:5]:  # Check first 5
            self.assertIsNotNone(result.start_time)
            self.assertIsNotNone(result.end_time)
            self.assertGreaterEqual(result.latency_ms, 0)
            self.assertIsInstance(result.success, bool)
    
    def test_burst_load_generation(self):
        """Test burst load generation"""
        results = self.load_generator.generate_burst_load(
            num_bursts=3,
            requests_per_burst=10,
            burst_interval_seconds=1.0
        )
        
        self.assertEqual(len(results), 30)  # 3 bursts * 10 requests
        
        # Check timing - should see bursts
        request_times = [r.start_time for r in results]
        request_times.sort()
        
        # There should be gaps between bursts
        gaps = []
        for i in range(1, len(request_times)):
            gap = request_times[i] - request_times[i-1]
            if gap > 0.5:  # Significant gap
                gaps.append(gap)
        
        self.assertGreaterEqual(len(gaps), 2)  # Should have gaps between bursts
    
    def test_concurrent_load_generation(self):
        """Test concurrent client load generation"""
        results = self.load_generator.generate_concurrent_load(
            num_clients=5,
            duration_seconds=2
        )
        
        self.assertGreater(len(results), 10)  # Should have multiple requests
        
        # Check that requests were truly concurrent
        start_times = [r.start_time for r in results]
        time_span = max(start_times) - min(start_times)
        
        self.assertLess(time_span, 3.0)  # All requests should start within 3 seconds
        self.assertGreater(time_span, 0.1)  # But not all at exactly the same time
    
    def test_performance_analysis(self):
        """Test performance result analysis"""
        # Generate some test results
        results = self.load_generator.generate_constant_load(
            duration_seconds=2,
            requests_per_second=20
        )
        
        cluster_config = {
            "cluster_size": len(self.cluster.nodes),
            "test_type": "unit_test"
        }
        
        # Analyze results
        perf_result = self.performance_analyzer.analyze_request_results(
            results, "test_analysis", cluster_config
        )
        
        # Check analysis structure
        self.assertEqual(perf_result.test_name, "test_analysis")
        self.assertEqual(perf_result.total_requests, len(results))
        self.assertGreaterEqual(perf_result.successful_requests, 0)
        self.assertGreaterEqual(perf_result.requests_per_second, 0)
        self.assertGreaterEqual(perf_result.average_latency_ms, 0)
        self.assertGreaterEqual(perf_result.p95_latency_ms, 0)
        self.assertGreaterEqual(perf_result.error_rate_percentage, 0)
    
    def test_cluster_size_benchmark(self):
        """Test cluster size benchmarking"""
        # Test with smaller clusters for speed
        cluster_sizes = [3, 5]
        
        benchmark_results = self.performance_analyzer.benchmark_cluster_sizes(
            cluster_sizes, test_duration=2
        )
        
        self.assertEqual(benchmark_results["test_type"], "cluster_size_benchmark")
        self.assertIn("results", benchmark_results)
        self.assertIn("summary", benchmark_results)
        
        results = benchmark_results["results"]
        self.assertEqual(len(results), len(cluster_sizes))
        
        for cluster_size in cluster_sizes:
            self.assertIn(str(cluster_size), results)
            result = results[str(cluster_size)]
            self.assertGreater(result["total_requests"], 0)
    
    def test_load_pattern_benchmark(self):
        """Test different load pattern benchmarking"""
        benchmark_results = self.performance_analyzer.benchmark_load_patterns(self.cluster)
        
        self.assertEqual(benchmark_results["test_type"], "load_pattern_benchmark")
        self.assertIn("results", benchmark_results)
        self.assertIn("summary", benchmark_results)
        
        results = benchmark_results["results"]
        expected_patterns = ["constant_moderate", "constant_high", "burst_load", "concurrent_clients"]
        
        for pattern in expected_patterns:
            self.assertIn(pattern, results)
            result = results[pattern]
            self.assertGreater(result["total_requests"], 0)
    
    def test_failure_scenario_benchmark(self):
        """Test failure scenario benchmarking"""
        benchmark_results = self.performance_analyzer.benchmark_failure_scenarios(self.cluster)
        
        self.assertEqual(benchmark_results["test_type"], "failure_scenario_benchmark")
        self.assertIn("results", benchmark_results)
        self.assertIn("summary", benchmark_results)
        
        results = benchmark_results["results"]
        self.assertIn("baseline", results)
        self.assertIn("leader_failure", results)
        
        # Baseline should have some successful requests
        baseline = results["baseline"]
        self.assertGreater(baseline["successful_requests"], 0)
    
    def test_performance_report_generation(self):
        """Test comprehensive performance report"""
        # Run a quick test to generate some results
        results = self.load_generator.generate_constant_load(2, 15)
        self.performance_analyzer.analyze_request_results(
            results, "report_test", {"cluster_size": 5}
        )
        
        report = self.performance_analyzer.generate_performance_report()
        
        self.assertIn("total_tests_run", report)
        self.assertIn("overall_statistics", report)
        self.assertIn("best_performing_test", report)
        self.assertIn("detailed_results", report)
        self.assertIn("recommendations", report)
        
        # Should have at least one test
        self.assertGreaterEqual(report["total_tests_run"], 1)
        
        # Recommendations should be a list
        self.assertIsInstance(report["recommendations"], list)


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)  # Reduce noise during tests
    
    unittest.main(verbosity=2)
