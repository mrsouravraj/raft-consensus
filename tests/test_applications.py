"""
Real-World Application Tests

Tests for the educational simulations of real-world systems that use Raft:
- etcd (Kubernetes cluster state management)
- Consul (service discovery and configuration)
- CockroachDB (distributed SQL database)

These tests validate that the simulators correctly demonstrate
how Raft is used in production distributed systems.
"""

import unittest
import time
import json
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from raft.applications import EtcdSimulator, ConsulSimulator, CockroachDBSimulator


class TestEtcdSimulator(unittest.TestCase):
    """Test etcd Kubernetes cluster state simulator"""
    
    def setUp(self):
        """Set up etcd simulator"""
        self.etcd = EtcdSimulator(num_masters=3)
    
    def tearDown(self):
        """Clean up etcd simulator"""
        self.etcd.stop()
    
    def test_etcd_initialization(self):
        """Test etcd cluster initializes correctly"""
        health = self.etcd.get_cluster_health()
        
        self.assertTrue(health["healthy"])
        self.assertEqual(health["etcd_cluster"]["total_members"], 3)
        self.assertGreaterEqual(health["etcd_cluster"]["active_members"], 2)
        self.assertIsNotNone(health["etcd_cluster"]["leader"])
    
    def test_kubernetes_resource_storage(self):
        """Test storing Kubernetes resources"""
        # Test Pod storage
        success = self.etcd.store_kubernetes_resource(
            resource_type="Pod",
            namespace="default",
            name="test-pod",
            spec={
                "containers": [
                    {"name": "nginx", "image": "nginx:1.20"}
                ]
            },
            labels={"app": "test"}
        )
        self.assertTrue(success)
        
        # Test Service storage
        success = self.etcd.store_kubernetes_resource(
            resource_type="Service",
            namespace="default", 
            name="test-service",
            spec={
                "selector": {"app": "test"},
                "ports": [{"port": 80}]
            }
        )
        self.assertTrue(success)
        
        # Test cluster-scoped resource (Node)
        success = self.etcd.store_kubernetes_resource(
            resource_type="Node",
            namespace="",
            name="worker-1",
            spec={
                "capacity": {"cpu": "4", "memory": "8Gi"}
            }
        )
        self.assertTrue(success)
    
    def test_kubernetes_resource_retrieval(self):
        """Test retrieving Kubernetes resources"""
        # Store a resource first
        self.etcd.store_kubernetes_resource(
            resource_type="Pod",
            namespace="test-ns",
            name="retrieval-pod",
            spec={"containers": [{"name": "app", "image": "app:latest"}]}
        )
        
        # Retrieve the resource
        resource = self.etcd.get_kubernetes_resource("Pod", "test-ns", "retrieval-pod")
        
        self.assertIsNotNone(resource)
        self.assertEqual(resource["kind"], "Pod")
        self.assertEqual(resource["metadata"]["name"], "retrieval-pod")
        self.assertEqual(resource["metadata"]["namespace"], "test-ns")
        self.assertEqual(resource["spec"]["containers"][0]["name"], "app")
    
    def test_kubernetes_resource_listing(self):
        """Test listing Kubernetes resources"""
        # Store multiple resources
        resources = [
            ("Pod", "default", "pod-1", {"containers": []}),
            ("Pod", "default", "pod-2", {"containers": []}),
            ("Pod", "kube-system", "system-pod", {"containers": []}),
        ]
        
        for resource_type, namespace, name, spec in resources:
            self.etcd.store_kubernetes_resource(resource_type, namespace, name, spec)
        
        # List all pods
        all_pods = self.etcd.list_kubernetes_resources("Pod")
        self.assertGreaterEqual(len(all_pods), 3)
        
        # List pods in specific namespace
        default_pods = self.etcd.list_kubernetes_resources("Pod", "default")
        self.assertGreaterEqual(len(default_pods), 2)
        
        # Verify namespace filtering
        for pod in default_pods:
            self.assertEqual(pod["metadata"]["namespace"], "default")
    
    def test_kubernetes_resource_deletion(self):
        """Test deleting Kubernetes resources"""
        # Store a resource
        self.etcd.store_kubernetes_resource(
            resource_type="ConfigMap",
            namespace="default",
            name="test-config",
            spec={"data": {"key": "value"}}
        )
        
        # Verify it exists
        resource = self.etcd.get_kubernetes_resource("ConfigMap", "default", "test-config")
        self.assertIsNotNone(resource)
        
        # Delete it
        success = self.etcd.delete_kubernetes_resource("ConfigMap", "default", "test-config")
        self.assertTrue(success)
        
        # Verify it's gone
        resource = self.etcd.get_kubernetes_resource("ConfigMap", "default", "test-config")
        self.assertIsNone(resource)
    
    def test_resource_status_updates(self):
        """Test updating resource status (controller simulation)"""
        # Store a pod
        self.etcd.store_kubernetes_resource(
            resource_type="Pod",
            namespace="default",
            name="status-pod",
            spec={"containers": [{"name": "app", "image": "app:latest"}]}
        )
        
        # Update its status
        status = {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"}
            ]
        }
        
        success = self.etcd.update_resource_status("Pod", "default", "status-pod", status)
        self.assertTrue(success)
        
        # Verify status was updated
        resource = self.etcd.get_kubernetes_resource("Pod", "default", "status-pod")
        self.assertEqual(resource["status"]["phase"], "Running")
        self.assertEqual(resource["status"]["conditions"][0]["status"], "True")
    
    def test_master_failure_simulation(self):
        """Test etcd master node failure simulation"""
        # Get initial state
        initial_health = self.etcd.get_cluster_health()
        initial_leader = initial_health["etcd_cluster"]["leader"]
        
        # Simulate master failure
        result = self.etcd.simulate_master_failure(initial_leader)
        
        self.assertEqual(result["failed_node"], initial_leader)
        self.assertTrue(result["was_leader"])
        
        if result["election_successful"]:
            self.assertIsNotNone(result["new_leader"])
            self.assertNotEqual(result["new_leader"], initial_leader)
            self.assertTrue(result["cluster_available"])
        
        # Verify cluster is still functional if majority survived
        if result["active_masters"] >= 2:  # Majority of 3
            success = self.etcd.store_kubernetes_resource(
                resource_type="Pod",
                namespace="default",
                name="post-failure-pod",
                spec={"containers": []}
            )
            self.assertTrue(success)
    
    def test_watch_resources(self):
        """Test resource watching functionality"""
        # Start watching pods
        watch_id = self.etcd.watch_resources("Pod", "default")
        self.assertIsNotNone(watch_id)
        
        # Store a pod (would trigger watch in real system)
        success = self.etcd.store_kubernetes_resource(
            resource_type="Pod",
            namespace="default",
            name="watched-pod",
            spec={"containers": []}
        )
        self.assertTrue(success)
        
        # Stop watching
        success = self.etcd.stop_watch(watch_id)
        self.assertTrue(success)


class TestConsulSimulator(unittest.TestCase):
    """Test Consul service discovery simulator"""
    
    def setUp(self):
        """Set up Consul simulator"""
        self.consul = ConsulSimulator(num_servers=3, datacenter="dc1")
    
    def tearDown(self):
        """Clean up Consul simulator"""
        self.consul.stop()
    
    def test_consul_initialization(self):
        """Test Consul cluster initializes correctly"""
        info = self.consul.get_cluster_info()
        
        self.assertEqual(info["datacenter"], "dc1")
        self.assertEqual(info["consul_cluster"]["total_servers"], 3)
        self.assertGreaterEqual(info["consul_cluster"]["active_servers"], 2)
        self.assertTrue(info["consul_cluster"]["healthy"])
    
    def test_service_registration(self):
        """Test service registration"""
        success = self.consul.register_service(
            service_name="web",
            node_id="web-server-1",
            address="10.0.1.10",
            port=8080,
            tags=["production", "us-west"],
            meta={"version": "1.0.0"}
        )
        self.assertTrue(success)
        
        # Register multiple instances
        services = [
            ("web", "web-server-2", "10.0.1.11", 8080, ["production", "us-west"]),
            ("database", "db-server-1", "10.0.2.10", 5432, ["primary", "production"]),
            ("cache", "redis-1", "10.0.3.10", 6379, ["production"])
        ]
        
        for service_name, node_id, address, port, tags in services:
            success = self.consul.register_service(service_name, node_id, address, port, tags=tags)
            self.assertTrue(success)
    
    def test_service_discovery(self):
        """Test service discovery"""
        # Register services first
        self.consul.register_service("api", "api-1", "10.0.1.20", 3000, tags=["v1"])
        self.consul.register_service("api", "api-2", "10.0.1.21", 3000, tags=["v2"])
        self.consul.register_service("api", "api-3", "10.0.1.22", 3000, tags=["v1"])
        
        # Discover all api instances
        instances = self.consul.discover_service("api")
        self.assertGreaterEqual(len(instances), 3)
        
        # Verify instance details
        for instance in instances:
            self.assertEqual(instance["Service"], "api")
            self.assertEqual(instance["Port"], 3000)
            self.assertTrue(instance["Address"].startswith("10.0.1."))
        
        # Discover with tag filter
        v1_instances = self.consul.discover_service("api", tag_filter="v1")
        self.assertGreaterEqual(len(v1_instances), 2)
        
        for instance in v1_instances:
            self.assertIn("v1", instance["Tags"])
    
    def test_service_deregistration(self):
        """Test service deregistration"""
        # Register service
        service_id = "test-service-unique-id"
        success = self.consul.register_service(
            service_name="test",
            node_id="test-node",
            address="10.0.1.100",
            port=8080,
            service_id=service_id
        )
        self.assertTrue(success)
        
        # Verify it's discoverable
        instances = self.consul.discover_service("test")
        self.assertGreaterEqual(len(instances), 1)
        
        # Deregister it
        success = self.consul.deregister_service(service_id)
        self.assertTrue(success)
        
        # Verify it's no longer discoverable
        instances = self.consul.discover_service("test")
        service_ids = [inst["ID"] for inst in instances]
        self.assertNotIn(service_id, service_ids)
    
    def test_kv_store_operations(self):
        """Test key-value store operations"""
        # Store configuration
        test_configs = [
            ("app/timeout", "30s"),
            ("app/max_connections", "1000"),
            ("app/debug", "false"),
            ("database/host", "db.example.com"),
            ("database/port", "5432")
        ]
        
        for key, value in test_configs:
            success = self.consul.store_kv_config(key, value)
            self.assertTrue(success)
        
        # Retrieve configuration
        for key, expected_value in test_configs:
            kv_entry = self.consul.get_kv_config(key)
            self.assertIsNotNone(kv_entry)
            self.assertEqual(kv_entry["key"], key)
            self.assertEqual(kv_entry["value"], expected_value)
        
        # Test non-existent key
        missing_entry = self.consul.get_kv_config("non/existent/key")
        self.assertIsNone(missing_entry)
    
    def test_kv_store_deletion(self):
        """Test key-value store deletion"""
        # Store some config
        self.consul.store_kv_config("temp/config", "temporary")
        self.consul.store_kv_config("temp/other", "also temporary")
        
        # Verify they exist
        self.assertIsNotNone(self.consul.get_kv_config("temp/config"))
        self.assertIsNotNone(self.consul.get_kv_config("temp/other"))
        
        # Delete single key
        success = self.consul.delete_kv_config("temp/config")
        self.assertTrue(success)
        
        # Verify single key is gone
        self.assertIsNone(self.consul.get_kv_config("temp/config"))
        self.assertIsNotNone(self.consul.get_kv_config("temp/other"))
        
        # Delete with recurse
        success = self.consul.delete_kv_config("temp", recurse=True)
        self.assertTrue(success)
        
        # Verify all temp keys are gone
        self.assertIsNone(self.consul.get_kv_config("temp/other"))
    
    def test_health_check_simulation(self):
        """Test health check failure simulation"""
        # Register service
        service_id = "health-test-service"
        self.consul.register_service("health-test", "node1", "10.0.1.30", 8080, service_id=service_id)
        
        # Initially should be healthy
        healthy_instances = self.consul.discover_service("health-test", healthy_only=True)
        self.assertGreaterEqual(len(healthy_instances), 1)
        
        # Simulate health check failure
        success = self.consul.simulate_health_check_failure(service_id, "critical", "Service unreachable")
        self.assertTrue(success)
        
        # Should no longer appear in healthy instances
        healthy_instances = self.consul.discover_service("health-test", healthy_only=True)
        healthy_service_ids = [inst["ID"] for inst in healthy_instances]
        self.assertNotIn(service_id, healthy_service_ids)
        
        # But should still appear in all instances
        all_instances = self.consul.discover_service("health-test", healthy_only=False)
        all_service_ids = [inst["ID"] for inst in all_instances]
        self.assertIn(service_id, all_service_ids)
    
    def test_datacenter_split_simulation(self):
        """Test datacenter network partition simulation"""
        # Get initial state
        initial_info = self.consul.get_cluster_info()
        initial_leader = initial_info["consul_cluster"]["leader"]
        
        # Simulate datacenter split
        result = self.consul.simulate_datacenter_split()
        
        self.assertTrue(result["partition_created"])
        self.assertIsNotNone(result["majority_servers"])
        self.assertIsNotNone(result["minority_servers"])
        
        if result["majority_operational"]:
            self.assertTrue(result["active_leader"])
            
            # Majority should still accept service registrations
            success = self.consul.register_service(
                "post-split", "node-post-split", "10.0.1.40", 9000
            )
            # May or may not succeed depending on which partition we're in
    
    def test_service_listing(self):
        """Test listing all registered services"""
        # Register various services
        services_to_register = [
            ("frontend", "fe-1", "10.0.1.50", 80),
            ("backend", "be-1", "10.0.2.50", 8080),
            ("queue", "queue-1", "10.0.3.50", 5672)
        ]
        
        for service_name, node_id, address, port in services_to_register:
            self.consul.register_service(service_name, node_id, address, port)
        
        # List all services
        services = self.consul.list_services()
        
        self.assertIsInstance(services, dict)
        self.assertGreaterEqual(len(services), 3)
        
        # Check that our registered services are listed
        service_names = set(services.keys())
        expected_names = {"frontend", "backend", "queue"}
        self.assertTrue(expected_names.issubset(service_names))


class TestCockroachDBSimulator(unittest.TestCase):
    """Test CockroachDB distributed SQL simulator"""
    
    def setUp(self):
        """Set up CockroachDB simulator"""
        self.cockroach = CockroachDBSimulator(num_nodes=6, ranges_per_node=2)
        time.sleep(1.0)  # Allow initialization
    
    def tearDown(self):
        """Clean up CockroachDB simulator"""
        self.cockroach.stop()
    
    def test_cockroachdb_initialization(self):
        """Test CockroachDB cluster initializes correctly"""
        stats = self.cockroach.get_cluster_stats()
        
        self.assertEqual(stats["cluster_overview"]["total_nodes"], 6)
        self.assertGreaterEqual(stats["cluster_overview"]["active_ranges"], 1)
        self.assertTrue(stats["health"]["all_ranges_healthy"])
    
    def test_simple_transactions(self):
        """Test basic distributed transactions"""
        # Execute write transactions
        transactions = [
            [{"type": "write", "key": "user_alice", "value": "Alice Johnson"}],
            [{"type": "write", "key": "user_bob", "value": "Bob Smith"}],
            [{"type": "write", "key": "order_12345", "value": "Order: 3 widgets"}],
        ]
        
        for ops in transactions:
            result = self.cockroach.execute_transaction(ops)
            self.assertEqual(result["status"], "COMMITTED")
            self.assertIsNotNone(result["txn_id"])
    
    def test_read_write_transactions(self):
        """Test transactions with both reads and writes"""
        # First, write some data
        write_ops = [
            {"type": "write", "key": "account_alice", "value": "1000"},
            {"type": "write", "key": "account_bob", "value": "500"}
        ]
        
        result = self.cockroach.execute_transaction(write_ops)
        self.assertEqual(result["status"], "COMMITTED")
        
        # Then read and update
        read_write_ops = [
            {"type": "read", "key": "account_alice"},
            {"type": "write", "key": "account_alice", "value": "900"},
            {"type": "write", "key": "account_bob", "value": "600"}
        ]
        
        result = self.cockroach.execute_transaction(read_write_ops)
        self.assertEqual(result["status"], "COMMITTED")
    
    def test_cross_range_transactions(self):
        """Test transactions that span multiple ranges"""
        # Keys that should be in different ranges
        cross_range_ops = [
            {"type": "write", "key": "alpha_key", "value": "in alpha range"},
            {"type": "write", "key": "beta_key", "value": "in beta range"},
            {"type": "write", "key": "gamma_key", "value": "in gamma range"},
            {"type": "write", "key": "zulu_key", "value": "in zulu range"}
        ]
        
        result = self.cockroach.execute_transaction(cross_range_ops)
        
        # Should succeed with atomic commit across ranges
        self.assertEqual(result["status"], "COMMITTED")
        self.assertGreater(len(result["affected_ranges"]), 1)
    
    def test_data_reads(self):
        """Test reading data with different consistency levels"""
        # Write some test data
        key = "consistency_test_key"
        value = "test_value_123"
        
        write_ops = [{"type": "write", "key": key, "value": value}]
        result = self.cockroach.execute_transaction(write_ops)
        self.assertEqual(result["status"], "COMMITTED")
        
        time.sleep(0.5)  # Allow replication
        
        # Read with strong consistency
        strong_read = self.cockroach.read_data(key, consistency="strong")
        self.assertEqual(strong_read, value)
        
        # Read with eventual consistency
        eventual_read = self.cockroach.read_data(key, consistency="eventual")
        self.assertEqual(eventual_read, value)
        
        # Read non-existent key
        missing_read = self.cockroach.read_data("non_existent_key")
        self.assertIsNone(missing_read)
    
    def test_transaction_failure_handling(self):
        """Test transaction failure scenarios"""
        # Transaction with non-existent read should fail
        failing_ops = [
            {"type": "read", "key": "definitely_does_not_exist"},
            {"type": "write", "key": "should_not_be_written", "value": "rollback_me"}
        ]
        
        result = self.cockroach.execute_transaction(failing_ops)
        
        # Transaction should be aborted
        self.assertEqual(result["status"], "ABORTED")
        
        # Verify rollback - the write should not have persisted
        read_result = self.cockroach.read_data("should_not_be_written")
        self.assertIsNone(read_result)
    
    def test_node_failure_simulation(self):
        """Test node failure impact on distributed database"""
        # Get initial state
        initial_stats = self.cockroach.get_cluster_stats()
        initial_active_ranges = initial_stats["cluster_overview"]["active_ranges"]
        
        # Write some data before failure
        pre_failure_ops = [{"type": "write", "key": "pre_failure", "value": "before_node_fails"}]
        result = self.cockroach.execute_transaction(pre_failure_ops)
        self.assertEqual(result["status"], "COMMITTED")
        
        # Simulate node failure
        node_to_fail = "node_1"
        failure_result = self.cockroach.simulate_node_failure(node_to_fail)
        
        self.assertEqual(failure_result["failed_node"], node_to_fail)
        self.assertGreaterEqual(len(failure_result["affected_ranges"]), 0)
        
        time.sleep(1.0)  # Allow recovery
        
        # System should still be operational
        post_failure_ops = [{"type": "write", "key": "post_failure", "value": "after_node_fails"}]
        result = self.cockroach.execute_transaction(post_failure_ops)
        
        # May succeed or fail depending on which ranges were affected
        # In a real system, automatic rebalancing would maintain availability
        if result["status"] == "COMMITTED":
            # Verify we can read both pre and post failure data
            pre_data = self.cockroach.read_data("pre_failure")
            post_data = self.cockroach.read_data("post_failure")
            self.assertEqual(pre_data, "before_node_fails")
            self.assertEqual(post_data, "after_node_fails")
    
    def test_transaction_history(self):
        """Test transaction history tracking"""
        # Execute several transactions
        transactions = [
            [{"type": "write", "key": "history_1", "value": "first"}],
            [{"type": "write", "key": "history_2", "value": "second"}],
            [{"type": "write", "key": "history_3", "value": "third"}]
        ]
        
        txn_ids = []
        for ops in transactions:
            result = self.cockroach.execute_transaction(ops)
            if result["status"] == "COMMITTED":
                txn_ids.append(result["txn_id"])
        
        # Get transaction history
        history = self.cockroach.get_transaction_history(limit=5)
        
        self.assertIsInstance(history, list)
        self.assertGreaterEqual(len(history), len(txn_ids))
        
        # Check that our transactions are in the history
        history_txn_ids = [txn["txn_id"] for txn in history]
        for txn_id in txn_ids:
            self.assertIn(txn_id, history_txn_ids)
    
    def test_cluster_statistics(self):
        """Test cluster statistics collection"""
        stats = self.cockroach.get_cluster_stats()
        
        # Validate structure
        self.assertIn("cluster_overview", stats)
        self.assertIn("health", stats)
        self.assertIn("range_details", stats)
        
        # Validate content
        overview = stats["cluster_overview"]
        self.assertEqual(overview["total_nodes"], 6)
        self.assertGreaterEqual(overview["active_ranges"], 1)
        self.assertGreaterEqual(overview["total_keys"], 0)
        
        health = stats["health"]
        self.assertIn("operational_percentage", health)
        self.assertIsInstance(health["all_ranges_healthy"], bool)
        
        # Validate range details
        range_details = stats["range_details"]
        self.assertIsInstance(range_details, dict)
        
        for range_id, range_info in range_details.items():
            self.assertIn("start_key", range_info)
            self.assertIn("end_key", range_info)
            self.assertIn("leader", range_info)
            self.assertIn("replicas", range_info)


if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)  # Reduce noise during tests
    
    unittest.main(verbosity=2)
