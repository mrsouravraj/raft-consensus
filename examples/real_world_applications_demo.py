#!/usr/bin/env python3
"""
Real-World Raft Applications Demo

Demonstrates how Raft is used in production systems:
- etcd for Kubernetes cluster state management
- Consul for service discovery and configuration
- CockroachDB for distributed SQL transactions

These educational simulations show how Raft enables
critical distributed systems used in production.
"""

import sys
import os
import time
import logging
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from raft.applications import EtcdSimulator, ConsulSimulator, CockroachDBSimulator


def setup_logging():
    """Configure logging for the demo"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Reduce noise from internal components
    logging.getLogger('raft.node').setLevel(logging.WARNING)
    logging.getLogger('raft.cluster').setLevel(logging.WARNING)


def print_section(title):
    """Print a section header"""
    print(f"\n{'='*70}")
    print(f" {title}")
    print(f"{'='*70}")


def demonstrate_etcd_kubernetes():
    """Demonstrate etcd for Kubernetes cluster state management"""
    print_section("etcd - Kubernetes Cluster State Management")
    
    print("🚀 Starting etcd cluster for Kubernetes...")
    etcd = EtcdSimulator(num_masters=3)
    
    try:
        # Show initial cluster health
        health = etcd.get_cluster_health()
        print(f"✅ etcd cluster healthy: {health['healthy']}")
        print(f"   Active masters: {health['etcd_cluster']['active_members']}/3")
        print(f"   Leader: {health['etcd_cluster']['leader']}")
        
        # Store Kubernetes resources
        print("\n📋 Storing Kubernetes resources in etcd...")
        
        k8s_resources = [
            ("Pod", "default", "nginx-deployment-1", {
                "containers": [{"name": "nginx", "image": "nginx:1.21"}]
            }, {"app": "nginx", "tier": "frontend"}),
            
            ("Pod", "default", "nginx-deployment-2", {
                "containers": [{"name": "nginx", "image": "nginx:1.21"}]
            }, {"app": "nginx", "tier": "frontend"}),
            
            ("Service", "default", "nginx-service", {
                "selector": {"app": "nginx"},
                "ports": [{"port": 80, "targetPort": 80}]
            }, {"tier": "frontend"}),
            
            ("Node", "", "worker-node-1", {
                "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"}
            }, {"zone": "us-west-1a"}),
            
            ("Node", "", "worker-node-2", {
                "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"}
            }, {"zone": "us-west-1b"}),
            
            ("ConfigMap", "default", "app-config", {
                "data": {
                    "database_url": "postgres://db:5432/myapp",
                    "log_level": "INFO",
                    "max_connections": "100"
                }
            })
        ]
        
        for resource_type, namespace, name, spec, labels in k8s_resources:
            success = etcd.store_kubernetes_resource(resource_type, namespace, name, spec, labels)
            status = "✅" if success else "❌"
            resource_path = f"{namespace}/{name}" if namespace else name
            print(f"   {status} {resource_type}: {resource_path}")
        
        # List resources
        print("\n📖 Listing stored resources...")
        pods = etcd.list_kubernetes_resources("Pod", "default")
        nodes = etcd.list_kubernetes_resources("Node")
        print(f"   Found {len(pods)} pods in 'default' namespace")
        print(f"   Found {len(nodes)} cluster nodes")
        
        # Demonstrate resource retrieval
        print("\n🔍 Retrieving specific resources...")
        nginx_pod = etcd.get_kubernetes_resource("Pod", "default", "nginx-deployment-1")
        if nginx_pod:
            print(f"   Retrieved pod: {nginx_pod['metadata']['name']}")
            print(f"   Resource version: {nginx_pod['metadata']['resourceVersion']}")
            print(f"   Labels: {nginx_pod['metadata']['labels']}")
        
        # Simulate controller updating pod status
        print("\n🔄 Simulating controller status updates...")
        pod_status = {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "PodReady"},
                {"type": "PodScheduled", "status": "True", "reason": "PodScheduled"}
            ],
            "containerStatuses": [
                {"name": "nginx", "ready": True, "state": {"running": {"startedAt": "2024-01-01T12:00:00Z"}}}
            ]
        }
        
        success = etcd.update_resource_status("Pod", "default", "nginx-deployment-1", pod_status)
        print(f"   {'✅' if success else '❌'} Updated pod status to Running")
        
        # Demonstrate master failure handling
        print("\n💥 Simulating etcd master failure...")
        failure_result = etcd.simulate_master_failure()
        
        if failure_result.get('election_successful'):
            print(f"   ✅ New etcd leader elected: {failure_result['new_leader']}")
            print("   🎉 Kubernetes API server remains available!")
            
            # Test that cluster is still functional
            success = etcd.store_kubernetes_resource(
                "Pod", "default", "post-failure-pod",
                {"containers": [{"name": "test", "image": "busybox"}]}
            )
            print(f"   {'✅' if success else '❌'} Post-failure resource storage")
        else:
            print("   ❌ etcd cluster lost quorum - Kubernetes API unavailable")
        
        # Show final cluster health
        final_health = etcd.get_cluster_health()
        print(f"\n📊 Final cluster health:")
        print(f"   Healthy: {final_health['healthy']}")
        print(f"   Total resources: {final_health['kubernetes_data']['total_resources']}")
        print(f"   Resource types: {list(final_health['kubernetes_data']['resource_counts'].keys())}")
        
    finally:
        etcd.stop()


def demonstrate_consul_service_discovery():
    """Demonstrate Consul for service discovery and configuration"""
    print_section("Consul - Service Discovery and Configuration Management")
    
    print("🚀 Starting Consul cluster...")
    consul = ConsulSimulator(num_servers=3, datacenter="dc1")
    
    try:
        # Show initial cluster info
        info = consul.get_cluster_info()
        print(f"✅ Consul cluster healthy in {info['datacenter']}")
        print(f"   Active servers: {info['consul_cluster']['active_servers']}/3")
        print(f"   Leader: {info['consul_cluster']['leader']}")
        
        # Register microservices
        print("\n🔧 Registering microservices...")
        
        services = [
            ("frontend", "web-server-1", "10.0.1.10", 3000, ["production", "v2.1", "nginx"]),
            ("frontend", "web-server-2", "10.0.1.11", 3000, ["production", "v2.1", "nginx"]),
            ("frontend", "web-server-3", "10.0.1.12", 3000, ["production", "v2.0", "nginx"]),
            
            ("api-gateway", "gateway-1", "10.0.2.10", 8080, ["production", "kong"]),
            ("api-gateway", "gateway-2", "10.0.2.11", 8080, ["production", "kong"]),
            
            ("auth-service", "auth-1", "10.0.3.10", 8081, ["production", "jwt", "oauth"]),
            ("auth-service", "auth-2", "10.0.3.11", 8081, ["production", "jwt", "oauth"]),
            
            ("user-service", "user-api-1", "10.0.4.10", 8082, ["production", "postgres"]),
            ("user-service", "user-api-2", "10.0.4.11", 8082, ["staging", "postgres"]),
            
            ("payment-service", "payment-1", "10.0.5.10", 8083, ["production", "secure"]),
            
            ("database", "postgres-primary", "10.0.10.10", 5432, ["primary", "postgres-13"]),
            ("database", "postgres-replica", "10.0.10.11", 5432, ["replica", "postgres-13"]),
            
            ("cache", "redis-cluster-1", "10.0.11.10", 6379, ["production", "redis-6"]),
            ("cache", "redis-cluster-2", "10.0.11.11", 6379, ["production", "redis-6"]),
            
            ("monitoring", "prometheus", "10.0.20.10", 9090, ["monitoring", "metrics"]),
            ("monitoring", "grafana", "10.0.20.11", 3000, ["monitoring", "dashboard"])
        ]
        
        for service_name, instance_id, address, port, tags in services:
            success = consul.register_service(service_name, instance_id, address, port, tags=tags)
            status = "✅" if success else "❌"
            print(f"   {status} {service_name}: {instance_id} at {address}:{port}")
        
        # Demonstrate service discovery
        print("\n🔍 Service discovery queries...")
        
        discovery_queries = [
            ("frontend", None, "All frontend instances"),
            ("database", "primary", "Primary database only"),
            ("api-gateway", None, "API gateway instances"),
            ("cache", "production", "Production cache instances")
        ]
        
        for service_name, tag_filter, description in discovery_queries:
            instances = consul.discover_service(service_name, tag_filter=tag_filter)
            print(f"   🎯 {description}: {len(instances)} instances found")
            
            if instances:
                for instance in instances[:2]:  # Show first 2
                    print(f"      - {instance['ID']} at {instance['Address']}:{instance['Port']}")
        
        # Store configuration data
        print("\n⚙️  Storing application configuration...")
        
        configs = [
            ("app/frontend/api_endpoint", "https://api.example.com"),
            ("app/frontend/cdn_url", "https://cdn.example.com"),
            ("app/frontend/feature_flags/new_ui", "true"),
            
            ("app/auth/jwt_secret", "super-secret-key"),
            ("app/auth/token_expiry", "24h"),
            ("app/auth/allowed_origins", "https://app.example.com,https://admin.example.com"),
            
            ("database/connection_pool/max_connections", "100"),
            ("database/connection_pool/idle_timeout", "10m"),
            ("database/backup/schedule", "0 2 * * *"),
            
            ("monitoring/prometheus/scrape_interval", "15s"),
            ("monitoring/prometheus/retention", "30d"),
            
            ("infrastructure/datacenter", "us-west-1"),
            ("infrastructure/environment", "production")
        ]
        
        for key, value in configs:
            success = consul.store_kv_config(key, value)
            status = "✅" if success else "❌"
            print(f"   {status} {key} = {value}")
        
        # Demonstrate configuration retrieval
        print("\n📖 Retrieving configuration...")
        app_configs = [
            "app/frontend/api_endpoint",
            "app/auth/token_expiry",
            "database/connection_pool/max_connections"
        ]
        
        for config_key in app_configs:
            config_entry = consul.get_kv_config(config_key)
            if config_entry:
                print(f"   🔧 {config_key}: {config_entry['value']}")
        
        # Simulate health check failures
        print("\n🏥 Simulating service health issues...")
        
        # Make one frontend instance unhealthy
        consul.simulate_health_check_failure(
            "web-server-1", "critical", "HTTP 500 - Service unavailable"
        )
        print("   ❌ web-server-1 marked as unhealthy")
        
        # Show healthy vs all instances
        healthy_frontend = consul.discover_service("frontend", healthy_only=True)
        all_frontend = consul.discover_service("frontend", healthy_only=False)
        
        print(f"   📊 Frontend instances: {len(all_frontend)} total, {len(healthy_frontend)} healthy")
        
        # Simulate datacenter partition
        print("\n🌐 Simulating datacenter network partition...")
        partition_result = consul.simulate_datacenter_split()
        
        if partition_result['majority_operational']:
            print("   ✅ Majority partition remains operational")
            print(f"   🎯 Active leader: {partition_result['active_leader']}")
            
            # Test service registration during partition
            success = consul.register_service(
                "emergency-service", "emergency-1", "10.0.99.10", 9999
            )
            print(f"   {'✅' if success else '❌'} Emergency service registration during partition")
        else:
            print("   ❌ Service discovery unavailable due to partition")
        
        # Show final cluster state
        final_info = consul.get_cluster_info()
        print(f"\n📊 Final cluster state:")
        print(f"   Services: {final_info['services']['registered_services']} types")
        print(f"   Service instances: {final_info['services']['total_instances']}")
        print(f"   KV store keys: {final_info['kv_store']['total_keys']}")
        print(f"   Consistency: {final_info['consistency']['percentage']:.1f}%")
        
    finally:
        consul.stop()


def demonstrate_cockroachdb_distributed_sql():
    """Demonstrate CockroachDB for distributed SQL transactions"""
    print_section("CockroachDB - Distributed SQL Database")
    
    print("🚀 Starting CockroachDB cluster...")
    cockroach = CockroachDBSimulator(num_nodes=6, ranges_per_node=2)
    
    try:
        # Show initial cluster stats
        stats = cockroach.get_cluster_stats()
        print(f"✅ CockroachDB cluster initialized")
        print(f"   Nodes: {stats['cluster_overview']['total_nodes']}")
        print(f"   Active ranges: {stats['cluster_overview']['active_ranges']}")
        print(f"   Health: {'Healthy' if stats['health']['all_ranges_healthy'] else 'Degraded'}")
        
        # Execute distributed transactions
        print("\n💳 Executing distributed transactions...")
        
        # Transaction 1: Create user accounts
        print("   📊 Transaction 1: Creating user accounts...")
        user_txn = [
            {"type": "write", "key": "users/alice", "value": json.dumps({
                "id": "alice", "name": "Alice Johnson", "email": "alice@example.com", "balance": 1000.00
            })},
            {"type": "write", "key": "users/bob", "value": json.dumps({
                "id": "bob", "name": "Bob Smith", "email": "bob@example.com", "balance": 500.00
            })},
            {"type": "write", "key": "users/charlie", "value": json.dumps({
                "id": "charlie", "name": "Charlie Brown", "email": "charlie@example.com", "balance": 750.00
            })}
        ]
        
        result = cockroach.execute_transaction(user_txn)
        status = "✅" if result['status'] == 'COMMITTED' else "❌"
        print(f"      {status} User creation: {result['status']} (txn: {result['txn_id']})")
        if result['status'] == 'COMMITTED':
            print(f"         Affected {len(result['affected_ranges'])} ranges")
        
        # Transaction 2: Create product catalog
        print("   🛍️  Transaction 2: Setting up product catalog...")
        product_txn = [
            {"type": "write", "key": "products/laptop", "value": json.dumps({
                "id": "laptop", "name": "MacBook Pro", "price": 1999.00, "stock": 50
            })},
            {"type": "write", "key": "products/phone", "value": json.dumps({
                "id": "phone", "name": "iPhone 14", "price": 999.00, "stock": 100
            })},
            {"type": "write", "key": "products/tablet", "value": json.dumps({
                "id": "tablet", "name": "iPad Air", "price": 599.00, "stock": 75
            })}
        ]
        
        result = cockroach.execute_transaction(product_txn)
        status = "✅" if result['status'] == 'COMMITTED' else "❌"
        print(f"      {status} Product catalog: {result['status']} (txn: {result['txn_id']})")
        
        # Transaction 3: Cross-range transaction (order processing)
        print("   🛒 Transaction 3: Processing customer order...")
        order_txn = [
            # Read user account
            {"type": "read", "key": "users/alice"},
            # Read product
            {"type": "read", "key": "products/laptop"},
            # Create order
            {"type": "write", "key": "orders/order_001", "value": json.dumps({
                "id": "order_001", "user": "alice", "product": "laptop",
                "quantity": 1, "total": 1999.00, "status": "processing"
            })},
            # Update user balance
            {"type": "write", "key": "users/alice", "value": json.dumps({
                "id": "alice", "name": "Alice Johnson", "email": "alice@example.com", "balance": -999.00
            })},
            # Update product stock
            {"type": "write", "key": "products/laptop", "value": json.dumps({
                "id": "laptop", "name": "MacBook Pro", "price": 1999.00, "stock": 49
            })}
        ]
        
        result = cockroach.execute_transaction(order_txn)
        status = "✅" if result['status'] == 'COMMITTED' else "❌"
        print(f"      {status} Order processing: {result['status']} (txn: {result['txn_id']})")
        if result['status'] == 'COMMITTED':
            print(f"         Cross-range transaction across {len(result['affected_ranges'])} ranges")
        
        # Transaction 4: Analytics query simulation
        print("   📈 Transaction 4: Analytics aggregation...")
        analytics_txn = [
            {"type": "read", "key": "users/alice"},
            {"type": "read", "key": "users/bob"},
            {"type": "read", "key": "users/charlie"},
            {"type": "write", "key": "analytics/daily_stats", "value": json.dumps({
                "date": "2024-01-15", "total_users": 3, "total_balance": 1251.00,
                "generated_at": "2024-01-15T10:30:00Z"
            })}
        ]
        
        result = cockroach.execute_transaction(analytics_txn)
        status = "✅" if result['status'] == 'COMMITTED' else "❌"
        print(f"      {status} Analytics: {result['status']} (txn: {result['txn_id']})")
        
        # Demonstrate read operations with different consistency
        print("\n📖 Reading data with different consistency levels...")
        
        read_tests = [
            ("users/alice", "strong", "Strong consistency read"),
            ("products/laptop", "eventual", "Eventual consistency read"),
            ("orders/order_001", "strong", "Order lookup")
        ]
        
        for key, consistency, description in read_tests:
            value = cockroach.read_data(key, consistency=consistency)
            if value:
                data = json.loads(value) if isinstance(value, str) else value
                print(f"   🔍 {description}: Found {data.get('id', 'data')}")
            else:
                print(f"   ❌ {description}: Not found")
        
        # Simulate node failure
        print("\n💥 Simulating CockroachDB node failure...")
        failure_result = cockroach.simulate_node_failure("node_2")
        
        print(f"   🔥 Failed node: {failure_result['failed_node']}")
        print(f"   📊 Affected ranges: {len(failure_result['affected_ranges'])}")
        print(f"   ⚡ Leader changes: {len(failure_result['leader_changes'])}")
        
        # Test transaction processing after failure
        print("   🔄 Testing post-failure transaction...")
        post_failure_txn = [
            {"type": "write", "key": "system/health_check", "value": json.dumps({
                "timestamp": time.time(), "status": "post_node_failure", "test": True
            })}
        ]
        
        result = cockroach.execute_transaction(post_failure_txn)
        status = "✅" if result['status'] == 'COMMITTED' else "❌"
        print(f"      {status} Post-failure transaction: {result['status']}")
        
        if result['status'] == 'COMMITTED':
            print("   🎉 Database maintains ACID properties despite node failure!")
        
        # Show transaction history
        print("\n📋 Recent transaction history...")
        history = cockroach.get_transaction_history(limit=5)
        
        for i, txn in enumerate(history[:3]):  # Show last 3
            operations_count = len(txn.get('operations', []))
            print(f"   {i+1}. {txn['txn_id']}: {txn['status']} ({operations_count} operations)")
        
        # Final cluster statistics
        final_stats = cockroach.get_cluster_stats()
        print(f"\n📊 Final cluster statistics:")
        print(f"   Total keys: {final_stats['cluster_overview']['total_keys']}")
        print(f"   Operational ranges: {final_stats['cluster_overview']['active_ranges']}")
        print(f"   Health: {final_stats['health']['operational_percentage']:.1f}% operational")
        
        if final_stats['health']['all_ranges_healthy']:
            print("   ✅ All ranges healthy - ACID guarantees maintained")
        
    finally:
        cockroach.stop()


def run_demo():
    """Run the complete real-world applications demo"""
    setup_logging()
    
    print("🌟 Real-World Raft Applications Demo")
    print("=" * 70)
    print("\nThis demo shows how Raft powers production distributed systems:")
    print("- etcd: Kubernetes cluster state management")
    print("- Consul: Service discovery and configuration")
    print("- CockroachDB: Distributed SQL with ACID transactions")
    
    try:
        # etcd demo
        demonstrate_etcd_kubernetes()
        
        input("\n🎯 Press Enter to continue to Consul demo...")
        demonstrate_consul_service_discovery()
        
        input("\n🎯 Press Enter to continue to CockroachDB demo...")
        demonstrate_cockroachdb_distributed_sql()
        
        print_section("Demo Complete")
        print("🎉 All real-world Raft applications demonstrated successfully!")
        print("\nKey insights:")
        print("- etcd enables Kubernetes to manage cluster state consistently")
        print("- Consul provides reliable service discovery for microservices")
        print("- CockroachDB delivers ACID transactions across distributed nodes")
        print("- All systems maintain consistency during failures")
        print("- Raft's consensus ensures these systems remain available and correct")
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    run_demo()
