"""
Consul Simulator - Service Discovery and Configuration

Educational simulation of HashiCorp Consul using Raft for distributed service discovery
and configuration management.

Consul uses Raft consensus to maintain consistent views of:
- Service registry (what services are running where)
- Health check status
- Key-value configuration store  
- Access control policies
- Network topology

This simulator demonstrates how Consul enables service mesh architectures
and configuration management across distributed systems.
"""

import json
import time
import random
import logging
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass
from ..cluster import RaftCluster


@dataclass
class ServiceInstance:
    """Represents a registered service instance"""
    service_id: str
    service_name: str
    node_id: str
    address: str
    port: int
    tags: List[str]
    meta: Dict[str, str]
    health_status: str  # passing, warning, critical
    registered_at: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "ID": self.service_id,
            "Service": self.service_name,
            "Node": self.node_id,
            "Address": self.address,
            "Port": self.port,
            "Tags": self.tags,
            "Meta": self.meta,
            "Status": self.health_status,
            "RegisteredAt": self.registered_at
        }


@dataclass
class HealthCheck:
    """Represents a health check for a service"""
    check_id: str
    service_id: str
    name: str
    status: str  # passing, warning, critical
    output: str
    interval: str
    last_check: float


class ConsulSimulator:
    """
    Educational simulation of Consul using Raft for service discovery.
    
    Demonstrates how Consul maintains distributed service registry and
    configuration using Raft consensus, enabling:
    - Service registration and discovery
    - Health monitoring
    - Key-value configuration store
    - Cross-datacenter replication
    - Network partition handling
    """
    
    def __init__(self, num_servers: int = 3, datacenter: str = "dc1"):
        """
        Initialize Consul cluster.
        
        Args:
            num_servers: Number of Consul server nodes
            datacenter: Datacenter name for this cluster
        """
        if num_servers < 3:
            raise ValueError("Consul cluster needs at least 3 servers for HA")
        
        self.cluster = RaftCluster(num_nodes=num_servers)
        self.datacenter = datacenter
        self.node_counter = 1000
        
        self.logger = logging.getLogger(f"consul.{datacenter}")
        
        # Wait for cluster to be ready
        if not self.cluster.wait_for_convergence(timeout=5.0):
            raise RuntimeError("Consul cluster failed to initialize")
        
        leader = self.cluster.get_leader()
        self.logger.info(f"Consul cluster initialized in {datacenter} with {num_servers} servers, leader: {leader}")
        
        # Initialize datacenter info
        self._store_datacenter_info()
    
    def _store_datacenter_info(self) -> None:
        """Store datacenter metadata"""
        dc_info = {
            "datacenter": self.datacenter,
            "servers": len(self.cluster.nodes),
            "created_at": time.time(),
            "version": "1.15.0-educational"
        }
        
        self.cluster.submit_request("SET", {
            "key": f"consul/datacenter/{self.datacenter}/info",
            "value": json.dumps(dc_info)
        })
    
    def register_service(self, service_name: str, node_id: str, address: str, 
                        port: int, service_id: Optional[str] = None,
                        tags: Optional[List[str]] = None,
                        meta: Optional[Dict[str, str]] = None) -> bool:
        """
        Register a service instance with Consul.
        
        Args:
            service_name: Name of the service (e.g., "web", "database")
            node_id: ID of the node hosting the service
            address: IP address of the service
            port: Port number of the service
            service_id: Unique ID for this instance (auto-generated if None)
            tags: List of tags for service filtering
            meta: Additional metadata
            
        Returns:
            bool: True if successfully registered
        """
        if service_id is None:
            service_id = f"{service_name}-{node_id}-{port}"
        
        service_instance = ServiceInstance(
            service_id=service_id,
            service_name=service_name,
            node_id=node_id,
            address=address,
            port=port,
            tags=tags or [],
            meta=meta or {},
            health_status="passing",  # Start as healthy
            registered_at=time.time()
        )
        
        # Store service instance
        instance_key = f"consul/services/{service_name}/instances/{service_id}"
        success = self.cluster.submit_request("SET", {
            "key": instance_key,
            "value": json.dumps(service_instance.to_dict())
        })
        
        if success:
            # Update service catalog
            self._update_service_catalog(service_name)
            
            # Register default health check
            self._register_default_health_check(service_instance)
            
            self.logger.info(f"Registered service: {service_name} ({service_id}) at {address}:{port}")
        
        return success
    
    def _update_service_catalog(self, service_name: str) -> None:
        """Update service catalog with current instances"""
        instances = self._get_service_instances(service_name)
        
        catalog_entry = {
            "service": service_name,
            "instance_count": len(instances),
            "tags": list(set(tag for instance in instances for tag in instance.get("Tags", []))),
            "datacenters": [self.datacenter],
            "last_updated": time.time()
        }
        
        catalog_key = f"consul/catalog/services/{service_name}"
        self.cluster.submit_request("SET", {
            "key": catalog_key,
            "value": json.dumps(catalog_entry)
        })
    
    def _register_default_health_check(self, service_instance: ServiceInstance) -> None:
        """Register default health check for service instance"""
        check = HealthCheck(
            check_id=f"service:{service_instance.service_id}",
            service_id=service_instance.service_id,
            name=f"Service '{service_instance.service_name}' check",
            status="passing",
            output="Service registered and assumed healthy",
            interval="10s",
            last_check=time.time()
        )
        
        check_key = f"consul/health/checks/{check.check_id}"
        self.cluster.submit_request("SET", {
            "key": check_key,
            "value": json.dumps({
                "CheckID": check.check_id,
                "ServiceID": check.service_id,
                "Name": check.name,
                "Status": check.status,
                "Output": check.output,
                "Interval": check.interval,
                "LastCheck": check.last_check
            })
        })
    
    def deregister_service(self, service_id: str) -> bool:
        """
        Deregister a service instance.
        
        Args:
            service_id: Unique ID of service instance to remove
            
        Returns:
            bool: True if successfully deregistered
        """
        # Find the service instance
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return False
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        service_name = None
        instance_key = None
        
        # Find service instance in state machine
        for key, value in state_machine.items():
            if key.startswith("consul/services/") and key.endswith(f"/instances/{service_id}"):
                try:
                    instance_data = json.loads(value)
                    if instance_data.get("ID") == service_id:
                        service_name = instance_data.get("Service")
                        instance_key = key
                        break
                except json.JSONDecodeError:
                    continue
        
        if not instance_key:
            self.logger.warning(f"Service instance {service_id} not found")
            return False
        
        # Remove service instance
        success = self.cluster.submit_request("DELETE", {"key": instance_key})
        
        if success:
            # Remove associated health checks
            check_key = f"consul/health/checks/service:{service_id}"
            self.cluster.submit_request("DELETE", {"key": check_key})
            
            # Update service catalog
            if service_name:
                self._update_service_catalog(service_name)
            
            self.logger.info(f"Deregistered service instance: {service_id}")
        
        return success
    
    def discover_service(self, service_name: str, healthy_only: bool = True,
                        tag_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Discover instances of a service.
        
        Args:
            service_name: Name of service to discover
            healthy_only: Only return healthy instances
            tag_filter: Filter by specific tag
            
        Returns:
            list: List of service instances
        """
        instances = self._get_service_instances(service_name)
        
        # Filter by health status
        if healthy_only:
            healthy_instances = []
            for instance in instances:
                if self._is_service_healthy(instance.get("ID")):
                    healthy_instances.append(instance)
            instances = healthy_instances
        
        # Filter by tag
        if tag_filter:
            instances = [
                instance for instance in instances 
                if tag_filter in instance.get("Tags", [])
            ]
        
        self.logger.debug(f"Discovered {len(instances)} instances of {service_name}")
        return instances
    
    def _get_service_instances(self, service_name: str) -> List[Dict[str, Any]]:
        """Get all instances of a service"""
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return []
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        instances = []
        prefix = f"consul/services/{service_name}/instances/"
        
        for key, value in state_machine.items():
            if key.startswith(prefix):
                try:
                    instance_data = json.loads(value)
                    instances.append(instance_data)
                except json.JSONDecodeError:
                    continue
        
        return instances
    
    def _is_service_healthy(self, service_id: str) -> bool:
        """Check if service instance is healthy"""
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return False
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        check_key = f"consul/health/checks/service:{service_id}"
        if check_key in state_machine:
            try:
                check_data = json.loads(state_machine[check_key])
                return check_data.get("Status") == "passing"
            except json.JSONDecodeError:
                pass
        
        return True  # Assume healthy if no check found
    
    def store_kv_config(self, key: str, value: str, flags: int = 0) -> bool:
        """
        Store configuration in Consul KV store.
        
        Args:
            key: Configuration key
            value: Configuration value
            flags: Optional flags for the key
            
        Returns:
            bool: True if successfully stored
        """
        kv_entry = {
            "key": key,
            "value": value,
            "flags": flags,
            "create_index": self.node_counter,
            "modify_index": self.node_counter,
            "lock_index": 0,
            "session": None
        }
        
        self.node_counter += 1
        
        kv_key = f"consul/kv/{key}"
        success = self.cluster.submit_request("SET", {
            "key": kv_key,
            "value": json.dumps(kv_entry)
        })
        
        if success:
            self.logger.info(f"Stored KV config: {key} = {value}")
        
        return success
    
    def get_kv_config(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration from Consul KV store.
        
        Args:
            key: Configuration key
            
        Returns:
            dict: KV entry or None if not found
        """
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return None
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        kv_key = f"consul/kv/{key}"
        if kv_key in state_machine:
            try:
                return json.loads(state_machine[kv_key])
            except json.JSONDecodeError:
                pass
        
        return None
    
    def delete_kv_config(self, key: str, recurse: bool = False) -> bool:
        """
        Delete configuration from Consul KV store.
        
        Args:
            key: Configuration key to delete
            recurse: Delete all keys with this prefix
            
        Returns:
            bool: True if successfully deleted
        """
        if recurse:
            # Delete all keys with prefix
            leader_id = self.cluster.get_leader()
            if not leader_id:
                return False
            
            leader = self.cluster.nodes[leader_id]
            state_machine = leader.get_state_machine()
            
            kv_prefix = f"consul/kv/{key}"
            keys_to_delete = [k for k in state_machine.keys() if k.startswith(kv_prefix)]
            
            success_count = 0
            for kv_key in keys_to_delete:
                if self.cluster.submit_request("DELETE", {"key": kv_key}):
                    success_count += 1
            
            self.logger.info(f"Deleted {success_count}/{len(keys_to_delete)} KV entries with prefix {key}")
            return success_count == len(keys_to_delete)
        else:
            # Delete single key
            kv_key = f"consul/kv/{key}"
            success = self.cluster.submit_request("DELETE", {"key": kv_key})
            
            if success:
                self.logger.info(f"Deleted KV config: {key}")
            
            return success
    
    def list_services(self) -> Dict[str, Dict[str, Any]]:
        """
        List all registered services.
        
        Returns:
            dict: Service name -> service info mapping
        """
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return {}
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        services = {}
        catalog_prefix = "consul/catalog/services/"
        
        for key, value in state_machine.items():
            if key.startswith(catalog_prefix):
                service_name = key[len(catalog_prefix):]
                try:
                    service_info = json.loads(value)
                    services[service_name] = service_info
                except json.JSONDecodeError:
                    continue
        
        return services
    
    def simulate_health_check_failure(self, service_id: str, status: str = "critical",
                                    output: str = "Health check failed") -> bool:
        """
        Simulate health check failure for testing.
        
        Args:
            service_id: Service instance ID
            status: New health status (passing, warning, critical)
            output: Health check output message
            
        Returns:
            bool: True if health check was updated
        """
        check_key = f"consul/health/checks/service:{service_id}"
        
        # Get existing check
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return False
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        if check_key not in state_machine:
            return False
        
        try:
            check_data = json.loads(state_machine[check_key])
            check_data["Status"] = status
            check_data["Output"] = output
            check_data["LastCheck"] = time.time()
            
            success = self.cluster.submit_request("SET", {
                "key": check_key,
                "value": json.dumps(check_data)
            })
            
            if success:
                self.logger.info(f"Updated health check for {service_id}: {status}")
            
            return success
            
        except json.JSONDecodeError:
            return False
    
    def simulate_datacenter_split(self, minority_servers: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Simulate datacenter network partition.
        
        Args:
            minority_servers: List of server IDs to isolate (auto-selected if None)
            
        Returns:
            dict: Partition simulation results
        """
        all_nodes = list(self.cluster.nodes.keys())
        
        if minority_servers is None:
            # Isolate minority of servers
            minority_size = len(all_nodes) // 2
            minority_servers = random.sample(all_nodes, minority_size)
        
        majority_servers = [n for n in all_nodes if n not in minority_servers]
        
        self.logger.info(f"Simulating datacenter split - isolating: {minority_servers}")
        
        # Create network partition
        success = self.cluster.simulate_network_partition(majority_servers, minority_servers)
        
        time.sleep(2.0)  # Allow cluster to adapt
        
        # Check which partition has leader
        leader_id = self.cluster.get_leader()
        
        result = {
            "partition_created": success,
            "minority_servers": minority_servers,
            "majority_servers": majority_servers,
            "active_leader": leader_id,
            "majority_operational": leader_id in majority_servers if leader_id else False
        }
        
        if leader_id and leader_id in majority_servers:
            self.logger.info(f"Majority partition operational with leader: {leader_id}")
            self.logger.info("Service discovery remains available!")
        else:
            self.logger.warning("No leader in majority partition - service discovery unavailable")
        
        return result
    
    def heal_datacenter_split(self) -> bool:
        """Heal datacenter network partition"""
        self.cluster.heal_network_partition()
        
        # Wait for cluster to reconverge
        if self.cluster.wait_for_convergence(timeout=5.0):
            leader = self.cluster.get_leader()
            self.logger.info(f"Datacenter partition healed, leader: {leader}")
            return True
        else:
            self.logger.error("Failed to heal datacenter partition")
            return False
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get comprehensive cluster information"""
        stats = self.cluster.get_cluster_stats()
        consistency = self.cluster.get_state_consistency()
        services = self.list_services()
        
        # Count KV entries
        kv_count = 0
        if stats.leader_id:
            leader = self.cluster.nodes[stats.leader_id]
            state_machine = leader.get_state_machine()
            kv_count = len([k for k in state_machine.keys() if k.startswith("consul/kv/")])
        
        return {
            "datacenter": self.datacenter,
            "consul_cluster": {
                "total_servers": stats.total_nodes,
                "active_servers": stats.active_nodes,
                "leader": stats.leader_id,
                "term": stats.current_term,
                "healthy": stats.leader_id is not None
            },
            "services": {
                "registered_services": len(services),
                "total_instances": sum(s.get("instance_count", 0) for s in services.values()),
                "service_names": list(services.keys())
            },
            "kv_store": {
                "total_keys": kv_count
            },
            "consistency": {
                "percentage": consistency["consistency_percentage"],
                "healthy": consistency["consistency_percentage"] == 100
            }
        }
    
    def stop(self) -> None:
        """Stop the Consul cluster"""
        self.cluster.stop()
        self.logger.info(f"Consul cluster in {self.datacenter} stopped")
