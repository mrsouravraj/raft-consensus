"""
etcd Simulator - Kubernetes Control Plane Storage

Educational simulation of etcd using Raft for Kubernetes cluster state management.

etcd is the distributed key-value store that Kubernetes uses to store all cluster data.
It uses Raft consensus to ensure that all master nodes have a consistent view of:
- Pod specifications and status
- Service definitions  
- ConfigMaps and Secrets
- Node information
- Custom resources

This simulator demonstrates how etcd ensures Kubernetes cluster consistency
even when master nodes fail or become unreachable.
"""

import json
import time
import logging
from typing import Dict, List, Any, Optional
from ..cluster import RaftCluster


class EtcdSimulator:
    """
    Educational simulation of etcd using Raft for Kubernetes.
    
    Demonstrates how etcd stores Kubernetes resources with hierarchical keys
    and maintains consistency across master nodes using Raft consensus.
    
    Key features:
    - Hierarchical key storage (/registry/resource_type/namespace/name)
    - Resource versioning for optimistic concurrency control
    - Watch functionality for controllers
    - Master node failure handling
    """
    
    def __init__(self, num_masters: int = 3):
        """
        Initialize etcd cluster for Kubernetes.
        
        Args:
            num_masters: Number of master nodes (etcd instances)
        """
        if num_masters < 3:
            raise ValueError("Kubernetes cluster needs at least 3 masters for HA")
        
        self.cluster = RaftCluster(num_nodes=num_masters)
        self.api_version_counter = 1000  # Start with higher number like real K8s
        self.watchers = {}  # Resource watchers (simplified)
        
        self.logger = logging.getLogger("etcd.simulator")
        
        # Wait for cluster to be ready
        if not self.cluster.wait_for_convergence(timeout=5.0):
            raise RuntimeError("etcd cluster failed to initialize")
        
        leader = self.cluster.get_leader()
        self.logger.info(f"etcd cluster initialized with {num_masters} masters, leader: {leader}")
    
    def store_kubernetes_resource(self, resource_type: str, namespace: str, 
                                 name: str, spec: Dict[str, Any], 
                                 labels: Optional[Dict[str, str]] = None) -> bool:
        """
        Store Kubernetes resource in etcd.
        
        Args:
            resource_type: Type of K8s resource (Pod, Service, Deployment, etc.)
            namespace: Kubernetes namespace (empty string for cluster-scoped)
            name: Resource name
            spec: Resource specification
            labels: Resource labels
            
        Returns:
            bool: True if successfully stored
        """
        # Build etcd key following Kubernetes conventions
        if namespace:
            key = f"/registry/{resource_type.lower()}s/{namespace}/{name}"
        else:
            key = f"/registry/{resource_type.lower()}s/{name}"
        
        # Create Kubernetes-style resource object
        resource = {
            "apiVersion": self._get_api_version(resource_type),
            "kind": resource_type,
            "metadata": {
                "name": name,
                "namespace": namespace if namespace else None,
                "resourceVersion": str(self.api_version_counter),
                "uid": f"uid-{self.api_version_counter}",
                "creationTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "labels": labels or {}
            },
            "spec": spec,
            "status": {}  # Would be populated by controllers
        }
        
        # Remove None values
        resource["metadata"] = {k: v for k, v in resource["metadata"].items() if v is not None}
        
        success = self.cluster.submit_request("SET", {
            "key": key,
            "value": json.dumps(resource, indent=2)
        })
        
        if success:
            self.api_version_counter += 1
            self.logger.info(f"Stored K8s {resource_type}: {namespace}/{name} (rv: {resource['metadata']['resourceVersion']})")
            self._notify_watchers(resource_type, "ADDED", resource)
        else:
            self.logger.error(f"Failed to store K8s {resource_type}: {namespace}/{name}")
        
        return success
    
    def get_kubernetes_resource(self, resource_type: str, namespace: str, name: str) -> Optional[Dict[str, Any]]:
        """
        Get Kubernetes resource from etcd.
        
        Args:
            resource_type: Type of K8s resource
            namespace: Kubernetes namespace
            name: Resource name
            
        Returns:
            dict: Resource object or None if not found
        """
        if namespace:
            key = f"/registry/{resource_type.lower()}s/{namespace}/{name}"
        else:
            key = f"/registry/{resource_type.lower()}s/{name}"
        
        leader_id = self.cluster.get_leader()
        if not leader_id:
            self.logger.error("No etcd leader available")
            return None
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        if key in state_machine:
            try:
                return json.loads(state_machine[key])
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to decode resource {key}: {e}")
        
        return None
    
    def list_kubernetes_resources(self, resource_type: str, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List Kubernetes resources of a specific type.
        
        Args:
            resource_type: Type of K8s resource
            namespace: Filter by namespace (None for all namespaces)
            
        Returns:
            list: List of resource objects
        """
        leader_id = self.cluster.get_leader()
        if not leader_id:
            return []
        
        leader = self.cluster.nodes[leader_id]
        state_machine = leader.get_state_machine()
        
        resources = []
        prefix = f"/registry/{resource_type.lower()}s/"
        
        for key, value in state_machine.items():
            if key.startswith(prefix):
                # Parse namespace from key
                key_parts = key[len(prefix):].split('/')
                if namespace is not None:
                    # For namespaced resources: /registry/pods/namespace/name
                    if len(key_parts) >= 2 and key_parts[0] != namespace:
                        continue
                
                try:
                    resource = json.loads(value)
                    resources.append(resource)
                except json.JSONDecodeError:
                    continue
        
        return resources
    
    def delete_kubernetes_resource(self, resource_type: str, namespace: str, name: str) -> bool:
        """
        Delete Kubernetes resource from etcd.
        
        Args:
            resource_type: Type of K8s resource
            namespace: Kubernetes namespace
            name: Resource name
            
        Returns:
            bool: True if successfully deleted
        """
        # First get the resource for watchers
        resource = self.get_kubernetes_resource(resource_type, namespace, name)
        
        if namespace:
            key = f"/registry/{resource_type.lower()}s/{namespace}/{name}"
        else:
            key = f"/registry/{resource_type.lower()}s/{name}"
        
        success = self.cluster.submit_request("DELETE", {"key": key})
        
        if success and resource:
            self.logger.info(f"Deleted K8s {resource_type}: {namespace}/{name}")
            self._notify_watchers(resource_type, "DELETED", resource)
        
        return success
    
    def update_resource_status(self, resource_type: str, namespace: str, name: str, 
                              status: Dict[str, Any]) -> bool:
        """
        Update resource status (simulates controller updates).
        
        Args:
            resource_type: Type of K8s resource
            namespace: Kubernetes namespace
            name: Resource name
            status: New status object
            
        Returns:
            bool: True if successfully updated
        """
        resource = self.get_kubernetes_resource(resource_type, namespace, name)
        if not resource:
            return False
        
        # Update status and resource version
        resource["status"] = status
        resource["metadata"]["resourceVersion"] = str(self.api_version_counter)
        self.api_version_counter += 1
        
        # Store updated resource
        if namespace:
            key = f"/registry/{resource_type.lower()}s/{namespace}/{name}"
        else:
            key = f"/registry/{resource_type.lower()}s/{name}"
        
        success = self.cluster.submit_request("SET", {
            "key": key,
            "value": json.dumps(resource, indent=2)
        })
        
        if success:
            self.logger.debug(f"Updated status for {resource_type}: {namespace}/{name}")
            self._notify_watchers(resource_type, "MODIFIED", resource)
        
        return success
    
    def get_cluster_nodes(self) -> List[Dict[str, Any]]:
        """Get list of Kubernetes nodes from etcd."""
        nodes = self.list_kubernetes_resources("Node")
        return [{"name": node["metadata"]["name"], 
                "status": node.get("status", {})} for node in nodes]
    
    def get_cluster_health(self) -> Dict[str, Any]:
        """Get Kubernetes cluster health from etcd perspective."""
        stats = self.cluster.get_cluster_stats()
        consistency = self.cluster.get_state_consistency()
        
        # Count different resource types
        resource_counts = {}
        if stats.leader_id:
            leader = self.cluster.nodes[stats.leader_id]
            state_machine = leader.get_state_machine()
            
            for key in state_machine.keys():
                if key.startswith("/registry/"):
                    resource_type = key.split("/")[2]  # Extract resource type
                    resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1
        
        return {
            "etcd_cluster": {
                "total_members": stats.total_nodes,
                "active_members": stats.active_nodes,
                "leader": stats.leader_id,
                "term": stats.current_term
            },
            "kubernetes_data": {
                "resource_counts": resource_counts,
                "total_resources": sum(resource_counts.values()),
                "consistency_check": {
                    "consistent": consistency["consistency_percentage"] == 100,
                    "percentage": consistency["consistency_percentage"]
                }
            },
            "healthy": stats.leader_id is not None and consistency["consistency_percentage"] == 100
        }
    
    def simulate_master_failure(self, master_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Simulate Kubernetes master node failure.
        
        Args:
            master_id: Specific master to fail (None for current leader)
            
        Returns:
            dict: Failure simulation results
        """
        target_node = master_id or self.cluster.get_leader()
        
        if not target_node:
            return {"error": "No target node to fail"}
        
        original_leader = self.cluster.get_leader()
        
        self.logger.info(f"Simulating failure of master node: {target_node}")
        success = self.cluster.simulate_node_failure(target_node)
        
        if not success:
            return {"error": f"Failed to simulate failure of {target_node}"}
        
        # Wait for new leader election
        time.sleep(2.0)
        
        new_leader = self.cluster.get_leader()
        stats = self.cluster.get_cluster_stats()
        
        result = {
            "failed_node": target_node,
            "was_leader": target_node == original_leader,
            "new_leader": new_leader,
            "election_successful": new_leader is not None,
            "active_masters": stats.active_nodes,
            "cluster_available": new_leader is not None
        }
        
        if new_leader:
            self.logger.info(f"New etcd leader elected: {new_leader}")
            self.logger.info("Kubernetes API server remains available!")
        else:
            self.logger.error("No new etcd leader - Kubernetes API unavailable")
        
        return result
    
    def _get_api_version(self, resource_type: str) -> str:
        """Get appropriate API version for resource type."""
        api_versions = {
            "Pod": "v1",
            "Service": "v1",
            "ConfigMap": "v1",
            "Secret": "v1",
            "Node": "v1",
            "Namespace": "v1",
            "Deployment": "apps/v1",
            "ReplicaSet": "apps/v1",
            "DaemonSet": "apps/v1",
            "StatefulSet": "apps/v1",
            "Ingress": "networking.k8s.io/v1",
            "PersistentVolume": "v1",
            "PersistentVolumeClaim": "v1"
        }
        return api_versions.get(resource_type, "v1")
    
    def _notify_watchers(self, resource_type: str, event_type: str, resource: Dict[str, Any]) -> None:
        """Notify resource watchers of changes (simplified)."""
        # In real etcd, this would be much more complex with proper watch streams
        watch_key = f"{resource_type}/{resource['metadata'].get('namespace', '')}"
        if watch_key in self.watchers:
            self.logger.debug(f"Watch event: {event_type} {resource_type}/{resource['metadata']['name']}")
    
    def watch_resources(self, resource_type: str, namespace: str = "") -> str:
        """
        Start watching resources (simplified implementation).
        
        Args:
            resource_type: Type of K8s resource to watch
            namespace: Namespace to watch (empty for all)
            
        Returns:
            str: Watch ID
        """
        watch_key = f"{resource_type}/{namespace}"
        watch_id = f"watch-{len(self.watchers)}"
        self.watchers[watch_key] = watch_id
        
        self.logger.info(f"Started watching {resource_type} in namespace '{namespace}' (ID: {watch_id})")
        return watch_id
    
    def stop_watch(self, watch_id: str) -> bool:
        """Stop watching resources."""
        for watch_key, wid in list(self.watchers.items()):
            if wid == watch_id:
                del self.watchers[watch_key]
                self.logger.info(f"Stopped watch {watch_id}")
                return True
        return False
    
    def stop(self) -> None:
        """Stop the etcd cluster."""
        self.cluster.stop()
        self.logger.info("etcd cluster stopped")
