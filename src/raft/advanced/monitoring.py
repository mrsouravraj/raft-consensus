"""
Raft Monitoring and Observability

Comprehensive monitoring system for Raft clusters including:
- Real-time metrics collection
- Health checks and alerting
- Performance monitoring
- Distributed system observability
- Issue detection and diagnosis

Essential for production Raft deployments to ensure:
- Early detection of consensus failures
- Performance bottleneck identification
- Capacity planning and scaling decisions
- Troubleshooting and debugging
"""

import time
import threading
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from collections import deque, defaultdict
from ..cluster import RaftCluster
from ..messages import NodeState


@dataclass
class MetricSample:
    """Single metric measurement at a point in time"""
    timestamp: float
    value: Any
    labels: Dict[str, str]


@dataclass
class HealthCheck:
    """Health check result"""
    name: str
    healthy: bool
    message: str
    timestamp: float
    severity: str  # INFO, WARNING, ERROR, CRITICAL


@dataclass
class Alert:
    """System alert"""
    id: str
    title: str
    description: str
    severity: str
    timestamp: float
    resolved: bool = False
    resolved_at: Optional[float] = None


class MetricsCollector:
    """
    Collects and stores time-series metrics from Raft clusters.
    
    Tracks key metrics for Raft consensus including:
    - Leadership stability
    - Election frequency
    - Log replication performance
    - Request latency and throughput
    - Resource utilization
    """
    
    def __init__(self, retention_seconds: int = 3600):
        """
        Initialize metrics collector.
        
        Args:
            retention_seconds: How long to retain metrics in memory
        """
        self.retention_seconds = retention_seconds
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.lock = threading.Lock()
        self.logger = logging.getLogger("raft.metrics")
    
    def record_metric(self, name: str, value: Any, labels: Optional[Dict[str, str]] = None) -> None:
        """
        Record a metric sample.
        
        Args:
            name: Metric name
            value: Metric value
            labels: Optional labels for metric dimensions
        """
        sample = MetricSample(
            timestamp=time.time(),
            value=value,
            labels=labels or {}
        )
        
        with self.lock:
            self.metrics[name].append(sample)
        
        self.logger.debug(f"Recorded metric {name}={value}")
    
    def get_metric_samples(self, name: str, duration_seconds: Optional[int] = None) -> List[MetricSample]:
        """
        Get metric samples for a specific metric.
        
        Args:
            name: Metric name
            duration_seconds: Only return samples from last N seconds
            
        Returns:
            list: List of metric samples
        """
        with self.lock:
            if name not in self.metrics:
                return []
            
            samples = list(self.metrics[name])
            
            if duration_seconds:
                cutoff_time = time.time() - duration_seconds
                samples = [s for s in samples if s.timestamp >= cutoff_time]
            
            return samples
    
    def get_metric_value(self, name: str) -> Optional[Any]:
        """Get the most recent value for a metric"""
        with self.lock:
            if name not in self.metrics or not self.metrics[name]:
                return None
            return self.metrics[name][-1].value
    
    def calculate_metric_stats(self, name: str, duration_seconds: int = 300) -> Dict[str, Any]:
        """
        Calculate statistics for a metric over a time period.
        
        Args:
            name: Metric name
            duration_seconds: Time period for calculation
            
        Returns:
            dict: Statistics (min, max, avg, count, etc.)
        """
        samples = self.get_metric_samples(name, duration_seconds)
        
        if not samples:
            return {"error": f"No samples found for metric {name}"}
        
        numeric_values = []
        for sample in samples:
            if isinstance(sample.value, (int, float)):
                numeric_values.append(sample.value)
        
        if not numeric_values:
            return {
                "count": len(samples),
                "latest_value": samples[-1].value,
                "numeric_stats": "Not available (non-numeric values)"
            }
        
        return {
            "count": len(samples),
            "min": min(numeric_values),
            "max": max(numeric_values),
            "avg": sum(numeric_values) / len(numeric_values),
            "latest": numeric_values[-1],
            "first_timestamp": samples[0].timestamp,
            "latest_timestamp": samples[-1].timestamp
        }
    
    def cleanup_old_metrics(self) -> None:
        """Remove old metrics beyond retention period"""
        cutoff_time = time.time() - self.retention_seconds
        
        with self.lock:
            for metric_name, samples in self.metrics.items():
                # Remove old samples
                while samples and samples[0].timestamp < cutoff_time:
                    samples.popleft()


class RaftMonitor:
    """
    Comprehensive Raft cluster monitoring and observability system.
    
    Provides:
    - Real-time health monitoring
    - Performance metrics collection
    - Issue detection and alerting
    - Historical analysis and reporting
    - Troubleshooting diagnostics
    """
    
    def __init__(self, cluster: RaftCluster, collection_interval: float = 1.0):
        """
        Initialize Raft monitor.
        
        Args:
            cluster: Raft cluster to monitor
            collection_interval: Metric collection interval in seconds
        """
        self.cluster = cluster
        self.collection_interval = collection_interval
        self.metrics_collector = MetricsCollector()
        
        # Health and alerting
        self.health_checks: List[HealthCheck] = []
        self.alerts: Dict[str, Alert] = {}
        self.alert_handlers: List[Callable[[Alert], None]] = []
        
        # Monitoring state
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        
        self.logger = logging.getLogger("raft.monitor")
        
        # Start monitoring
        self.start_monitoring()
    
    def start_monitoring(self) -> None:
        """Start continuous monitoring"""
        with self.lock:
            if self.monitoring:
                return
            
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
            self.monitor_thread.start()
        
        self.logger.info(f"Started monitoring cluster with {len(self.cluster.nodes)} nodes")
    
    def stop_monitoring(self) -> None:
        """Stop continuous monitoring"""
        with self.lock:
            self.monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2.0)
        
        self.logger.info("Stopped cluster monitoring")
    
    def _monitoring_loop(self) -> None:
        """Main monitoring loop"""
        while self.monitoring:
            try:
                self._collect_cluster_metrics()
                self._run_health_checks()
                self._cleanup_old_data()
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.collection_interval)
    
    def _collect_cluster_metrics(self) -> None:
        """Collect metrics from the entire cluster"""
        cluster_stats = self.cluster.get_cluster_stats()
        
        # Cluster-level metrics
        self.metrics_collector.record_metric("cluster.total_nodes", cluster_stats.total_nodes)
        self.metrics_collector.record_metric("cluster.active_nodes", cluster_stats.active_nodes)
        self.metrics_collector.record_metric("cluster.current_term", cluster_stats.current_term)
        self.metrics_collector.record_metric("cluster.has_leader", cluster_stats.leader_id is not None)
        
        # Leader stability
        if cluster_stats.leader_id:
            self.metrics_collector.record_metric("cluster.leader_id", cluster_stats.leader_id)
        
        # Node-level metrics
        leaders_count = 0
        followers_count = 0
        candidates_count = 0
        total_log_entries = 0
        total_committed_entries = 0
        total_state_machine_size = 0
        
        for node_id, node_stats in cluster_stats.nodes.items():
            labels = {"node_id": node_id}
            
            # Node state
            self.metrics_collector.record_metric("node.state", node_stats.state.value, labels)
            self.metrics_collector.record_metric("node.term", node_stats.term, labels)
            self.metrics_collector.record_metric("node.log_length", node_stats.log_length, labels)
            self.metrics_collector.record_metric("node.commit_index", node_stats.commit_index, labels)
            self.metrics_collector.record_metric("node.last_applied", node_stats.last_applied, labels)
            self.metrics_collector.record_metric("node.state_machine_size", node_stats.state_machine_size, labels)
            
            # Count states
            if node_stats.state == NodeState.LEADER:
                leaders_count += 1
            elif node_stats.state == NodeState.FOLLOWER:
                followers_count += 1
            elif node_stats.state == NodeState.CANDIDATE:
                candidates_count += 1
            
            # Accumulate totals
            total_log_entries += node_stats.log_length
            total_committed_entries += max(0, node_stats.commit_index + 1)  # commit_index is 0-based
            total_state_machine_size += node_stats.state_machine_size
        
        # State distribution metrics
        self.metrics_collector.record_metric("cluster.leaders_count", leaders_count)
        self.metrics_collector.record_metric("cluster.followers_count", followers_count)
        self.metrics_collector.record_metric("cluster.candidates_count", candidates_count)
        
        # Data metrics
        self.metrics_collector.record_metric("cluster.total_log_entries", total_log_entries)
        self.metrics_collector.record_metric("cluster.total_committed_entries", total_committed_entries)
        self.metrics_collector.record_metric("cluster.total_state_machine_size", total_state_machine_size)
        
        # Consistency metrics
        consistency_stats = self.cluster.get_state_consistency()
        self.metrics_collector.record_metric("cluster.consistency_percentage", consistency_stats["consistency_percentage"])
        self.metrics_collector.record_metric("cluster.consistent_keys", consistency_stats["consistent_keys"])
        self.metrics_collector.record_metric("cluster.total_keys", consistency_stats["total_keys"])
    
    def _run_health_checks(self) -> None:
        """Run all health checks and generate alerts"""
        new_health_checks = []
        
        # Health check: Cluster has leader
        leader_check = self._check_cluster_has_leader()
        new_health_checks.append(leader_check)
        if not leader_check.healthy:
            self._raise_alert("no_leader", "No Leader Elected",
                            "Cluster has no leader - system unavailable",
                            "CRITICAL")
        else:
            self._resolve_alert("no_leader")
        
        # Health check: No split brain
        split_brain_check = self._check_no_split_brain()
        new_health_checks.append(split_brain_check)
        if not split_brain_check.healthy:
            self._raise_alert("split_brain", "Split Brain Detected",
                            "Multiple leaders detected in cluster",
                            "CRITICAL")
        else:
            self._resolve_alert("split_brain")
        
        # Health check: Data consistency
        consistency_check = self._check_data_consistency()
        new_health_checks.append(consistency_check)
        if not consistency_check.healthy:
            self._raise_alert("data_inconsistency", "Data Inconsistency",
                            "Data inconsistency detected across nodes",
                            "WARNING")
        else:
            self._resolve_alert("data_inconsistency")
        
        # Health check: Node failures
        node_failure_check = self._check_node_failures()
        new_health_checks.append(node_failure_check)
        if not node_failure_check.healthy:
            self._raise_alert("node_failures", "Node Failures",
                            node_failure_check.message,
                            "WARNING")
        
        # Health check: Excessive elections
        election_check = self._check_election_frequency()
        new_health_checks.append(election_check)
        if not election_check.healthy:
            self._raise_alert("frequent_elections", "Frequent Elections",
                            "Excessive leader elections detected",
                            "WARNING")
        else:
            self._resolve_alert("frequent_elections")
        
        # Update health checks
        with self.lock:
            self.health_checks = new_health_checks
    
    def _check_cluster_has_leader(self) -> HealthCheck:
        """Check if cluster has an elected leader"""
        leader_id = self.cluster.get_leader()
        
        return HealthCheck(
            name="cluster_has_leader",
            healthy=leader_id is not None,
            message=f"Leader: {leader_id}" if leader_id else "No leader elected",
            timestamp=time.time(),
            severity="CRITICAL" if leader_id is None else "INFO"
        )
    
    def _check_no_split_brain(self) -> HealthCheck:
        """Check for split brain scenario (multiple leaders)"""
        leaders = []
        for node_id, node in self.cluster.nodes.items():
            if node_id not in self.cluster.failed_nodes and node.is_leader():
                leaders.append(node_id)
        
        healthy = len(leaders) <= 1
        
        return HealthCheck(
            name="no_split_brain",
            healthy=healthy,
            message=f"Leaders: {leaders}" if len(leaders) <= 1 else f"Split brain: {leaders}",
            timestamp=time.time(),
            severity="INFO" if healthy else "CRITICAL"
        )
    
    def _check_data_consistency(self) -> HealthCheck:
        """Check data consistency across nodes"""
        consistency_stats = self.cluster.get_state_consistency()
        consistency_percentage = consistency_stats["consistency_percentage"]
        
        healthy = consistency_percentage >= 95.0  # 95% threshold
        
        return HealthCheck(
            name="data_consistency",
            healthy=healthy,
            message=f"Consistency: {consistency_percentage:.1f}%",
            timestamp=time.time(),
            severity="INFO" if healthy else "WARNING"
        )
    
    def _check_node_failures(self) -> HealthCheck:
        """Check for node failures"""
        failed_nodes = len(self.cluster.failed_nodes)
        total_nodes = len(self.cluster.nodes)
        active_nodes = total_nodes - failed_nodes
        
        # Healthy if majority of nodes are active
        healthy = active_nodes >= (total_nodes // 2 + 1)
        
        return HealthCheck(
            name="node_failures",
            healthy=healthy,
            message=f"Active: {active_nodes}/{total_nodes} nodes",
            timestamp=time.time(),
            severity="INFO" if healthy else "WARNING"
        )
    
    def _check_election_frequency(self) -> HealthCheck:
        """Check for excessive leader elections"""
        # Count term changes in last 5 minutes
        term_samples = self.metrics_collector.get_metric_samples("cluster.current_term", 300)
        
        if len(term_samples) < 2:
            return HealthCheck(
                name="election_frequency",
                healthy=True,
                message="Insufficient data for election frequency check",
                timestamp=time.time(),
                severity="INFO"
            )
        
        # Count term increases (elections)
        elections = 0
        prev_term = term_samples[0].value
        
        for sample in term_samples[1:]:
            if sample.value > prev_term:
                elections += 1
                prev_term = sample.value
        
        # More than 5 elections in 5 minutes is excessive
        healthy = elections <= 5
        
        return HealthCheck(
            name="election_frequency",
            healthy=healthy,
            message=f"{elections} elections in last 5 minutes",
            timestamp=time.time(),
            severity="INFO" if healthy else "WARNING"
        )
    
    def _raise_alert(self, alert_id: str, title: str, description: str, severity: str) -> None:
        """Raise a new alert or update existing one"""
        with self.lock:
            if alert_id in self.alerts and not self.alerts[alert_id].resolved:
                return  # Alert already active
            
            alert = Alert(
                id=alert_id,
                title=title,
                description=description,
                severity=severity,
                timestamp=time.time()
            )
            
            self.alerts[alert_id] = alert
            
            # Notify alert handlers
            for handler in self.alert_handlers:
                try:
                    handler(alert)
                except Exception as e:
                    self.logger.error(f"Error in alert handler: {e}")
            
            self.logger.warning(f"ALERT [{severity}]: {title} - {description}")
    
    def _resolve_alert(self, alert_id: str) -> None:
        """Resolve an active alert"""
        with self.lock:
            if alert_id in self.alerts and not self.alerts[alert_id].resolved:
                self.alerts[alert_id].resolved = True
                self.alerts[alert_id].resolved_at = time.time()
                
                self.logger.info(f"RESOLVED: Alert {alert_id}")
    
    def _cleanup_old_data(self) -> None:
        """Clean up old metrics and health data"""
        self.metrics_collector.cleanup_old_metrics()
        
        # Clean up old resolved alerts (older than 1 hour)
        cutoff_time = time.time() - 3600
        with self.lock:
            alerts_to_remove = [
                alert_id for alert_id, alert in self.alerts.items()
                if alert.resolved and alert.resolved_at and alert.resolved_at < cutoff_time
            ]
            
            for alert_id in alerts_to_remove:
                del self.alerts[alert_id]
    
    def add_alert_handler(self, handler: Callable[[Alert], None]) -> None:
        """Add alert handler function"""
        self.alert_handlers.append(handler)
    
    def get_cluster_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive cluster health summary"""
        with self.lock:
            health_checks = self.health_checks.copy()
            active_alerts = [alert for alert in self.alerts.values() if not alert.resolved]
        
        # Calculate overall health
        critical_issues = len([hc for hc in health_checks if not hc.healthy and hc.severity == "CRITICAL"])
        warning_issues = len([hc for hc in health_checks if not hc.healthy and hc.severity == "WARNING"])
        
        if critical_issues > 0:
            overall_health = "CRITICAL"
        elif warning_issues > 0:
            overall_health = "WARNING"
        else:
            overall_health = "HEALTHY"
        
        return {
            "overall_health": overall_health,
            "timestamp": time.time(),
            "health_checks": [asdict(hc) for hc in health_checks],
            "active_alerts": [asdict(alert) for alert in active_alerts],
            "summary": {
                "total_checks": len(health_checks),
                "passed_checks": len([hc for hc in health_checks if hc.healthy]),
                "failed_checks": len([hc for hc in health_checks if not hc.healthy]),
                "critical_issues": critical_issues,
                "warning_issues": warning_issues,
                "active_alerts": len(active_alerts)
            }
        }
    
    def get_performance_dashboard(self) -> Dict[str, Any]:
        """Get performance metrics dashboard"""
        dashboard = {
            "timestamp": time.time(),
            "cluster_overview": {},
            "node_metrics": {},
            "performance_metrics": {},
            "trends": {}
        }
        
        # Cluster overview
        dashboard["cluster_overview"] = {
            "total_nodes": self.metrics_collector.get_metric_value("cluster.total_nodes"),
            "active_nodes": self.metrics_collector.get_metric_value("cluster.active_nodes"),
            "current_leader": self.metrics_collector.get_metric_value("cluster.leader_id"),
            "current_term": self.metrics_collector.get_metric_value("cluster.current_term"),
            "consistency_percentage": self.metrics_collector.get_metric_value("cluster.consistency_percentage")
        }
        
        # Performance metrics (last 5 minutes)
        metrics_to_analyze = [
            "cluster.total_log_entries",
            "cluster.total_state_machine_size",
            "cluster.leaders_count",
            "cluster.followers_count"
        ]
        
        for metric_name in metrics_to_analyze:
            stats = self.metrics_collector.calculate_metric_stats(metric_name, 300)
            dashboard["performance_metrics"][metric_name.replace("cluster.", "")] = stats
        
        return dashboard
    
    def diagnose_issues(self) -> Dict[str, Any]:
        """Diagnose potential issues in the cluster"""
        issues = []
        recommendations = []
        
        # Check for leader stability
        leader_samples = self.metrics_collector.get_metric_samples("cluster.leader_id", 300)
        if len(leader_samples) > 1:
            leader_changes = 0
            prev_leader = leader_samples[0].value
            
            for sample in leader_samples[1:]:
                if sample.value != prev_leader:
                    leader_changes += 1
                    prev_leader = sample.value
            
            if leader_changes > 3:
                issues.append(f"Frequent leader changes: {leader_changes} in last 5 minutes")
                recommendations.append("Check network stability and node health")
        
        # Check for log replication lag
        cluster_stats = self.cluster.get_cluster_stats()
        if cluster_stats.leader_id:
            leader_stats = cluster_stats.nodes[cluster_stats.leader_id]
            max_lag = 0
            
            for node_id, node_stats in cluster_stats.nodes.items():
                if node_id != cluster_stats.leader_id:
                    lag = leader_stats.commit_index - node_stats.commit_index
                    max_lag = max(max_lag, lag)
            
            if max_lag > 50:
                issues.append(f"High replication lag: {max_lag} entries")
                recommendations.append("Check follower node performance and network latency")
        
        # Check consistency
        consistency_percentage = self.metrics_collector.get_metric_value("cluster.consistency_percentage")
        if consistency_percentage and consistency_percentage < 100:
            issues.append(f"Data inconsistency: {consistency_percentage:.1f}% consistent")
            recommendations.append("Wait for cluster to converge or investigate network partitions")
        
        return {
            "timestamp": time.time(),
            "issues_found": len(issues),
            "issues": issues,
            "recommendations": recommendations,
            "overall_diagnosis": "HEALTHY" if not issues else "ISSUES_DETECTED"
        }
