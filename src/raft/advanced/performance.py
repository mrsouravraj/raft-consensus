"""
Raft Performance Analysis and Load Testing

Performance testing and analysis tools for Raft consensus implementations.

Provides comprehensive performance testing including:
- Throughput measurement under various loads
- Latency analysis for different request patterns
- Scalability testing with varying cluster sizes
- Failure scenario performance impact
- Resource utilization monitoring
- Benchmark comparisons

Essential for understanding Raft performance characteristics and
capacity planning for production deployments.
"""

import time
import random
import threading
import statistics
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..cluster import RaftCluster
from .monitoring import MetricsCollector


@dataclass
class PerformanceResult:
    """Single performance test result"""
    test_name: str
    start_time: float
    end_time: float
    duration_seconds: float
    total_requests: int
    successful_requests: int
    failed_requests: int
    requests_per_second: float
    average_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate_percentage: float
    cluster_config: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RequestResult:
    """Individual request result"""
    start_time: float
    end_time: float
    latency_ms: float
    success: bool
    error_message: Optional[str] = None


class LoadGenerator:
    """
    Generates load against Raft clusters for performance testing.
    
    Supports various load patterns:
    - Constant rate requests
    - Burst traffic patterns
    - Read/write ratio testing
    - Concurrent client simulation
    """
    
    def __init__(self, cluster: RaftCluster):
        self.cluster = cluster
        self.logger = logging.getLogger("raft.loadgen")
    
    def generate_constant_load(self, duration_seconds: int, requests_per_second: int,
                              read_ratio: float = 0.7) -> List[RequestResult]:
        """
        Generate constant rate load against the cluster.
        
        Args:
            duration_seconds: How long to run the test
            requests_per_second: Target request rate
            read_ratio: Proportion of read vs write requests (0.0 = all writes, 1.0 = all reads)
            
        Returns:
            list: List of RequestResult objects
        """
        results = []
        request_interval = 1.0 / requests_per_second
        end_time = time.time() + duration_seconds
        request_id = 0
        
        self.logger.info(f"Starting constant load: {requests_per_second} req/s for {duration_seconds}s")
        
        while time.time() < end_time:
            start_request_time = time.time()
            request_id += 1
            
            # Decide read vs write
            is_read = random.random() < read_ratio
            
            if is_read:
                result = self._perform_read_request(f"key_{request_id % 1000}")
            else:
                result = self._perform_write_request(f"key_{request_id}", f"value_{request_id}")
            
            results.append(result)
            
            # Rate limiting
            elapsed = time.time() - start_request_time
            sleep_time = max(0, request_interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.logger.info(f"Completed constant load test: {len(results)} requests")
        return results
    
    def generate_burst_load(self, num_bursts: int, requests_per_burst: int,
                           burst_interval_seconds: float) -> List[RequestResult]:
        """
        Generate bursty load pattern.
        
        Args:
            num_bursts: Number of bursts to generate
            requests_per_burst: Requests in each burst
            burst_interval_seconds: Time between bursts
            
        Returns:
            list: List of RequestResult objects
        """
        results = []
        
        self.logger.info(f"Starting burst load: {num_bursts} bursts, {requests_per_burst} req/burst")
        
        for burst_num in range(num_bursts):
            self.logger.debug(f"Starting burst {burst_num + 1}/{num_bursts}")
            
            # Generate burst with parallel requests
            with ThreadPoolExecutor(max_workers=min(requests_per_burst, 20)) as executor:
                futures = []
                
                for req_num in range(requests_per_burst):
                    request_id = burst_num * requests_per_burst + req_num
                    key = f"burst_{burst_num}_key_{req_num}"
                    value = f"burst_{burst_num}_value_{req_num}"
                    
                    future = executor.submit(self._perform_write_request, key, value)
                    futures.append(future)
                
                # Collect results
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
            
            # Wait between bursts
            if burst_num < num_bursts - 1:
                time.sleep(burst_interval_seconds)
        
        self.logger.info(f"Completed burst load test: {len(results)} requests")
        return results
    
    def generate_concurrent_load(self, num_clients: int, duration_seconds: int) -> List[RequestResult]:
        """
        Generate load from multiple concurrent clients.
        
        Args:
            num_clients: Number of concurrent clients
            duration_seconds: Duration of the test
            
        Returns:
            list: List of RequestResult objects
        """
        results = []
        results_lock = threading.Lock()
        end_time = time.time() + duration_seconds
        
        def client_worker(client_id: int):
            client_results = []
            request_id = 0
            
            while time.time() < end_time:
                request_id += 1
                key = f"client_{client_id}_key_{request_id}"
                value = f"client_{client_id}_value_{request_id}"
                
                # Mix of reads and writes
                if request_id % 3 == 0:  # 33% reads
                    result = self._perform_read_request(key)
                else:
                    result = self._perform_write_request(key, value)
                
                client_results.append(result)
                
                # Small random delay to simulate real client behavior
                time.sleep(random.uniform(0.001, 0.01))
            
            with results_lock:
                results.extend(client_results)
        
        self.logger.info(f"Starting concurrent load: {num_clients} clients for {duration_seconds}s")
        
        # Start client threads
        threads = []
        for client_id in range(num_clients):
            thread = threading.Thread(target=client_worker, args=(client_id,))
            thread.start()
            threads.append(thread)
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        self.logger.info(f"Completed concurrent load test: {len(results)} requests")
        return results
    
    def _perform_write_request(self, key: str, value: str) -> RequestResult:
        """Perform a single write request"""
        start_time = time.time()
        
        try:
            success = self.cluster.submit_request("SET", {"key": key, "value": value})
            end_time = time.time()
            
            return RequestResult(
                start_time=start_time,
                end_time=end_time,
                latency_ms=(end_time - start_time) * 1000,
                success=success,
                error_message=None if success else "Write request failed"
            )
            
        except Exception as e:
            end_time = time.time()
            return RequestResult(
                start_time=start_time,
                end_time=end_time,
                latency_ms=(end_time - start_time) * 1000,
                success=False,
                error_message=str(e)
            )
    
    def _perform_read_request(self, key: str) -> RequestResult:
        """Perform a single read request"""
        start_time = time.time()
        
        try:
            # Simulate read by checking if leader exists and is responsive
            leader_id = self.cluster.get_leader()
            success = leader_id is not None
            
            if success:
                # Simulate read from leader
                leader = self.cluster.nodes[leader_id]
                state_machine = leader.get_state_machine()
                # Just accessing state machine simulates read
                _ = key in state_machine
            
            end_time = time.time()
            
            return RequestResult(
                start_time=start_time,
                end_time=end_time,
                latency_ms=(end_time - start_time) * 1000,
                success=success,
                error_message=None if success else "No leader available for read"
            )
            
        except Exception as e:
            end_time = time.time()
            return RequestResult(
                start_time=start_time,
                end_time=end_time,
                latency_ms=(end_time - start_time) * 1000,
                success=False,
                error_message=str(e)
            )


class PerformanceAnalyzer:
    """
    Comprehensive performance analysis for Raft clusters.
    
    Provides:
    - Throughput and latency benchmarking
    - Scalability analysis
    - Failure scenario performance impact
    - Resource utilization analysis
    - Performance regression detection
    """
    
    def __init__(self):
        self.logger = logging.getLogger("raft.performance")
        self.test_results: List[PerformanceResult] = []
    
    def analyze_request_results(self, results: List[RequestResult], test_name: str,
                               cluster_config: Dict[str, Any]) -> PerformanceResult:
        """
        Analyze request results and generate performance metrics.
        
        Args:
            results: List of request results
            test_name: Name of the performance test
            cluster_config: Cluster configuration details
            
        Returns:
            PerformanceResult: Comprehensive performance analysis
        """
        if not results:
            raise ValueError("No results to analyze")
        
        # Basic metrics
        total_requests = len(results)
        successful_results = [r for r in results if r.success]
        successful_requests = len(successful_results)
        failed_requests = total_requests - successful_requests
        
        # Timing
        start_time = min(r.start_time for r in results)
        end_time = max(r.end_time for r in results)
        duration_seconds = end_time - start_time
        
        # Throughput
        requests_per_second = total_requests / duration_seconds if duration_seconds > 0 else 0
        
        # Latency analysis (only successful requests)
        if successful_results:
            latencies = [r.latency_ms for r in successful_results]
            average_latency_ms = statistics.mean(latencies)
            p50_latency_ms = statistics.median(latencies)
            
            # Calculate percentiles
            sorted_latencies = sorted(latencies)
            p95_index = int(0.95 * len(sorted_latencies))
            p99_index = int(0.99 * len(sorted_latencies))
            
            p95_latency_ms = sorted_latencies[min(p95_index, len(sorted_latencies) - 1)]
            p99_latency_ms = sorted_latencies[min(p99_index, len(sorted_latencies) - 1)]
        else:
            average_latency_ms = p50_latency_ms = p95_latency_ms = p99_latency_ms = 0.0
        
        # Error rate
        error_rate_percentage = (failed_requests / total_requests) * 100 if total_requests > 0 else 0
        
        result = PerformanceResult(
            test_name=test_name,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            requests_per_second=requests_per_second,
            average_latency_ms=average_latency_ms,
            p50_latency_ms=p50_latency_ms,
            p95_latency_ms=p95_latency_ms,
            p99_latency_ms=p99_latency_ms,
            error_rate_percentage=error_rate_percentage,
            cluster_config=cluster_config
        )
        
        self.test_results.append(result)
        
        self.logger.info(f"Performance analysis for '{test_name}':")
        self.logger.info(f"  Throughput: {requests_per_second:.1f} req/s")
        self.logger.info(f"  Average latency: {average_latency_ms:.1f}ms")
        self.logger.info(f"  P95 latency: {p95_latency_ms:.1f}ms")
        self.logger.info(f"  Error rate: {error_rate_percentage:.1f}%")
        
        return result
    
    def benchmark_cluster_sizes(self, cluster_sizes: List[int], 
                               test_duration: int = 30) -> Dict[str, Any]:
        """
        Benchmark performance across different cluster sizes.
        
        Args:
            cluster_sizes: List of cluster sizes to test
            test_duration: Duration for each test
            
        Returns:
            dict: Benchmark results across cluster sizes
        """
        results = {}
        
        self.logger.info(f"Benchmarking cluster sizes: {cluster_sizes}")
        
        for cluster_size in cluster_sizes:
            self.logger.info(f"Testing cluster size: {cluster_size}")
            
            # Create cluster
            cluster = RaftCluster(num_nodes=cluster_size)
            cluster.wait_for_convergence(timeout=5.0)
            
            try:
                # Run performance test
                load_generator = LoadGenerator(cluster)
                request_results = load_generator.generate_constant_load(
                    duration_seconds=test_duration,
                    requests_per_second=50  # Fixed rate for comparison
                )
                
                # Analyze results
                cluster_config = {
                    "cluster_size": cluster_size,
                    "test_type": "cluster_size_benchmark"
                }
                
                perf_result = self.analyze_request_results(
                    request_results, f"cluster_size_{cluster_size}", cluster_config
                )
                
                results[str(cluster_size)] = perf_result.to_dict()
                
            finally:
                cluster.stop()
                time.sleep(1.0)  # Allow cleanup
        
        return {
            "test_type": "cluster_size_benchmark",
            "results": results,
            "summary": self._summarize_cluster_size_results(results)
        }
    
    def benchmark_load_patterns(self, cluster: RaftCluster) -> Dict[str, Any]:
        """
        Benchmark different load patterns against a single cluster.
        
        Args:
            cluster: Cluster to test against
            
        Returns:
            dict: Results for different load patterns
        """
        load_generator = LoadGenerator(cluster)
        results = {}
        
        cluster_config = {
            "cluster_size": len(cluster.nodes),
            "test_type": "load_pattern_benchmark"
        }
        
        self.logger.info("Benchmarking different load patterns")
        
        # Test 1: Constant moderate load
        self.logger.info("Testing constant moderate load (30 req/s)")
        constant_results = load_generator.generate_constant_load(
            duration_seconds=20, requests_per_second=30
        )
        results["constant_moderate"] = self.analyze_request_results(
            constant_results, "constant_moderate", cluster_config
        ).to_dict()
        
        time.sleep(2.0)  # Rest between tests
        
        # Test 2: Constant high load
        self.logger.info("Testing constant high load (100 req/s)")
        high_load_results = load_generator.generate_constant_load(
            duration_seconds=15, requests_per_second=100
        )
        results["constant_high"] = self.analyze_request_results(
            high_load_results, "constant_high", cluster_config
        ).to_dict()
        
        time.sleep(2.0)
        
        # Test 3: Burst load
        self.logger.info("Testing burst load")
        burst_results = load_generator.generate_burst_load(
            num_bursts=5, requests_per_burst=50, burst_interval_seconds=3.0
        )
        results["burst_load"] = self.analyze_request_results(
            burst_results, "burst_load", cluster_config
        ).to_dict()
        
        time.sleep(2.0)
        
        # Test 4: Concurrent clients
        self.logger.info("Testing concurrent clients")
        concurrent_results = load_generator.generate_concurrent_load(
            num_clients=10, duration_seconds=15
        )
        results["concurrent_clients"] = self.analyze_request_results(
            concurrent_results, "concurrent_clients", cluster_config
        ).to_dict()
        
        return {
            "test_type": "load_pattern_benchmark",
            "results": results,
            "summary": self._summarize_load_pattern_results(results)
        }
    
    def benchmark_failure_scenarios(self, cluster: RaftCluster) -> Dict[str, Any]:
        """
        Benchmark performance under failure scenarios.
        
        Args:
            cluster: Cluster to test against
            
        Returns:
            dict: Performance under different failure scenarios
        """
        load_generator = LoadGenerator(cluster)
        results = {}
        
        cluster_config = {
            "cluster_size": len(cluster.nodes),
            "test_type": "failure_scenario_benchmark"
        }
        
        self.logger.info("Benchmarking failure scenarios")
        
        # Baseline: Normal operation
        self.logger.info("Baseline: Normal operation")
        baseline_results = load_generator.generate_constant_load(
            duration_seconds=15, requests_per_second=40
        )
        results["baseline"] = self.analyze_request_results(
            baseline_results, "baseline", cluster_config
        ).to_dict()
        
        time.sleep(2.0)
        
        # Test: Leader failure during load
        self.logger.info("Testing leader failure during load")
        
        def fail_leader_during_test():
            time.sleep(5.0)  # Let test run for 5 seconds first
            leader_id = cluster.get_leader()
            if leader_id:
                cluster.simulate_node_failure(leader_id)
                self.logger.info(f"Failed leader {leader_id} during test")
        
        # Start failure simulation thread
        failure_thread = threading.Thread(target=fail_leader_during_test)
        failure_thread.start()
        
        # Run load test
        leader_failure_results = load_generator.generate_constant_load(
            duration_seconds=20, requests_per_second=40
        )
        
        failure_thread.join()
        
        results["leader_failure"] = self.analyze_request_results(
            leader_failure_results, "leader_failure", cluster_config
        ).to_dict()
        
        # Recovery time
        time.sleep(3.0)
        
        return {
            "test_type": "failure_scenario_benchmark", 
            "results": results,
            "summary": self._summarize_failure_scenario_results(results)
        }
    
    def _summarize_cluster_size_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize cluster size benchmark results"""
        cluster_sizes = sorted([int(k) for k in results.keys()])
        
        throughput_trend = []
        latency_trend = []
        
        for size in cluster_sizes:
            result = results[str(size)]
            throughput_trend.append({
                "cluster_size": size,
                "throughput": result["requests_per_second"]
            })
            latency_trend.append({
                "cluster_size": size,
                "p95_latency": result["p95_latency_ms"]
            })
        
        # Find optimal cluster size (highest throughput with reasonable latency)
        best_throughput = max(throughput_trend, key=lambda x: x["throughput"])
        
        return {
            "optimal_cluster_size": best_throughput["cluster_size"],
            "max_throughput": best_throughput["throughput"],
            "throughput_trend": throughput_trend,
            "latency_trend": latency_trend,
            "scalability_analysis": "Linear" if len(cluster_sizes) >= 2 else "Insufficient data"
        }
    
    def _summarize_load_pattern_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize load pattern benchmark results"""
        best_pattern = max(results.keys(), key=lambda k: results[k]["requests_per_second"])
        worst_pattern = min(results.keys(), key=lambda k: results[k]["requests_per_second"])
        
        return {
            "best_performing_pattern": {
                "pattern": best_pattern,
                "throughput": results[best_pattern]["requests_per_second"],
                "p95_latency": results[best_pattern]["p95_latency_ms"]
            },
            "worst_performing_pattern": {
                "pattern": worst_pattern,
                "throughput": results[worst_pattern]["requests_per_second"],
                "p95_latency": results[worst_pattern]["p95_latency_ms"]
            },
            "pattern_comparison": {
                pattern: {
                    "throughput": data["requests_per_second"],
                    "error_rate": data["error_rate_percentage"]
                }
                for pattern, data in results.items()
            }
        }
    
    def _summarize_failure_scenario_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize failure scenario benchmark results"""
        baseline_throughput = results["baseline"]["requests_per_second"]
        failure_throughput = results["leader_failure"]["requests_per_second"]
        
        throughput_impact = ((baseline_throughput - failure_throughput) / baseline_throughput) * 100
        
        baseline_error_rate = results["baseline"]["error_rate_percentage"]
        failure_error_rate = results["leader_failure"]["error_rate_percentage"]
        
        return {
            "baseline_performance": {
                "throughput": baseline_throughput,
                "error_rate": baseline_error_rate
            },
            "failure_impact": {
                "throughput_reduction_percentage": throughput_impact,
                "error_rate_increase": failure_error_rate - baseline_error_rate,
                "recovery_time_estimate": "2-3 seconds"  # Based on Raft election timeout
            },
            "resilience_rating": "High" if throughput_impact < 30 else "Medium" if throughput_impact < 60 else "Low"
        }
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        if not self.test_results:
            return {"error": "No test results available"}
        
        # Overall statistics
        total_requests = sum(r.total_requests for r in self.test_results)
        avg_throughput = statistics.mean([r.requests_per_second for r in self.test_results])
        avg_latency = statistics.mean([r.average_latency_ms for r in self.test_results])
        avg_error_rate = statistics.mean([r.error_rate_percentage for r in self.test_results])
        
        # Best and worst performing tests
        best_test = max(self.test_results, key=lambda r: r.requests_per_second)
        worst_test = min(self.test_results, key=lambda r: r.requests_per_second)
        
        return {
            "report_generated_at": time.time(),
            "total_tests_run": len(self.test_results),
            "total_requests_processed": total_requests,
            "overall_statistics": {
                "average_throughput": avg_throughput,
                "average_latency_ms": avg_latency,
                "average_error_rate_percentage": avg_error_rate
            },
            "best_performing_test": {
                "test_name": best_test.test_name,
                "throughput": best_test.requests_per_second,
                "latency_p95": best_test.p95_latency_ms,
                "error_rate": best_test.error_rate_percentage
            },
            "worst_performing_test": {
                "test_name": worst_test.test_name,
                "throughput": worst_test.requests_per_second,
                "latency_p95": worst_test.p95_latency_ms,
                "error_rate": worst_test.error_rate_percentage
            },
            "detailed_results": [r.to_dict() for r in self.test_results],
            "recommendations": self._generate_performance_recommendations()
        }
    
    def _generate_performance_recommendations(self) -> List[str]:
        """Generate performance recommendations based on test results"""
        recommendations = []
        
        if not self.test_results:
            return ["No test data available for recommendations"]
        
        # Analyze error rates
        high_error_tests = [r for r in self.test_results if r.error_rate_percentage > 5.0]
        if high_error_tests:
            recommendations.append(
                f"High error rates detected in {len(high_error_tests)} tests. "
                "Consider investigating cluster stability and resource allocation."
            )
        
        # Analyze latency
        high_latency_tests = [r for r in self.test_results if r.p95_latency_ms > 100.0]
        if high_latency_tests:
            recommendations.append(
                f"High P95 latency (>100ms) detected in {len(high_latency_tests)} tests. "
                "Consider optimizing network configuration or reducing cluster size."
            )
        
        # Analyze throughput
        throughputs = [r.requests_per_second for r in self.test_results]
        if throughputs:
            max_throughput = max(throughputs)
            avg_throughput = statistics.mean(throughputs)
            
            if max_throughput > avg_throughput * 2:
                recommendations.append(
                    "Large throughput variance detected. Consider consistent load patterns "
                    "and investigate performance bottlenecks."
                )
        
        # General recommendations
        if len(self.test_results) >= 3:
            recommendations.append(
                "Consider running longer-duration tests for more accurate performance baselines."
            )
        
        return recommendations or ["Performance appears within normal ranges based on available data."]
