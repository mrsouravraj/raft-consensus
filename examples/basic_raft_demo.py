#!/usr/bin/env python3
"""
Basic Raft Consensus Demo

Demonstrates fundamental Raft consensus functionality including:
- Leader election with automatic failover
- Log replication and state machine consistency
- Client request processing
- Network partition handling
- Node failure and recovery scenarios

This educational example shows how Raft maintains consistency
across distributed nodes even during failures.
"""

import sys
import os
import time
import logging

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from raft import RaftCluster


def setup_logging():
    """Configure logging for the demo"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Reduce noise from Raft internals
    logging.getLogger('raft.node').setLevel(logging.WARNING)
    logging.getLogger('raft.cluster').setLevel(logging.INFO)


def print_section(title):
    """Print a section header"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")


def print_cluster_state(cluster):
    """Print current cluster state"""
    stats = cluster.get_cluster_stats()
    consistency = cluster.get_state_consistency()
    
    print(f"\nCluster State:")
    print(f"  Total nodes: {stats.total_nodes}")
    print(f"  Active nodes: {stats.active_nodes}")
    print(f"  Current leader: {stats.leader_id}")
    print(f"  Current term: {stats.current_term}")
    print(f"  Data consistency: {consistency['consistency_percentage']:.1f}%")
    
    if stats.leader_id and stats.nodes:
        leader_stats = stats.nodes[stats.leader_id]
        print(f"  Leader log entries: {leader_stats.log_length}")
        print(f"  Leader committed: {leader_stats.commit_index + 1}")
        print(f"  State machine entries: {leader_stats.state_machine_size}")


def demonstrate_basic_consensus():
    """Demonstrate basic Raft consensus functionality"""
    print_section("Basic Raft Consensus Demonstration")
    
    print("Creating 5-node Raft cluster...")
    cluster = RaftCluster(num_nodes=5)
    
    print("Waiting for leader election...")
    if not cluster.wait_for_convergence(timeout=10.0):
        print("❌ Failed to elect leader within timeout")
        return
    
    leader_id = cluster.get_leader()
    print(f"✅ Leader elected: {leader_id}")
    
    print_cluster_state(cluster)
    
    # Submit client requests
    print_section("Client Request Processing")
    
    requests = [
        ("SET", {"key": "user:alice", "value": "Alice Johnson"}),
        ("SET", {"key": "user:bob", "value": "Bob Smith"}),
        ("SET", {"key": "config:timeout", "value": "30s"}),
        ("SET", {"key": "config:max_conn", "value": "1000"}),
        ("SET", {"key": "app:version", "value": "1.2.3"})
    ]
    
    print(f"Submitting {len(requests)} client requests...")
    
    for i, (command, data) in enumerate(requests):
        success = cluster.submit_request(command, data)
        status = "✅" if success else "❌"
        print(f"  {status} Request {i+1}: {command} {data['key']}={data['value']}")
        time.sleep(0.2)  # Small delay for replication
    
    time.sleep(1.0)  # Allow full replication
    print_cluster_state(cluster)
    
    # Verify data consistency
    print_section("Data Consistency Verification")
    
    consistency_stats = cluster.get_state_consistency()
    print(f"Consistency check: {consistency_stats['consistency_percentage']:.1f}%")
    
    if consistency_stats['consistency_percentage'] == 100.0:
        print("✅ All nodes have identical state machines")
    else:
        print(f"⚠️  Found {len(consistency_stats['mismatched_keys'])} mismatched keys")
        for key, values in consistency_stats['mismatched_keys'].items():
            print(f"   Key '{key}': {values}")
    
    return cluster


def demonstrate_leader_failure_recovery(cluster):
    """Demonstrate leader failure and recovery"""
    print_section("Leader Failure and Recovery")
    
    original_leader = cluster.get_leader()
    print(f"Original leader: {original_leader}")
    
    # Submit a request to show system is working
    success = cluster.submit_request("SET", {"key": "before_failure", "value": "working"})
    print(f"Pre-failure request: {'✅' if success else '❌'}")
    
    # Simulate leader failure
    print(f"\n🔥 Simulating failure of leader {original_leader}...")
    cluster.simulate_node_failure(original_leader)
    
    print("Waiting for new leader election...")
    time.sleep(3.0)  # Allow election time
    
    new_leader = cluster.get_leader()
    if new_leader:
        print(f"✅ New leader elected: {new_leader}")
        
        # Test that system is still functional
        success = cluster.submit_request("SET", {"key": "after_failure", "value": "recovered"})
        print(f"Post-failure request: {'✅' if success else '❌'}")
        
        if success:
            print("🎉 Cluster successfully recovered from leader failure!")
        
        print_cluster_state(cluster)
    else:
        print("❌ No new leader elected - cluster unavailable")


def demonstrate_network_partition_handling(cluster):
    """Demonstrate network partition handling"""
    print_section("Network Partition Handling")
    
    all_nodes = list(cluster.nodes.keys())
    majority = all_nodes[:3]  # 3 out of 5 nodes
    minority = all_nodes[3:]  # 2 out of 5 nodes
    
    print(f"Simulating network partition:")
    print(f"  Majority partition: {majority}")
    print(f"  Minority partition: {minority}")
    
    # Create network partition
    cluster.simulate_network_partition(majority, minority)
    time.sleep(2.0)  # Allow partition to take effect
    
    # Check which partition has the leader
    leader = cluster.get_leader()
    if leader:
        if leader in majority:
            print(f"✅ Majority partition has leader: {leader}")
            
            # Test that majority can still process requests
            success = cluster.submit_request("SET", {"key": "partition_test", "value": "majority_wins"})
            print(f"Majority partition request: {'✅' if success else '❌'}")
            
            if success:
                print("🎉 Majority partition remains operational!")
        else:
            print(f"⚠️  Minority partition has leader: {leader} (shouldn't happen)")
    else:
        print("❌ No leader available after partition")
    
    # Heal the partition
    print("\n🔧 Healing network partition...")
    cluster.heal_network_partition()
    time.sleep(2.0)  # Allow convergence
    
    if cluster.wait_for_convergence(timeout=5.0):
        print("✅ Cluster reconverged after partition healing")
        print_cluster_state(cluster)
    else:
        print("⚠️  Cluster taking time to reconverge")


def demonstrate_concurrent_operations(cluster):
    """Demonstrate concurrent client operations"""
    print_section("Concurrent Client Operations")
    
    import threading
    results = []
    results_lock = threading.Lock()
    
    def worker_thread(worker_id, num_ops):
        """Worker thread simulating a client"""
        worker_results = []
        
        for i in range(num_ops):
            key = f"worker_{worker_id}_item_{i}"
            value = f"data_from_worker_{worker_id}_{i}"
            
            success = cluster.submit_request("SET", {"key": key, "value": value})
            worker_results.append(success)
            
            time.sleep(0.05)  # Small delay between operations
        
        with results_lock:
            results.extend(worker_results)
        
        success_rate = sum(worker_results) / len(worker_results) * 100
        print(f"  Worker {worker_id}: {success_rate:.1f}% success rate")
    
    # Start multiple worker threads
    print("Starting 4 concurrent clients, 5 operations each...")
    
    threads = []
    for worker_id in range(4):
        thread = threading.Thread(target=worker_thread, args=(worker_id, 5))
        threads.append(thread)
        thread.start()
    
    # Wait for all workers to complete
    for thread in threads:
        thread.join()
    
    # Calculate overall success rate
    overall_success_rate = sum(results) / len(results) * 100
    print(f"\nOverall success rate: {overall_success_rate:.1f}%")
    
    time.sleep(1.0)  # Allow replication
    
    # Check final consistency
    consistency = cluster.get_state_consistency()
    print(f"Final consistency: {consistency['consistency_percentage']:.1f}%")
    
    if consistency['consistency_percentage'] == 100.0:
        print("✅ All concurrent operations maintained consistency!")


def run_interactive_demo():
    """Run interactive demo with user prompts"""
    print_section("Interactive Raft Demo")
    
    cluster = None
    
    try:
        cluster = demonstrate_basic_consensus()
        
        if not cluster:
            print("Failed to initialize cluster")
            return
        
        # Ask user what to demonstrate next
        while True:
            print("\nWhat would you like to demonstrate next?")
            print("1. Leader failure and recovery")
            print("2. Network partition handling") 
            print("3. Concurrent operations")
            print("4. Show current cluster state")
            print("5. Exit")
            
            try:
                choice = input("\nEnter your choice (1-5): ").strip()
                
                if choice == '1':
                    demonstrate_leader_failure_recovery(cluster)
                elif choice == '2':
                    demonstrate_network_partition_handling(cluster)
                elif choice == '3':
                    demonstrate_concurrent_operations(cluster)
                elif choice == '4':
                    print_cluster_state(cluster)
                elif choice == '5':
                    break
                else:
                    print("Invalid choice. Please enter 1-5.")
                    
            except KeyboardInterrupt:
                print("\nDemo interrupted by user")
                break
    
    finally:
        if cluster:
            print("\nStopping cluster...")
            cluster.stop()
            print("Cluster stopped.")


def run_automated_demo():
    """Run automated demo showing all features"""
    print_section("Automated Raft Demo - All Features")
    
    cluster = None
    
    try:
        # Basic consensus
        cluster = demonstrate_basic_consensus()
        
        if not cluster:
            print("Failed to initialize cluster")
            return
        
        input("\nPress Enter to continue to leader failure demo...")
        demonstrate_leader_failure_recovery(cluster)
        
        input("\nPress Enter to continue to network partition demo...")
        demonstrate_network_partition_handling(cluster)
        
        input("\nPress Enter to continue to concurrent operations demo...")
        demonstrate_concurrent_operations(cluster)
        
        print_section("Demo Complete")
        print("🎉 All Raft consensus features demonstrated successfully!")
        print("\nKey takeaways:")
        print("- Raft automatically elects leaders and handles failures")
        print("- Strong consistency is maintained across all nodes")
        print("- Majority partitions remain operational")
        print("- Concurrent operations are handled safely")
        print("- The system recovers automatically from failures")
        
    finally:
        if cluster:
            print("\nStopping cluster...")
            cluster.stop()
            print("Demo complete.")


def main():
    """Main demo entry point"""
    setup_logging()
    
    print("🚀 Raft Consensus Algorithm - Educational Demo")
    print("=" * 60)
    print("\nThis demo shows how Raft maintains distributed consensus")
    print("even during node failures and network partitions.")
    
    # Choose demo mode
    print("\nDemo modes:")
    print("1. Interactive demo (choose features to demonstrate)")
    print("2. Automated demo (shows all features)")
    
    try:
        mode = input("\nChoose demo mode (1 or 2): ").strip()
        
        if mode == '1':
            run_interactive_demo()
        elif mode == '2':
            run_automated_demo()
        else:
            print("Invalid choice. Running automated demo...")
            run_automated_demo()
            
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
