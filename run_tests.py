#!/usr/bin/env python3
"""
Fast Test Runner for Raft Consensus Algorithm

Provides different test modes:
- quick: Essential tests only (~30 seconds)
- core: Core Raft algorithm tests
- apps: Application simulator tests  
- advanced: Advanced features tests
- full: All tests (slow)

Usage:
  python3 run_tests.py quick
  python3 run_tests.py core
  python3 run_tests.py full
"""

import sys
import os
import time
import unittest
import argparse

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def run_test_category(category, verbose=True):
    """Run tests for a specific category"""
    loader = unittest.TestLoader()
    
    if category == 'quick':
        # Essential tests only - fastest subset
        test_names = [
            'tests.test_core_raft.TestRaftNode',
            'tests.test_core_raft.TestRaftCluster.test_leader_election',
            'tests.test_core_raft.TestRaftCluster.test_client_request_processing',
            'tests.test_applications.TestEtcdSimulator.test_etcd_initialization',
            'tests.test_applications.TestConsulSimulator.test_consul_initialization',
            'tests.test_applications.TestCockroachDBSimulator.test_cockroachdb_initialization',
            'tests.test_advanced_features.TestMultiRaftSystem.test_multiraft_initialization',
        ]
        
        suite = unittest.TestSuite()
        for test_name in test_names:
            try:
                test = loader.loadTestsFromName(test_name)
                suite.addTest(test)
            except Exception as e:
                print(f"Warning: Could not load {test_name}: {e}")
    
    elif category == 'core':
        suite = loader.loadTestsFromName('tests.test_core_raft')
    
    elif category == 'apps':
        suite = loader.loadTestsFromName('tests.test_applications')
    
    elif category == 'advanced':
        suite = loader.loadTestsFromName('tests.test_advanced_features')
    
    elif category == 'full':
        suite = loader.discover('tests/')
    
    else:
        print(f"Unknown category: {category}")
        return False
    
    # Configure test runner
    runner = unittest.TextTestRunner(
        verbosity=2 if verbose else 1,
        stream=sys.stdout,
        buffer=False
    )
    
    # Run tests with timing
    print(f"\n🧪 Running {category} tests...")
    print("=" * 60)
    
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    # Print results
    print("=" * 60)
    print(f"⏱️  Total time: {end_time - start_time:.2f} seconds")
    print(f"✅ Tests run: {result.testsRun}")
    print(f"❌ Failures: {len(result.failures)}")
    print(f"⚠️  Errors: {len(result.errors)}")
    
    if result.failures:
        print(f"\n🔴 Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print(f"\n🟡 Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    if success:
        print(f"\n🎉 All {category} tests PASSED!")
    else:
        print(f"\n💥 Some {category} tests FAILED!")
    
    return success

def show_test_summary():
    """Show available test categories and estimated times"""
    categories = {
        'quick': 'Essential tests only (~15-30 seconds)',
        'core': 'Core Raft algorithm tests (~30-45 seconds)',
        'apps': 'Application simulator tests (~45-60 seconds)',  
        'advanced': 'Advanced features tests (~60-90 seconds)',
        'full': 'All tests (~120-180 seconds)',
    }
    
    print("📋 Available test categories:")
    print()
    for category, description in categories.items():
        print(f"  {category:10} - {description}")
    print()
    print("Usage:")
    print("  python3 run_tests.py <category>")
    print("  python3 run_tests.py quick    # Fastest")
    print("  python3 run_tests.py full     # Complete")

def main():
    parser = argparse.ArgumentParser(description='Fast Raft Test Runner')
    parser.add_argument('category', nargs='?', default='quick',
                       choices=['quick', 'core', 'apps', 'advanced', 'full'],
                       help='Test category to run')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('-l', '--list', action='store_true',
                       help='List available categories')
    
    args = parser.parse_args()
    
    if args.list:
        show_test_summary()
        return
    
    print("🚀 Raft Consensus Algorithm - Fast Test Runner")
    
    try:
        success = run_test_category(args.category, args.verbose)
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nTest runner failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
