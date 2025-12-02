#!/usr/bin/env python3


import grpc
import time
import threading
import random
import statistics
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys
import argparse
import hashlib

import chord_pb2
import chord_pb2_grpc


class EnhancedStressTester:
    def __init__(self, node_addresses):
        self.node_addresses = node_addresses
        self.stubs = []
        self.results = {
            'put_latencies': [],
            'get_latencies': [],
            'delete_latencies': [],
            'lookup_latencies': [],
            'put_successes': 0,
            'put_failures': 0,
            'get_successes': 0,
            'get_failures': 0,
            'delete_successes': 0,
            'delete_failures': 0,
            'total_hops': [],
            'errors': [],
            'timestamps': {
                'put': [],
                'get': [],
            }
        }
        
        for address, port in node_addresses:
            channel = grpc.insecure_channel(
                f"{address}:{port}",
                options=[
                    ('grpc.max_send_message_length', 50 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 50 * 1024 * 1024),
                ]
            )
            self.stubs.append(chord_pb2_grpc.ChordServiceStub(channel))
    
    def _hash(self, key):
        h = hashlib.sha1(key.encode()).digest()
        return int.from_bytes(h[:8], 'big') % (2**32)
    
    def _random_stub(self):
        return random.choice(self.stubs)
    
    def _measure_operation(self, operation, *args, **kwargs):
        start_time = time.time()
        try:
            result = operation(*args, **kwargs)
            latency = (time.time() - start_time) * 1000
            return result, latency, None
        except Exception as e:
            latency = (time.time() - start_time) * 1000
            return None, latency, str(e)
    
    def test_put(self, key, value):
        stub = self._random_stub()
        request = chord_pb2.PutRequest(key=key, value=value, is_replica=False)
        
        result, latency, error = self._measure_operation(
            stub.Put, request, timeout=20
        )
        
        self.results['timestamps']['put'].append(time.time())
        
        if error:
            self.results['put_failures'] += 1
            self.results['errors'].append(f"PUT {key}: {error}")
            return False, latency
        elif result and result.success:
            self.results['put_successes'] += 1
            self.results['put_latencies'].append(latency)
            return True, latency
        else:
            self.results['put_failures'] += 1
            return False, latency
    
    def test_get(self, key):
        stub = self._random_stub()
        request = chord_pb2.GetRequest(key=key)
        
        result, latency, error = self._measure_operation(
            stub.Get, request, timeout=20
        )
        
        self.results['timestamps']['get'].append(time.time())
        
        if error:
            self.results['get_failures'] += 1
            self.results['errors'].append(f"GET {key}: {error}")
            return False, latency, None
        elif result and result.found:
            self.results['get_successes'] += 1
            self.results['get_latencies'].append(latency)
            return True, latency, result.value
        else:
            self.results['get_failures'] += 1
            return False, latency, None
    
    def test_lookup(self, key_id):
        stub = self._random_stub()
        request = chord_pb2.FindSuccessorRequest(key_id=key_id)
        
        result, latency, error = self._measure_operation(
            stub.FindSuccessor, request, timeout=20
        )
        
        if error:
            self.results['errors'].append(f"LOOKUP {key_id}: {error}")
            return False, latency, 0
        elif result:
            self.results['lookup_latencies'].append(latency)
            self.results['total_hops'].append(result.hops)
            return True, latency, result.hops
        else:
            return False, latency, 0
    
    def get_node_stats(self):
        stats = []
        for i, stub in enumerate(self.stubs):
            try:
                request = chord_pb2.GetStatsRequest()
                response = stub.GetStats(request, timeout=5)
                stats.append({
                    'node_id': response.node_id,
                    'address': f"{self.node_addresses[i][0]}:{self.node_addresses[i][1]}",
                    'primary_keys': response.primary_keys,
                    'replica_keys': response.replica_keys,
                    'lookups': response.lookups,
                    'avg_hops': response.avg_hops,
                    'alive_successors': response.alive_successors,
                    'replication_factor': response.replication_factor
                })
            except Exception as e:
                stats.append({
                    'node_id': -1,
                    'address': f"{self.node_addresses[i][0]}:{self.node_addresses[i][1]}",
                    'error': str(e)
                })
        return stats
    
    def calculate_network_metrics(self):
        n = len(self.node_addresses)
        theoretical_hops = n.bit_length() - 1 if n > 1 else 1
        
        actual_avg_hops = statistics.mean(self.results['total_hops']) if self.results['total_hops'] else 0
        efficiency_ratio = (theoretical_hops / actual_avg_hops * 100) if actual_avg_hops > 0 else 0
        
        return {
            'network_size': n,
            'theoretical_hops': theoretical_hops,
            'actual_avg_hops': actual_avg_hops,
            'efficiency_ratio': efficiency_ratio
        }
    
    def save_results(self, filename):
        output = {
            'timestamp': datetime.now().isoformat(),
            'node_count': len(self.node_addresses),
            'results': {
                'put_successes': self.results['put_successes'],
                'put_failures': self.results['put_failures'],
                'get_successes': self.results['get_successes'],
                'get_failures': self.results['get_failures'],
                'delete_successes': self.results['delete_successes'],
                'delete_failures': self.results['delete_failures'],
                'put_latencies': self.results['put_latencies'],
                'get_latencies': self.results['get_latencies'],
                'delete_latencies': self.results['delete_latencies'],
                'lookup_latencies': self.results['lookup_latencies'],
                'total_hops': self.results['total_hops'],
                'test_duration': self.results.get('test_duration', 0),
                'errors': self.results['errors']
            },
            'network_metrics': self.calculate_network_metrics()
        }
        
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\n Results saved to {filename}")
        return output


def run_comprehensive_test(tester, num_operations=5000, num_threads=20, duration=180):
    print("\n" + "="*70)
    print("COMPREHENSIVE TEST FOR REPORT METRICS")
    print("="*70)
    
    start_time = time.time()
    
    # Phase 1: Initial data population
    print(f"\nPhase 1: Populating with {num_operations} keys...")
    for i in range(num_operations):
        key = f"test_key_{i}"
        value = f"value_{i}_{random.randint(1000, 9999)}"
        tester.test_put(key, value)
        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1}/{num_operations} keys")
    
    print("\n Phase 1 complete")
    time.sleep(3)
    
    # Phase 2: Mixed concurrent workload
    print(f"\nPhase 2: Mixed workload ({duration}s, {num_threads} threads)...")
    print("  This generates realistic load patterns for better metrics")
    stop_flag = threading.Event()
    operation_count = [0]
    
    def worker():
        while not stop_flag.is_set():
            operation = random.choice(['put', 'get', 'get', 'get'])  # 75% reads
            key = f"test_key_{random.randint(0, num_operations-1)}"
            
            if operation == 'put':
                value = f"updated_{random.randint(1000, 9999)}"
                tester.test_put(key, value)
            else:
                tester.test_get(key)
            
            operation_count[0] += 1
    
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    # Run for duration
    for elapsed in range(0, duration, 15):
        time.sleep(15)
        print(f"  Running... {elapsed+15}/{duration}s ({operation_count[0]} ops)")
    
    stop_flag.set()
    for t in threads:
        t.join()
    
    print(f"\n Phase 2 complete ({operation_count[0]} operations)")
    
    # Phase 3: Lookup performance
    num_lookups = 2000
    print(f"\nPhase 3: Testing lookup performance ({num_lookups} lookups)...")
    print("  More lookups = better hop distribution statistics")
    for i in range(num_lookups):
        key_id = random.randint(0, 2**32 - 1)
        tester.test_lookup(key_id)
        if (i + 1) % 400 == 0:
            print(f"  Progress: {i + 1}/{num_lookups} lookups")
    
    print("\n Phase 3 complete")
    
    tester.results['test_duration'] = time.time() - start_time
    
    # Collect node statistics
    print("\nCollecting node statistics...")
    node_stats = tester.get_node_stats()
    
    return node_stats


def print_summary(tester, node_stats):
    """Print test summary"""
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    total_ops = (tester.results['put_successes'] + tester.results['get_successes'])
    duration = tester.results.get('test_duration', 0)
    
    print(f"\nOperations:")
    print(f"  PUT: {tester.results['put_successes']} success, {tester.results['put_failures']} failures")
    print(f"  GET: {tester.results['get_successes']} success, {tester.results['get_failures']} failures")
    print(f"  Total: {total_ops} operations in {duration:.2f}s")
    print(f"  Throughput: {total_ops/duration:.2f} ops/sec")
    
    if tester.results['put_latencies']:
        put_lats = tester.results['put_latencies']
        print(f"\nPUT Latency:")
        print(f"  P50: {statistics.median(put_lats):.2f}ms")
        print(f"  P95: {statistics.quantiles(put_lats, n=20)[18]:.2f}ms")
        print(f"  P99: {statistics.quantiles(put_lats, n=100)[98]:.2f}ms")
    
    if tester.results['get_latencies']:
        get_lats = tester.results['get_latencies']
        print(f"\nGET Latency:")
        print(f"  P50: {statistics.median(get_lats):.2f}ms")
        print(f"  P95: {statistics.quantiles(get_lats, n=20)[18]:.2f}ms")
        print(f"  P99: {statistics.quantiles(get_lats, n=100)[98]:.2f}ms")
    
    if tester.results['total_hops']:
        print(f"\nLookup Performance:")
        print(f"  Average hops: {statistics.mean(tester.results['total_hops']):.2f}")
        print(f"  Min hops: {min(tester.results['total_hops'])}")
        print(f"  Max hops: {max(tester.results['total_hops'])}")
    
    print(f"\nNode Distribution:")
    for stat in node_stats:
        if 'error' not in stat:
            print(f"  Node {stat['node_id']}: {stat['primary_keys']} primary, {stat['replica_keys']} replicas")


def main():
    parser = argparse.ArgumentParser(description='Enhanced Chord Stress Test for Report')
    parser.add_argument('nodes', nargs='+', help='Node addresses (host:port)')
    parser.add_argument('--operations', type=int, default=2000, help='Initial keys to insert')
    parser.add_argument('--threads', type=int, default=20, help='Concurrent threads')
    parser.add_argument('--duration', type=int, default=120, help='Mixed workload duration (seconds)')
    parser.add_argument('--output', default='report_metrics.json', help='Output file')
    
    args = parser.parse_args()
    
    node_addresses = []
    for node in args.nodes:
        parts = node.split(':')
        if len(parts) != 2:
            print(f"Error: Invalid format '{node}'. Use host:port")
            sys.exit(1)
        node_addresses.append((parts[0], int(parts[1])))
    
    print(f"\n{'='*70}")
    print(f"Connecting to {len(node_addresses)} nodes...")
    print(f"{'='*70}")
    
    tester = EnhancedStressTester(node_addresses)
    
    node_stats = run_comprehensive_test(
        tester,
        num_operations=args.operations,
        num_threads=args.threads,
        duration=args.duration
    )
    
    print_summary(tester, node_stats)
    
    results = tester.save_results(args.output)
    
    print(f"\n Test complete! Results saved to {args.output}")
    print(f"  Next: Run plot_report.py to generate plots and tables")


if __name__ == '__main__':
    main()
