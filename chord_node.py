#!/usr/bin/env python3

import grpc
from concurrent import futures
import time
import threading
import hashlib
import sys
import argparse
from datetime import datetime
import random

import chord_pb2
import chord_pb2_grpc


class ChordNode(chord_pb2_grpc.ChordServiceServicer):
    def __init__(self, address, port, m=32, replication_factor=3):
        self.address = address
        self.port = port
        self.m = m
        self.max_nodes = 2 ** m
        self.replication_factor = replication_factor
        
        # Calculate node ID
        node_key = f"{address}:{port}"
        self.id = self._hash(node_key)
        
        # Pointers
        self.successor = None
        self.predecessor = None
        self.finger_table = [None] * m
        self.successor_list = []
        
        # Data storage
        self.data_store = {}  # Primary data: {key: DataItem}
        self.replica_store = {}  # Replica data: {key: DataItem}
        
        # Statistics
        self.lookup_count = 0
        self.total_hops = 0
        self.operations_count = 0
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Background threads
        self.running = True
        self.stabilize_thread = None
        self.fix_fingers_thread = None
        self.check_replicas_thread = None
        
        # Flag to track initialization
        self.is_initialized = False
        
        # Initialize as single node
        self.successor = self._make_node_info(self.id, self.address, self.port)
        self.predecessor = None
        self.successor_list = [self.successor]
        
        print(f"[Node {self.id}] Created at {address}:{port}")
        print(f"[Node {self.id}] Replication factor: {replication_factor}")
    
    def _hash(self, key):
        h = hashlib.sha1(key.encode()).digest()
        hash_val = int.from_bytes(h[:8], 'big') % self.max_nodes
        return hash_val
    
    def _make_node_info(self, node_id, address, port):
        return chord_pb2.NodeInfo(id=node_id, address=address, port=port)
    
    def _in_range(self, key, start, end, inclusive=True):
        if start == end:
            return inclusive
        
        if start < end:
            if inclusive:
                return start < key <= end
            else:
                return start < key < end
        else:  # Wraps around
            if inclusive:
                return key > start or key <= end
            else:
                return key > start or key < end
    
    def _create_stub(self, node_info):
        target = f"{node_info.address}:{node_info.port}"
        channel = grpc.insecure_channel(
            target,
            options=[
                ('grpc.max_send_message_length', 50 * 1024 * 1024),
                ('grpc.max_receive_message_length', 50 * 1024 * 1024),
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
            ]
        )
        return chord_pb2_grpc.ChordServiceStub(channel)
    
    def _closest_preceding_finger(self, key_id):
     
        with self.lock:
            # Search backwards through finger table
            for i in range(self.m - 1, -1, -1):
                if self.finger_table[i] is not None:
                    finger = self.finger_table[i]
                    if finger.id != self.id and self._in_range(finger.id, self.id, key_id, inclusive=False):
                        if self._is_node_alive(finger, timeout=1):
                            return finger
            
            # Check successor list
            for succ in self.successor_list:
                if succ.id != self.id and self._in_range(succ.id, self.id, key_id, inclusive=False):
                    if self._is_node_alive(succ, timeout=1):
                        return succ
            
            return self._make_node_info(self.id, self.address, self.port)
    
    def _is_node_alive(self, node_info, timeout=1):
        try:
            stub = self._create_stub(node_info)
            response = stub.Ping(chord_pb2.PingRequest(), timeout=timeout)
            return response.alive
        except:
            return False
    
    
    def _build_successor_list(self):
        successors = []
        current = self.successor
        
        # Don't add self to successor list
        if current.id == self.id:
            return []
        
        seen_ids = set([self.id])
        max_attempts = self.replication_factor * 2  # Try harder to build full list
        
        for i in range(max_attempts):
            if len(successors) >= self.replication_factor - 1:
                break
                
            if current.id in seen_ids:
                break
            
            # Check if node is alive before adding
            if self._is_node_alive(current, timeout=1.0):  # Increase timeout
                successors.append(current)
                seen_ids.add(current.id)
                
                try:
                    stub = self._create_stub(current)
                    resp = stub.GetSuccessor(chord_pb2.GetSuccessorRequest(), timeout=2)
                    
                    if resp.node.id in seen_ids:
                        break
                    current = resp.node
                except Exception as e:
                    break
            else:
                # Try to skip failed node
                try:
                    stub = self._create_stub(current)
                    resp = stub.GetSuccessor(chord_pb2.GetSuccessorRequest(), timeout=1)
                    if resp.node.id not in seen_ids:
                        current = resp.node
                        continue
                except:
                    break
        
        return successors


    def _replicate_put(self, key, value, version, timestamp):
        successful_replicas = 0
        
        for successor in self.successor_list[:self.replication_factor-1]:
            if successor.id == self.id:
                continue
                
            try:
                stub = self._create_stub(successor)
                req = chord_pb2.SyncReplicaRequest(
                    key=key,
                    value=value,
                    version=version,
                    timestamp=timestamp
                )
                resp = stub.SyncReplica(req, timeout=1) 
                if resp.success:
                    successful_replicas += 1
                    print(f"[Node {self.id}] Replicated {key} v{version} to node {successor.id}")
            except Exception as e:
                pass  # Already silent
        
        return successful_replicas
    
    def _redistribute_keys_on_join(self, new_node):
        keys_to_transfer = []
        
        with self.lock:
            for key in list(self.data_store.keys()):
                key_hash = self._hash(key)
                # If key now belongs to new node
                if self.predecessor and self._in_range(key_hash, self.predecessor.id, new_node.id, inclusive=True):
                    keys_to_transfer.append(key)
        
        if keys_to_transfer:
            print(f"[Node {self.id}] Transferring {len(keys_to_transfer)} keys to new node {new_node.id}")
            self._transfer_keys_batch(keys_to_transfer, new_node)
    
    def _transfer_keys_batch(self, keys, target_node):
        try:
            stub = self._create_stub(target_node)
            
            for key in keys:
                if key in self.data_store:
                    item = self.data_store[key]
                    req = chord_pb2.PutRequest(
                        key=key,
                        value=item.value,
                        is_replica=False,
                        version=item.version
                    )
                    stub.Put(req, timeout=3)
                    # Remove from local store after successful transfer
                    with self.lock:
                        if key in self.data_store:
                            del self.data_store[key]
            
            print(f"[Node {self.id}] Transferred {len(keys)} keys to node {target_node.id}")
        except Exception as e:
            print(f"[Node {self.id}] Key transfer failed: {e}")
    
    # gRPC Service Methods
    
    def FindSuccessor(self, request, context):
        key_id = request.key_id
        key_id = key_id % self.max_nodes
        
        path = [self.id]
        hops = 1
        
        # Update statistics
        self.lookup_count += 1
        
        with self.lock:
            if self._in_range(key_id, self.id, self.successor.id, inclusive=True):
                self.total_hops += hops
                return chord_pb2.FindSuccessorResponse(
                    node=self.successor,
                    path=path,
                    hops=hops
                )
            
            next_node = self._closest_preceding_finger(key_id)
            
            if next_node.id == self.id:
                self.total_hops += hops
                return chord_pb2.FindSuccessorResponse(
                    node=self.successor,
                    path=path,
                    hops=hops
                )
        
        # Forward to next node
        try:
            stub = self._create_stub(next_node)
            response = stub.FindSuccessor(request, timeout=5)
            response.path.insert(0, self.id)
            response.hops += 1
            self.total_hops += response.hops
            return response
        except Exception as e:
            self.total_hops += hops
            return chord_pb2.FindSuccessorResponse(
                node=self.successor,
                path=path,
                hops=hops
            )
    
    def GetPredecessor(self, request, context):
   
        with self.lock:
            if self.predecessor is not None:
                return chord_pb2.GetPredecessorResponse(
                    node=self.predecessor,
                    exists=True
                )
            else:
                return chord_pb2.GetPredecessorResponse(exists=False)
    
    def GetSuccessor(self, request, context):

        with self.lock:
            return chord_pb2.GetSuccessorResponse(node=self.successor)
    
    def GetSuccessorList(self, request, context):
        with self.lock:
            return chord_pb2.GetSuccessorListResponse(successors=self.successor_list)
    
    def Notify(self, request, context):
        node = request.node
        
        with self.lock:
            should_update = (self.predecessor is None or 
                           self._in_range(node.id, self.predecessor.id, self.id, inclusive=False))
            
            if should_update:
                old_predecessor = self.predecessor
                self.predecessor = node
                print(f"[Node {self.id}] Updated predecessor to {node.id}")
                
                # Transfer keys in background
                if old_predecessor is not None and old_predecessor.id != node.id:
                    threading.Thread(
                        target=self._redistribute_keys_on_join,
                        args=(node,),
                        daemon=True
                    ).start()
        
        return chord_pb2.NotifyResponse(success=True)
    
    def Ping(self, request, context):

        return chord_pb2.PingResponse(alive=True, node_id=self.id)
    
    def Put(self, request, context):
        key = request.key
        value = request.value
        is_replica = request.is_replica
        requested_version = request.version
        
        key_hash = self._hash(key)
        self.operations_count += 1
        
        with self.lock:
            # Check if we're responsible for this key
            if self.predecessor is not None:
                responsible = self._in_range(key_hash, self.predecessor.id, self.id, inclusive=True)
            else:
                responsible = True
            
            if not responsible and not is_replica:
                # Forward to responsible node
                try:
                    find_req = chord_pb2.FindSuccessorRequest(key_id=key_hash)
                    find_resp = self.FindSuccessor(find_req, context)
                    
                    if find_resp.node.id != self.id:
                        stub = self._create_stub(find_resp.node)
                        return stub.Put(request, timeout=5)
                except Exception as e:
                    return chord_pb2.PutResponse(
                        success=False,
                        message=f"Routing failed: {e}"
                    )
            
            # Store locally
            timestamp = int(time.time() * 1000)
            
            if is_replica:
                # For replicas, use provided version
                version = requested_version if requested_version > 0 else 1
                item = chord_pb2.DataItem(
                    key=key,
                    value=value,
                    version=version,
                    timestamp=timestamp
                )
                self.replica_store[key] = item
                return chord_pb2.PutResponse(success=True, message="Replica stored", version=version)
            else:
                # For primary, determine version
                if key in self.data_store:
                    version = self.data_store[key].version + 1
                else:
                    version = 1
                
                item = chord_pb2.DataItem(
                    key=key,
                    value=value,
                    version=version,
                    timestamp=timestamp
                )
                self.data_store[key] = item
                print(f"[Node {self.id}] Stored primary: {key} = {value} v{version}")
                
                # Replicate to successors (only if initialized)
                successful_replicas = 0
                if self.is_initialized:
                    successful_replicas = self._replicate_put(key, value, version, timestamp)
                
                return chord_pb2.PutResponse(
                    success=True, 
                    message=f"Stored with {successful_replicas} replicas",
                    version=version
                )
    
    def SyncReplica(self, request, context):
        key = request.key
        value = request.value
        version = request.version
        timestamp = request.timestamp
        
        with self.lock:
            item = chord_pb2.DataItem(
                key=key,
                value=value,
                version=version,
                timestamp=timestamp
            )
            self.replica_store[key] = item
        
        return chord_pb2.SyncReplicaResponse(success=True)
    
    
    def Get(self, request, context):
        key = request.key
        key_hash = self._hash(key)
        self.operations_count += 1
        
        with self.lock:
            # Check primary store first
            if key in self.data_store:
                item = self.data_store[key]
                return chord_pb2.GetResponse(
                    found=True,
                    value=item.value,
                    version=item.version
                )
            
            # Check replica store
            if key in self.replica_store:
                item = self.replica_store[key]
                return chord_pb2.GetResponse(
                    found=True,
                    value=item.value,
                    version=item.version
                )
        
        # If not found locally, try successor list for replicas
        for succ in self.successor_list:
            if succ.id == self.id:
                continue
            try:
                stub = self._create_stub(succ)
                response = stub.Get(request, timeout=2)
                if response.found:
                    return response
            except:
                continue
        
        # Finally, route to responsible node
        with self.lock:
            if self.predecessor is not None:
                responsible = self._in_range(key_hash, self.predecessor.id, self.id, inclusive=True)
            else:
                responsible = True
            
            if not responsible:
                try:
                    find_req = chord_pb2.FindSuccessorRequest(key_id=key_hash)
                    find_resp = self.FindSuccessor(find_req, context)
                    
                    if find_resp.node.id != self.id:
                        stub = self._create_stub(find_resp.node)
                        return stub.Get(request, timeout=5)
                except Exception as e:
                    pass
            
            return chord_pb2.GetResponse(found=False)
    
    def Delete(self, request, context):
        key = request.key
        is_replica = request.is_replica
        self.operations_count += 1
        
        with self.lock:
            deleted = False
            
            if is_replica:
                if key in self.replica_store:
                    del self.replica_store[key]
                    deleted = True
            else:
                if key in self.data_store:
                    del self.data_store[key]
                    deleted = True
                    
                    # Delete from replicas
                    for succ in self.successor_list:
                        if succ.id != self.id:
                            try:
                                stub = self._create_stub(succ)
                                req = chord_pb2.DeleteRequest(key=key, is_replica=True)
                                stub.Delete(req, timeout=2)
                            except:
                                pass
            
            if deleted:
                return chord_pb2.DeleteResponse(success=True, message="Deleted")
            else:
                return chord_pb2.DeleteResponse(success=False, message="Not found")
    
    def Join(self, request, context):
      
        joining_node = request.joining_node
        new_id = joining_node.id
        
        print(f"[Node {self.id}] Processing join request from node {new_id}")
        
        with self.lock:
            # Check if new node should be between us and our successor
            if self._in_range(new_id, self.id, self.successor.id, inclusive=False) or \
               self.successor.id == self.id:
                # Insert new node
                old_successor = self.successor
                self.successor = joining_node
                
                print(f"[Node {self.id}] Spliced in node {new_id}, old successor was {old_successor.id}")
                
                # Update successor list
                self.successor_list = self._build_successor_list()
                
                return chord_pb2.JoinResponse(
                    successor=old_successor,
                    success=True,
                    message="Joined successfully"
                )
        
        # Forward to successor
        try:
            stub = self._create_stub(self.successor)
            return stub.Join(request, timeout=5)
        except Exception as e:
            return chord_pb2.JoinResponse(
                success=False,
                message=f"Join forward failed: {e}"
            )
    
    def TransferKeys(self, request, context):
        start_id = request.start_id
        end_id = request.end_id
        target_node = request.target_node
        
        transferred_items = []
        
        with self.lock:
            for key in list(self.data_store.keys()):
                key_hash = self._hash(key)
                if self._in_range(key_hash, start_id, end_id, inclusive=True):
                    item = self.data_store[key]
                    transferred_items.append(item)
                    del self.data_store[key]
        
        return chord_pb2.TransferKeysResponse(
            items=transferred_items,
            success=True,
            message=f"Transferred {len(transferred_items)} keys"
        )
    
    def GetStats(self, request, context):
        with self.lock:
            avg_hops = self.total_hops / self.lookup_count if self.lookup_count > 0 else 0
            alive_successors = sum(1 for s in self.successor_list if self._is_node_alive(s, timeout=0.5))
            
            return chord_pb2.GetStatsResponse(
                node_id=self.id,
                primary_keys=len(self.data_store),
                replica_keys=len(self.replica_store),
                lookups=self.lookup_count,
                avg_hops=avg_hops,
                status="active",
                replication_factor=self.replication_factor,
                alive_successors=alive_successors
            )
    
    # Background maintenance
    
    def start_background_threads(self):
        self.stabilize_thread = threading.Thread(target=self._stabilize_loop, daemon=True)
        self.fix_fingers_thread = threading.Thread(target=self._fix_fingers_loop, daemon=True)
        
        self.stabilize_thread.start()
        self.fix_fingers_thread.start()
        
        # Mark as initialized after a delay
        def mark_initialized():
            time.sleep(5)
            self.is_initialized = True
            print(f"[Node {self.id}] Initialization complete")
        
        threading.Thread(target=mark_initialized, daemon=True).start()
        
        print(f"[Node {self.id}] Background threads started")
    
    def _stabilize_loop(self):
        while self.running:
            try:
                self._stabilize()
            except Exception as e:
                pass
            
            time.sleep(3)
    

    def _promote_replicas_on_failure(self, failed_node_id):
        with self.lock:
            promoted = []
            for key, item in list(self.replica_store.items()):
                # Check if this replica should now be primary
                key_hash = self._hash(key)
                if self.predecessor:
                    if self._in_range(key_hash, self.predecessor.id, self.id, inclusive=True):
                        # This key now belongs to us as primary
                        self.data_store[key] = item
                        del self.replica_store[key]
                        promoted.append(key)
                        print(f"[Node {self.id}] Promoted replica {key} to primary")
            
            # Re-replicate promoted keys
            for key in promoted:
                item = self.data_store[key]
                self._replicate_put(key, item.value, item.version, item.timestamp)

    def _stabilize(self):
        with self.lock:
            if self.successor is None or self.successor.id == self.id:
                return
            
            current_successor = self.successor
        
        # Check if successor is alive
        if not self._is_node_alive(current_successor, timeout=1):
            print(f"[Node {self.id}] Successor {current_successor.id} failed, finding new successor")
            self._handle_successor_failure()
            # ADD THIS: Promote replicas to primary when successor fails
            self._promote_replicas_on_failure(current_successor.id)
            return
        try:
            # Get successor's predecessor
            stub = self._create_stub(current_successor)
            resp = stub.GetPredecessor(chord_pb2.GetPredecessorRequest(), timeout=2)
            
            if resp.exists and resp.node.id != self.id:
                # If successor's predecessor is between us and successor, update
                if self._in_range(resp.node.id, self.id, current_successor.id, inclusive=False):
                    with self.lock:
                        self.successor = resp.node
                        print(f"[Node {self.id}] Updated successor to {resp.node.id}")
            
            # Notify successor of our existence
            notify_req = chord_pb2.NotifyRequest(
                node=self._make_node_info(self.id, self.address, self.port)
            )
            stub.Notify(notify_req, timeout=2)
            
            # Update successor list
            with self.lock:
                self.successor_list = self._build_successor_list()
            
        except Exception as e:
            self._handle_successor_failure()
    
    def _handle_successor_failure(self):
        with self.lock:
            # Try to find an alive successor from successor list
            for succ in self.successor_list:
                if succ.id != self.id and self._is_node_alive(succ, timeout=1):
                    self.successor = succ
                    print(f"[Node {self.id}] Switched to backup successor {succ.id}")
                    self.successor_list = self._build_successor_list()
                    return
            
            # If no alive successors, become own successor
            self.successor = self._make_node_info(self.id, self.address, self.port)
            self.successor_list = [self.successor]
    
    def _fix_fingers_loop(self):
        next_finger = 0
        while self.running:
            try:
                self._fix_finger(next_finger)
                next_finger = (next_finger + 1) % self.m
            except:
                pass
            
            time.sleep(1)
    
    def _fix_finger(self, index):
        with self.lock:
            start = (self.id + (2 ** index)) % self.max_nodes
        
        try:
            req = chord_pb2.FindSuccessorRequest(key_id=start)
            resp = self.FindSuccessor(req, None)
            
            with self.lock:
                self.finger_table[index] = resp.node
        except:
            pass
    
    def stop(self):
        self.running = False


def serve(address, port, join_address=None, join_port=None, replication_factor=3):
    node = ChordNode(address, port, replication_factor=replication_factor)
    
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=20),
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024),
            ('grpc.so_reuseport', 1),
            ('grpc.keepalive_time_ms', 10000),
        ]
    )
    chord_pb2_grpc.add_ChordServiceServicer_to_server(node, server)
    
    listen_addr = f'{address}:{port}'
    server.add_insecure_port(listen_addr)
    server.start()
    
    print(f"[Node {node.id}] Server started on {listen_addr}")
    
    # Join existing ring if specified
    if join_address and join_port:
        time.sleep(1)  # Wait for server to be ready
        try:
            print(f"[Node {node.id}] Attempting to join ring via {join_address}:{join_port}")
            
            channel = grpc.insecure_channel(f"{join_address}:{join_port}")
            stub = chord_pb2_grpc.ChordServiceStub(channel)
            
            join_req = chord_pb2.JoinRequest(
                joining_node=node._make_node_info(node.id, address, port)
            )
            
            resp = stub.Join(join_req, timeout=5)
            
            if resp.success:
                node.successor = resp.successor
                node.successor_list = node._build_successor_list()
                print(f"[Node {node.id}] Successfully joined! Successor: {resp.successor.id}")
                print(f"[Node {node.id}] Successor list: {[s.id for s in node.successor_list]}")
            else:
                print(f"[Node {node.id}] Join failed: {resp.message}")
        
        except Exception as e:
            print(f"[Node {node.id}] Join error: {e}")
    
    # Start background maintenance
    node.start_background_threads()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n[Node {node.id}] Shutting down...")
        node.stop()
        server.stop(0)


def main():
    parser = argparse.ArgumentParser(description='Enhanced Chord DHT Node')
    parser.add_argument('port', type=int, help='Port to listen on')
    parser.add_argument('--address', default='localhost', help='Address to bind to')
    parser.add_argument('--join', help='Join existing ring (format: address:port)')
    parser.add_argument('--replication', type=int, default=3, help='Replication factor (default: 3)')
    
    args = parser.parse_args()
    
    join_address = None
    join_port = None
    
    if args.join:
        parts = args.join.split(':')
        if len(parts) == 2:
            join_address = parts[0]
            join_port = int(parts[1])
    
    serve(args.address, args.port, join_address, join_port, args.replication)


if __name__ == '__main__':
    main()
