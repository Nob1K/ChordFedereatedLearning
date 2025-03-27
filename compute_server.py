import sys
import glob
import os
import threading
import hashlib

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

import thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from compute import compute
from compute.ttypes import node, weights
from supernode import supernode
from ML import ML

MAX_NODES = 10
M = 4  #4 bits for hash table index
FINGER_TABLE_SIZE = M

# consistent hashing function used in the system
def hash_to_number(input_string):
    sha1_hash = hashlib.sha1(input_string.encode()).hexdigest()
    hash_int = int(sha1_hash, 16)
    
    return hash_int % MAX_NODES

class ComputeHandler:
    def __init__(self, port, super_ip, super_port):
        self.ip = self._load_compute_nodes()[port]
        self.port = port
        self.node_id = None
        self.supernode_ip = super_ip
        self.supernode_port = super_port
        
        self.lock = threading.RLock()
        self.model_lock = threading.RLock()
        
        self.predecessor = None
        self.successor = None
        self.finger_table = [{}] * FINGER_TABLE_SIZE
        
        self.models = {}
        self.data_files = set()
        
        self.join_network()
        
        print(f"✅ Compute node initialized with IP: {self.ip}, Port: {port}, ID: {self.node_id}")
        self.print_info()

    def _load_compute_nodes(self):
        node_map = {}
        try:
            with open('compute_nodes.txt', 'r') as f:
                for line in f:
                    ip, port = line.strip().split(',')
                    node_map[int(port)] = ip
        except FileNotFoundError:
            raise RuntimeError("compute_nodes.txt not found")
        return node_map

    def join_network(self):
        """Join the Chord DHT network through the supernode."""
        try:
            transport = TSocket.TSocket(self.supernode_ip, self.supernode_port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            supernode_client = supernode.Client(protocol)
            
            transport.open()
            
            print("🔄 Requesting to join the network...")
            self.node_id = supernode_client.request_join(self.port)
            
            if self.node_id == -1:
                print("❌ Failed to join: network is busy or full")
                transport.close()
                sys.exit(1)
                
            print(f"🔄 Received node ID: {self.node_id}")
            
            existing_node = supernode_client.get_node()
            
            # first node
            if existing_node.port == 0:
                print("🔄 First node in the network")
                self.predecessor = None
                self.successor = {"id": self.node_id, "node": node(self.ip, self.port)}
                
                for i in range(FINGER_TABLE_SIZE):
                    self.finger_table[i] = {"start": (self.node_id + 2**i) % MAX_NODES, 
                                            "successor_id": self.node_id, 
                                            "node": node(self.ip, self.port)}
            else:
                print(f"🔄 Joining through existing node: {existing_node.ip}:{existing_node.port}")
                # connect to the existing node
                self._init_finger_table(existing_node)
                self._update_others()
            
            # confirm joining
            success = supernode_client.confirm_join()
            if success:
                print("✅ Successfully joined the network")
            else:
                print("❌ Failed to confirm joining")
                transport.close()
                sys.exit(1)
                
            transport.close()
            
        except Exception as e:
            print(f"❌ Error joining network: {e}")
            sys.exit(1)

    def find_successor(self, id):
        """Find the successor node for a given ID."""
        curr_id = self.node_id
        current = {"node": node(self.ip, self.port), "id": curr_id}
        
        # 1 node
        if self.successor.port == self.port:
            return current
        
        # current node is preceding id
        if self._is_between(id, curr_id, self.successor_id()):
            return self.successor
        
        # forward request
        try:
            next_node = self.closest_preceding_node(id)
            if next_node is None:
                return self.successor

            next_node = next_node["node"]
            
            transport = TSocket.TSocket(next_node.ip, next_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = compute.Client(protocol)
            transport.open()
            result = client.find_successor(id)
            transport.close()
            
            return result
        
        except Exception as e:
            print(f"❌ Error finding successor: {e}")
            return current  # Return self if there's an error

    def find_predecessor(self, id):
        """Find the node preceding a given ID."""
        curr_id = self.node_id
        current = {"node":node(self.ip, self.port), "id":curr_id}
        
        # one node in the network
        if self.successor.port == self.port:
            return current
        
        if self._is_between(id, curr_id, self.successor_id()):
            return current
            
        try:
            next_node = self.closest_preceding_node(id)
            if next_node is None:
                return current
            next_node = next_node["node"]
            # Connect to the next node
            transport = TSocket.TSocket(next_node.ip, next_node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = compute.Client(protocol)
            transport.open()
            result = client.find_predecessor(id)
            transport.close()
            
            return result
                
        except Exception as e:
            print(f"❌ Error finding predecessor: {e}")
            return current

    def successor_id(self):
        """Get the ID of the successor node."""
        return self.successor["id"]
    

    def closest_preceding_node(self, hash):
        """Find the closest preceding node for a given hash."""
        with self.lock:
            for i in range(FINGER_TABLE_SIZE - 1, -1, -1):
                finger_start = self.finger_table[i]["start"]
                finger_successor_id = self.finger_table[i]["successor_id"]
                
                if self._is_between(finger_successor_id, self.node_id, hash):
                    return self.finger_table[i]
        
        return None

    def get_predecessor(self):
        """Return this node's predecessor."""
        with self.lock:
            return self.predecessor if self.predecessor else node("", 0)

    def _is_between(self, id, start, end):
        """Check if id is in the range (start, end]."""
        if start < end:
            return start < id <= end
        elif start > end:
            return id > start or id <= end
        else:
            return id != start

    def put_data(self, filename):
        """Store a data file on the node responsible"""
        print(f"📥 Received put_data request for {filename}")
        hash = hash_to_number(filename)
        # base case
        if hash <= self.node_id and hash > self.predecessor["id"]:
            model = ML.mlp()
            model.init_training_random(filename, 26, 20)
            model.train(0.0001, 250)
            v, w = model.get_weights()
            curr_weights = weights(w, v, 0)
            with self.model_lock:
                self.models[filename] = curr_weights
        # forward to other node
        else:
            finger = self.closest_preceding_node(hash)
            # current node is closest preceding finger
            if finger is None:
                node = self.successor["node"]
            # forward to closest preceding finger  
            else:
                node = finger["node"]
            transport = TSocket.TSocket(node.ip, node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = compute.Client(protocol)
            transport.open()
            client.put_data(filename)
            transport.close()
                
                
    def get_model(self, filename):
        """Return a model weights for a given filename."""
        print(f"📤 Received get_model request for {filename}")
        hash = hash_to_number(filename)
        # base case
        if hash <= self.node_id and hash > self.predecessor["id"]:
            with self.model_lock:
                if filename in self.models.keys():
                    return self.models[filename]
                else:
                    # Return a weights object with wait status
                    return weights(w=[[0.0]], v=[[0.0]], status=1)
        # forward to other node
        else:
            finger = self.closest_preceding_node(hash)
            # current node is closest preceding finger
            if finger is None:
                node = self.successor["node"]
            # forward to closest preceding finger  
            else:
                node = finger["node"]
            transport = TSocket.TSocket(node.ip, node.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = compute.Client(protocol)
            transport.open()
            model = client.get_model(filename)
            transport.close()
            return model
        
                
    def print_info(self):
        """Print information about this node's state."""
        with self.lock:
            print("\n----- Node Information -----")
            print(f"Node ID: {self.node_id}")
            print(f"IP:Port: {self.ip}:{self.port}")
            
            if self.predecessor:
                print(f"Predecessor: {self.predecessor["node"].ip}:{self.predecessor["node"].port} (ID: {self.predecessor["id"]})")
            else:
                print("Predecessor: None")
                
            print(f"Successor: {self.successor["node"].ip}:{self.successor["node"].port} (ID: {self.successor["id"]})")
            
            print("\nFinger Table:")
            for i in range(FINGER_TABLE_SIZE):
                if self.finger_table[i]:
                    print(f" [{i}] - start: {self.finger_table[i]["start"]}, successor ID: {self.finger_table[i]["successor_id"]}, socket: {self.finger_table[i]["node"].ip}:{self.finger_table[i]["node"].port}")
                else:
                    print(f" [{i}] - None")
                    
            print("\nStored Data Files:")
            for file in self.data_files:
                print(f"  - {file}")
                
            # print("\nStored Models:")
            # for filename in self.models:
            #     print(f"  - {filename}")
                
            print("----------------------------\n")
    
    # def _init_finger_table(self, bootstrap_node):
        """Initialize finger table using an existing node."""
        # todo:
        # with self.lock:
        #     try:
        #         # Connect to bootstrap node
        #         transport = TSocket.TSocket(bootstrap_node.ip, bootstrap_node.port)
        #         transport = TTransport.TBufferedTransport(transport)
        #         protocol = TBinaryProtocol.TBinaryProtocol(transport)
        #         node_client = compute.Client(protocol)
                
        #         transport.open()
                
        #         # Find successor for this node
        #         self.finger_table[0] = node_client.find_successor(self.node_id)
        #         self.successor = self.finger_table[0]
                
        #         # Get predecessor from successor
        #         succ_transport = TSocket.TSocket(self.successor.ip, self.successor.port)
        #         succ_transport = TTransport.TBufferedTransport(succ_transport)
        #         succ_protocol = TBinaryProtocol.TBinaryProtocol(succ_transport)
        #         succ_client = compute.Client(succ_protocol)
                
        #         succ_transport.open()
        #         self.predecessor = succ_client.get_predecessor()
                
        #         # Notify successor about our presence
        #         succ_client.notify(node(self.ip, self.port))
        #         succ_transport.close()
                
        #         # Fill the rest of the finger table
        #         for i in range(FINGER_TABLE_SIZE - 1):
        #             finger_id = (self.node_id + 2**i) % MAX_NODES
        #             self.finger_table[i+1] = node_client.find_successor(finger_id)
                
        #         transport.close()
                
        #     except Exception as e:
        #         print(f"❌ Error initializing finger table: {e}")
        #         raise

    # def _update_others(self):
        """Update all nodes whose finger tables should refer to us."""
        # todo
        # with self.lock:
        #     try:
        #         for i in range(FINGER_TABLE_SIZE):
        #             # Find the node whose i-th finger might be us
        #             # That would be the node preceding (n - 2^i)
        #             pred_id = (self.node_id - 2**i) % MAX_NODES
        #             pred_node = self.find_predecessor(pred_id)
                    
        #             # Skip if pred_node is us
        #             if pred_node.port == self.port:
        #                 continue
                    
        #             # Connect to predecessor
        #             transport = TSocket.TSocket(pred_node.ip, pred_node.port)
        #             transport = TTransport.TBufferedTransport(transport)
        #             protocol = TBinaryProtocol.TBinaryProtocol(transport)
        #             pred_client = compute.Client(protocol)
                    
        #             transport.open()
        #             # Ask it to update its finger table
        #             pred_client.fix_fingers(i)
        #             transport.close()
                    
        #     except Exception as e:
        #         print(f"❌ Error updating others: {e}")
        
        # def fix_fingers(self, start_id):
    #     """Update a finger table entry."""
    #     with self.lock:
    #         try:
    #             if start_id >= FINGER_TABLE_SIZE:
    #                 return
                    
    #             # Update the finger table entry
    #             next_id = (self.node_id + 2**start_id) % MAX_NODES
    #             self.finger_table[start_id] = self.find_successor(next_id)
                
    #             print(f"📝 Updated finger[{start_id}] to {self.finger_table[start_id].ip}:{self.finger_table[start_id].port}")
                
    #         except Exception as e:
    #             print(f"❌ Error fixing finger {start_id}: {e}")



def start_server(port, super_ip, super_port):
    """Start the Thrift server for this compute node."""
    handler = ComputeHandler(port, super_ip, super_port)
    processor = compute.Processor(handler)
    
    server_transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, server_transport, tfactory, pfactory)
    
    print(f"🚀 Starting compute node server on port: {port}")
    server.serve()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("python3 compute_server.py <supernode_ip> <supernode_port> <compute_port>")
        sys.exit(1)
    super_ip = sys.argv[1]
    super_port = int(sys.argv[2])
    compute_port = int(sys.argv[3])
    start_server(compute_port, super_ip, super_port)