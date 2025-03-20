import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TException
from supernode import supernode
from supernode.ttypes import node
from thrift.server import TServer
from threading import Lock
import random
import threading

MAX_NODES = 10

# lock for things below
global_lock = Lock()
busy = False
online_nodes = {} # key is node_id, val is node(custom ds)
node_map = {} # key is port, val is ip
next_id = 0

class SupernodeHandler:
    def __init__(self):
        self.pending_join = None
        global node_map
        node_map = self._load_compute_nodes()
        print("✅ Supernode initialized")

    # load compute node info from compute_nodes.txt
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

    # called by a compute node to join the DHT
    def request_join(self, node_port):
        global busy
        global next_id
        print(busy)
        with global_lock:
            if busy:
                return -1
            print(f"📨 Received join request from port {node_port}")
            if node_port not in node_map.keys():
                raise TException("Invalid port number")
            if next_id >= MAX_NODES:
                print("Exceeded max node count")
                return -1
            busy = True
            node_id = next_id
            next_id += 1
            self.pending_join = (node_port, node_id)
            return node_id

    def confirm_join(self):
        global busy
        global online_nodes
        with global_lock:
            print("🟢 Confirm_join called")
            if not self.pending_join:
                print("No node called request_join yet")
                return False
            node_port, node_id = self.pending_join
            ip = node_map[node_port]
            online_nodes[node_id] = node(ip, node_port)
            self.pending_join = None
            print(f"🟢 Node {node_id} (Port: {node_port}) confirmed")
            print("online nodes:", online_nodes)
            busy = False
        return True

    # return a node containing the random node info, if no nodes in the ring, return "" and 0
    def get_node(self):
        with global_lock:
            print("🔀 Providing random node")
            if not online_nodes:
                return node(ip="", port=0)
            node_id = random.choice(list(online_nodes.keys()))
            return online_nodes[node_id]



def main():
    
    if len(sys.argv) < 2:
        print("python3 supernode_server.py <port>")
        return
    input_port = int(sys.argv[1])

    handler = SupernodeHandler()
    processor = supernode.Processor(handler)
    transport = TSocket.TServerSocket(port=input_port)
    transport_factory = TTransport.TBufferedTransportFactory()
    protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, transport_factory, protocol_factory)
    print("supernode is online")
    server.serve()



if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)

