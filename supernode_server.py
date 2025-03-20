import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from supernode import supernode
from supernode.ttypes import node
from thrift.server import TServer
from threading import Lock
import random
import threading

MAX_NODES = 10

busy = False
busy_var_lock = Lock()

online_nodes = {}

class SupernodeHandler:
    def __init__(self):
        self.node_map = self._load_compute_nodes()
        self.lock = threading.Lock()
        self.online_nodes = {}
        self.next_id = 0
        self.pending_join = None
        print("✅ Supernode initialized")

    def _load_compute_nodes(self):
        """加载compute_nodes.txt配置文件"""
        node_map = {}
        try:
            with open('compute_nodes.txt', 'r') as f:
                for line in f:
                    ip, port = line.strip().split(',')
                    node_map[int(port)] = ip
        except FileNotFoundError:
            raise RuntimeError("compute_nodes.txt not found")
        return node_map

    def request_join(self, node_port):
        global busy
        if busy:
            return -1
        else:
            with busy_var_lock:
                busy = True
            print(f"📨 Received join request from port {node_port}")
            if node_port not in self.node_map:
                raise TApplicationException(
                    TApplicationException.INVALID_DATA, 
                    "Invalid port number"
                )
            
            # 分配节点ID
            node_id = self.next_id % MAX_NODES
            self.next_id += 1
            self.pending_join = (node_port, node_id)  # 暂存信息
            return node_id

    def confirm_join(self):
        with self.lock:
            print("🟢 Confirm_join called")
            if not self.pending_join:
                return False
            node_port, node_id = self.pending_join
            ip = self.node_map[node_port]
            self.online_nodes[node_id] = node(ip, node_port)
            self.pending_join = None
            print(f"🟢 Node {node_id} (Port: {node_port}) confirmed")
            return True

    def get_node(self):
        print("🔀 Providing random node")
        if not self.online_nodes:
            raise TApplicationException(...)
        node_id = random.choice(list(self.online_nodes.keys()))
        ip, port = self.online_nodes[node_id]
        return node(ip=ip, port=port)



def main():
    
    if len(sys.argv) < 2:
        print("python3 supernode.py <port>")
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

