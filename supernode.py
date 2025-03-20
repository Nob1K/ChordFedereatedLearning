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

class SupernodeHandler:
    def __init__(self):
        self.node = node(1, 2)
        self.lock = Lock()

    def request_join(self, node_port):
        return 0 

    def confirm_join(self):
        return True

    def get_node(self):
        return self.node


def main():
    
    if len(sys.argv) < 2:
        print("python3 supernode.py <port>")
        return
    port = int(sys.argv[1])

    handler = SupernodeHandler()
    processor = supernode.Processor(handler)
    transport = TSocket.TServerSocket(port)
    transport_factory = TTransport.TBufferedTransportFactory()
    protocol_factory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, transport_factory, protocol_factory)
    server.serve()
    


if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)

