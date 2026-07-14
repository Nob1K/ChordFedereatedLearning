import sys
import glob
import os
import time

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('../thrift-0.19.0/lib/py/build/lib*')[0])

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from compute import compute
from compute.ttypes import node, weights
from supernode import supernode
from ML import ML

# populate files
def get_files_in_directory(directory_path):
    file_paths = []
    
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_paths.append(os.path.join(root, file))
    
    return file_paths

def main():
    
    if len(sys.argv) < 3:
        print("usage: python3 client.py <supernode_ip> <supernode_port>")
        return
    training_files = get_files_in_directory("letters")[:-15]
    # contact supernode and get connection point
    super_ip = sys.argv[1]
    super_port = int(sys.argv[2])
    super_transport = TSocket.TSocket(super_ip, super_port)
    super_transport = TTransport.TBufferedTransport(super_transport)
    super_protocol = TBinaryProtocol.TBinaryProtocol(super_transport)
    super_client = supernode.Client(super_protocol)
    super_transport.open()
    contact = super_client.get_node()
    super_transport.close()
    if contact.id != -1:
        # initialize shared ML model
        mlp = ML.mlp()
        mlp.init_training_random(training_files[0], 26, 20)
        shared_v, shared_w = mlp.get_weights()
        shared_v = ML.scale_matricies(shared_v, 0)
        shared_w = ML.scale_matricies(shared_w, 0)
    
        print(f"Received contact info for node {contact.id} at {contact.ip}:{contact.port}")
        # connect with the contact received from supernode
        node_transport = TSocket.TSocket(contact.ip, contact.port)
        node_transport = TTransport.TBufferedTransport(node_transport)
        protocal = TBinaryProtocol.TBinaryProtocol(node_transport)
        node_client = compute.Client(protocal)
        node_transport.open()
        # send out all training files
        for file in training_files:
            node_client.put_data(file)
        # sleep for a bit to wait for training
        print("Done providing data, sleeping to let the models train")
        time.sleep(90)
        # acquire and aggregate each model
        for file in training_files:
            model = node_client.get_model(file)
            while model.status == 1:
                print(f"waiting for file {file}'s model to be done")
                time.sleep(10)
                model = node_client.get_model(file)
            shared_v = ML.sum_matricies(shared_v, model.v)
            shared_w = ML.sum_matricies(shared_w, model.w)
        # validate
        shared_v = ML.scale_matricies(shared_v, 1/len(training_files))
        shared_w = ML.scale_matricies(shared_w, 1/len(training_files))
        mlp.set_weights(shared_v, shared_w)
        v_err = mlp.validate("validate_letters.txt")
        print("final validation error:", v_err)
        node_transport.close()
    



if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)