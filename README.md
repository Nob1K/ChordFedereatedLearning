# ChordFedereatedLearning

If the gen-py directory is not present, generate the Thrift RPC code by running:
thrift --gen py compute.thrift
thrift --gen py supernode.thrift

Make sure the sys paths are set correctly to link the thrift library in .py executables before running servers and clients.
Here is how to run different components of the system:
1. Supernode - run the supernode by running: "python3 supernode_server.py <port>" , the port number which supernode runs on can be decided by the user.
2. Compute - run the compute node by running: "python3 compute_server.py <supernode_ip> <supernode_port> <compute_port>" , compute_nodes can be on different machines, as long as the supernode information given is correct. The socket which compute_nodes run from should correspond to one in compute_nodes.txt
3. Client - run the client by running : "python3 client.py <supernode_ip> <supernode_port>" , the client uses validate_letters.txt to validate the ML model, so the file needs to be in the same directory.