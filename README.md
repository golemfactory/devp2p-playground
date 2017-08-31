# An app for testing various DevP2P proof-of-concepts

## Running

Use `run_one.py` to start one node, or `run_many.py` to start multiple nodes in one process.
They both take a list of additional bootstrap nodes as arguments (`enode://` URIs).
Edit the scripts to set number of nodes, min/max number of peers, RNG seed, etc.
The RNG seed from the run scripts is used for generating keypairs, make sure you don't have multiple nodes with the same keypair.

By default, each node will derive its port number by adding its node number to 29870. 

There is currently no NAT traversal in pydevp2p.
For node A to connect to node B, node B's devp2p port must be reachable from node A, and node A must receive the address and port under which node B is reachable from it, either explicitely as a bootstrap node, or from a neighbour that can reach node B under _the same address_.
So eg. if both A and B are behind NAT, and node C is not, and both A and B are connected to C, then A won't be able to connect to B's private address. 

## TCP console

Each node will start TCP console at port equal to its devp2p port minus 100 (eg.
node 12 will have devp2p port 29882 and console port 29782). You can connect to it with telnet, netcat, etc.

Anything you type that starts with `/` will be handled by respective `cmd_...` method in `app.py` (eg. `/foo` will be handled by `cmd_foo`).
Anything you type that doesn't start with `/` will be handled by `cmd_chat`.

## PoCs:

The following proof of concepts are implemented:
- broadcast chat (useful for testing connectivity)
- direct file transfer (`/file <filename>` command)
- bittorrent-over-devp2p file transfer (`/seed <filename>` command)
