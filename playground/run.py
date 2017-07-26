import sys
import random

import gevent

from devp2p.app_helper import mk_privkey, create_app, serve_until_stopped
from devp2p.crypto import privtopub as privtopub_raw
from devp2p.discovery import NodeDiscovery
from devp2p.peermanager import PeerManager
from devp2p.utils import host_port_pubkey_to_uri, update_config_with_defaults

from .app import PlaygroundApp, PlaygroundService

def run(app_class, service_class, num_nodes=3, all_nodes=0, seed=0, min_peers=2, max_peers=2, random_port=False, bootstrap_nodes=[]):
    gevent.get_hub().SYSTEM_ERROR = BaseException
    if random_port:
        base_port = random.randint(10000, 60000)
    else:
        base_port = 29870

    if not all_nodes:
        all_nodes = num_nodes

    # get bootstrap node (node0) enode
    bootstrap_node_privkey = mk_privkey('%d:udp:%d' % (seed, 0))
    bootstrap_node_pubkey = privtopub_raw(bootstrap_node_privkey)
    enode = host_port_pubkey_to_uri('0.0.0.0', base_port, bootstrap_node_pubkey)
    print(enode)
    bootstrap_nodes.append(enode)

    #services = [NodeDiscovery, PeerManager, service_class]
    services = app_class.services

    # prepare config
    base_config = dict()
    for s in services:
        update_config_with_defaults(base_config, s.default_config)

    base_config['discovery']['bootstrap_nodes'] = bootstrap_nodes
    base_config['seed'] = seed
    base_config['base_port'] = base_port
    base_config['num_nodes'] = all_nodes
    base_config['min_peers'] = min_peers
    base_config['max_peers'] = max_peers

    # prepare apps
    apps = []
    for node_num in range(num_nodes):
        app = create_app(node_num, base_config, services, app_class)
        apps.append(app)

    # start apps
    serve_until_stopped(apps)

if __name__ == '__main__':
    run(PlaygroundApp, PlaygroundService, num_nodes=3, all_nodes=4, max_peers=2, min_peers=1, bootstrap_nodes=[x.encode() for x in sys.argv[1:]])
    #run(PlaygroundApp, PlaygroundService, bootstrap_nodes=sys.argv[1:])
