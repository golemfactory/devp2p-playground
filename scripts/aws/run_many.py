#!/usr/bin/env python
import sys

from playground.run import run
from playground.app import PlaygroundApp, PlaygroundService

run(PlaygroundApp, PlaygroundService, num_nodes=1, start_num={{ec2_tag_node_num}}, all_nodes=11, max_peers=10, min_peers=4, seed={{40 |random}}, bootstrap_nodes=[x.encode() for x in sys.argv[1:]])
