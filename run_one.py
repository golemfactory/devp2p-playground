#!/usr/bin/env python
import sys

from playground.run import run
from playground.app import PlaygroundApp, PlaygroundService

my_node_num=CHANGEME
seed=0

run(PlaygroundApp, PlaygroundService, num_nodes=1, start_num=my_node_num, seed=seed, all_nodes=20, max_peers=19, min_peers=7, bootstrap_nodes=[b'enode://316d88516ff96ed22251f178684c6ea296d224be06e7ee301d363abd03aa9ad757b0fb647ad726e2367750e0f30c7240b010efe87a1ae9c5e9896f78e8827aff@52.91.184.235:29870'])
