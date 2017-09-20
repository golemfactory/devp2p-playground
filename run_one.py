#!/usr/bin/env python
import sys
import random

from playground.run import run
from playground.app import PlaygroundApp, PlaygroundService

run(PlaygroundApp, PlaygroundService, num_nodes=1, all_nodes=15, max_peers=14, min_peers=7,
    seed=random.randint(43, 1000),
    base_port=29870,
    bootstrap_nodes=[x.encode() for x in sys.argv[1:]])
