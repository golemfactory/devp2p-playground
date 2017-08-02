#!/usr/bin/env python
import sys

from playground.run import run
from playground.app import PlaygroundApp, PlaygroundService

run(PlaygroundApp, PlaygroundService, num_nodes=3, all_nodes=10, max_peers=8, min_peers=3, bootstrap_nodes=[x.encode() for x in sys.argv[1:]])
