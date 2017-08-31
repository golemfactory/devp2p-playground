#!/bin/bash

set -eo pipefail

PYDEVP2P_REF="fixes-dev"
PLAYGROUND_REF="choke"

virtualenv --python=python3 venv
. venv/bin/activate

# Install pydevp2p
git clone https://github.com/Wolf480pl/pydevp2p.git
pushd pydevp2p
git checkout $PYDEVP2P_REF
pip install -r requirements.txt
pip install ethereum # for slogging
python setup.py develop
popd

# Install devp2p-playground
git clone https://github.com/Wolf480pl/devp2p-playground.git
pushd devp2p-playground
git checkout $PLAYGROUND_REF
popd
pip install pymultihash pyblake2 bson
