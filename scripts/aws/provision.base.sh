#!/bin/bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

sudo DEBIAN_FRONTEND=noninteractive apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -qy \
    autoconf \
    awscli \
    docker.io \
    libffi-dev \
    libgmp-dev \
    libssl-dev \
    libtool \
    openssl \
    pkg-config \
    python3 \
    python3-pip \
    virtualenv \
    software-properties-common \
    iftop \
    ;

#pip install -q --upgrade pip
