#!/bin/sh

exec "$@" 2>&1 |tee /home/ubuntu/playground.log
