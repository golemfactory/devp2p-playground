#!/bin/sh

journalctl -u playground.service |grep this_enode |tail -n1 |sed -r "s%.*b'(enode://[^']+)'%\1%"
