#!/bin/sh

# 1. filter out irrelevant lines
# 2. remove garbage
# 3. replace 'set()' with empty list
# 4. replace '<...<...NODEx...>>' with 'NODEx'
# 5. JSONize - replace '{...}' with JSON lists
# 6. JSONize - move the node name into the dict and make that dict a dict, not a list
# 7. parse as JSON and output chokes/unchokes/uninterested in desired order
# 8. deJSONize

grep unchok "$1" \
|sed -r 's/\x1B\[[0-9]+m//g; s/^DEBUG:app[ \t]*//; s/will unchoke *//' \
|sed -r 's/set\(\)/[]/g' \
|sed -r 's/<FileSwarmProtocol <Peer\([^)]*\) b([^>]*)>>/\1/g' \
|tr "{}'" '[]"' \
|sed -r 's/^(NODE[0-9]+) +\[/{"node": "\1", /; s/\] *$/}/' \
|jq -r '"\(.node) : \(.unchokes) : \(.chokes) : \(.uninterested)"' \
|tr -d '"[]' |tr ',' ' '
