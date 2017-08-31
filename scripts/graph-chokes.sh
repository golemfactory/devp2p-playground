#!/bin/sh

#grep unchok "$1" |awk '{$1=$5=$6=""; $4=":"; print}' |sed -r 's/chokes//; s/[0-9]+m//g' |sed -r 's/<FileSwarmProtocol <Peer\([^(]*\) b([^>]*)>>/\1/g' |tr -d "{}',][\033" |sed -r 's/ +/ /g; s/^ //g' |awk -F':' '{ if ($1 ~ /NODE0/) print "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"; gsub(/NODE/, "", $2); gsub(/NODE/, "", $3); split($2, unch, " "); split($3, ch, " "); for (i=0;i<=9;++i) N[i] = " "; for (i in unch) N[unch[i]]="U"; for (i in ch) N[ch[i]]="."; n=""; for (i in N) n = n " " N[i]; print $1 " " n}'
grep unchok "$1" |awk '{$1=$5=$6=""; $4=":"; print}' |sed -r 's/chokes|uninterested//g' |sed -r 's/[0-9]+m//g' \
|sed -r 's/<FileSwarmProtocol <Peer\([^(]*\) b([^>]*)>>/\1/g' |tr -d "{}',][\033" |sed 's/set()//g' |sed -r 's/ +/ /g; s/^ //g' \
| awk -F':' '{ if ($1 ~ /NODE0/) print "\n\n\n\n\n\n\n\n\n\n\n";
	gsub(/NODE/, "", $2); gsub(/NODE/, "", $3); gsub(/NODE/, "", $4); split($2, unch, " "); split($3, ch, " "); split($4, unin, " ");
	for (i=0;i<=19;++i) N[i] = " ";
	for (i in unch) N[unch[i]]="U"; for (i in ch) N[ch[i]]="*"; n=""; for (i in unin) N[unin[i]]=".";
	for (i in N) n = n " " N[i]; print $1 "\t " n}'
