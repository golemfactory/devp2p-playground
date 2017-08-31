#!/bin/awk -f

BEGIN{
  FS=":"
  screen_size=32
  lines_left=0
  max_nodes=20
}

{
  if ($1 ~ /NODE0/) {
    for (i=lines_left; i>0; --i) {
      print "";
    }
    lines_left = screen_size;
  }

  gsub(/NODE/, "", $2);
  gsub(/NODE/, "", $3);
  gsub(/NODE/, "", $4);

  split($2, unchoked, " ");
  split($3, choked, " ");
  split($4, uninterested, " ");

  for (i=0; i<max_nodes; ++i) N[i]=" ";

  for (i in unchoked) N[unchoked[i]]="U";
  for (i in choked) N[choked[i]]="*";
  for (i in uninterested) N[uninterested[i]]=".";

  n = "";
  for (i in N) n = n " " N[i];
  print $1 "\t " n;
  --lines_left;
}
