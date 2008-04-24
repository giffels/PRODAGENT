#!/bin/sh
echo $(date +"%Y%m%d-%Hh%Mm%Ss %s") "  " `uptime` " " `cat /proc/meminfo | grep -E Mem\|Swap\|Cached | awk '{printf("%i ",$2)} END {printf("\n")}' | awk '{print "mem: " 100-100*($2+$3)/$1 "  cached: " 100*$3/$1"  swap: " 100-100*($4+$6)/$5}'`

