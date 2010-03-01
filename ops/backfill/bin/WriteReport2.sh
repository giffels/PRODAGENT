#!/bin/sh

alias condorq='condor_q -format "%i  " ClusterID -format " %i " JobStatus  -format " %d " ServerTime-EnteredCurrentStatus -format "%s" ProdAgent_JobID -format " %s\n" DESIRED_Sites' 


sites="ASGC CNAF IN2P3 FNAL FZK PIC RAL"

for s in $sites; do 
    for wf in `condorq | grep $s | grep -v merge | awk '{print $4}' | sed -s 's/-[[:digit:]]*$//' | sort -n -r | uniq`; do
	~/backfill/bin/CheckStatus.sh $wf > .checkStatus
        total=`grep processing .checkStatus | head -n 1 | awk '{print $2}'`;
        success=`grep successes .checkStatus | awk '{print $2}'`;
        failed=`grep failures .checkStatus | awk '{print $2}'`;
        rightnow=`date`;
        echo "| $s | $wf | $total | $success | $failed | - |"
    done
done
