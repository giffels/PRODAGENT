#!/bin/sh

listA=$1;
listB=$2;

for a in `cat $listA`; do
    val=`grep -c $a $listB`;
    if [[ $val == 0 ]]; then
	echo $a; 
    fi	
done;
