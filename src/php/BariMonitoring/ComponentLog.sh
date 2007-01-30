#!/bin/sh
COUNTER=5
while [ 1 ]
do
type[1]="JobSubmitter"
type[2]="JobTracking"
type[3]="DBSInterface"
type[4]="MergeSensor"

for j in 1 2 3 4
do
echo `tail -n12 $PRODAGENT_WORKDIR/${type[j]}/ComponentLog` > ${type[j]}.txt
done
if [ `expr $COUNTER % 5 ` = 0 ]; then
	sleep 2
	lcg-info --list-ce --vo cms --sed --attrs Tag > software_deployment.txt
	sleep 2
fi
let COUNTER=COUNTER+1 
sleep 2
done
