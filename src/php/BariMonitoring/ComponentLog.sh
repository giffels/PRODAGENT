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
sleep 5m
done
