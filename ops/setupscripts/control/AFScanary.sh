#!/bin/bash

while [ 1==1 ]

do

 afspathtotest='/afs/cern.ch/user/c/cmsprod/scratch2/cmsprod/logs/testing123'

 mkdir $afspathtotest

 returnvalue=$?

 echo AFS canary output follows: > /tmp/canaryfile

 date >> /tmp/canaryfile
 tokens >> /tmp/canaryfile
 klist >> /tmp/canaryfile
 prodAgentd --status >> /tmp/canaryfile
 

 #cat /tmp/canaryfile

 TO='dmason@fnal.gov,mlmiller@mit.edu'
 #TO='dmason@fnal.gov'

 

 if [ $returnvalue -eq 0 ]
  then
 #   echo 'woohoo!'
    SUBJ='AFScanary: Woohoo!  AFS worked!'
    rmdir $afspathtotest
  else
 #   echo 'crap'
    SUBJ='AFScanary: CRAP!  AFS FAIL'
    printenv >> /tmp/canaryfile

    echo restarting DBSInterface and JobSubmitter... >> /tmp/canaryfile
    aklog
    prodAgentd --restart --components=DBSInterface,JobSubmitter >> /tmp/canaryfile
    prodAgentd --status >> /tmp/canaryfile
 fi

 /bin/mail -s "$SUBJ" "$TO" < /tmp/canaryfile

 rm /tmp/canaryfile

sleep 900

done
