#!/usr/local/bin/bash

cd $MYTESTAREA/PRODAGENT

rm -rf $PRODAGENT_WORKDIR/mysqldata
rm -rf $PRODAGENT_WORKDIR/BOSS

mkdir -p $PRODAGENT_WORKDIR/mysqldata
mkdir -p $PRODAGENT_WORKDIR/BOSS

mysql_install_db --datadir=$PRODAGENT_WORKDIR/mysqldata

nohup mysqld_safe --datadir=$PRODAGENT_WORKDIR/mysqldata --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock --skip-networking --log-error=$PRODAGENT_WORKDIR/mysqldata/error.log --pid-file=$PRODAGENT_WORKDIR/mysqldata/mysqld.pid > /dev/null 2>&1 < /dev/null &

sleep 6

mysqladmin -u root password '98passwd' --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock

prodAgent-install-db

#This is a magic line that is necessary only if you want to use JobQueue/ResourceMonitor setup
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --new --site=CERN --ce-name=none --se-name=srm.cern.ch --processing-threshold=500 --min-submit=50 --max-submit=100

#once that's in,you can dynamically edit the sql via:
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=repackThreshold --value=1000
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=minimumSubmission --value=10
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=maximumSubmission --value=500
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=processingThreshold --value=1000
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=cleanupThreshold --value=200
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=mergeThreshold --value=500
#see https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgentResourceControlDB  for full documentation
