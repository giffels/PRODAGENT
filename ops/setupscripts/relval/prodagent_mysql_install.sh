#!/usr/local/bin/bash

cd $MYTESTAREA/PRODAGENT

rm -rf $PRODAGENT_WORKDIR/mysqldata
rm -rf $PRODAGENT_WORKDIR/BOSS

mkdir -p $PRODAGENT_WORKDIR/mysqldata
mkdir -p $PRODAGENT_WORKDIR/BOSS

# Install the MySQL database:
echo "Installation of the MySQL database. It uses the information from the ProdAgentConfig.xml for its configuration"

#if [ "$PAVERSION" == "0_11_0" -o "$PAVERSION" == "0_11_3"  ]; then
	prodAgent-edit-config --component=ProdAgentDB --parameter=dbType --value=mysql
#	echo "Updating config file..."
#	sleep 3
#fi

mysql_install_db --datadir=$PRODAGENT_WORKDIR/mysqldata

nohup mysqld_safe --datadir=$PRODAGENT_WORKDIR/mysqldata --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock --skip-networking --log-error=$PRODAGENT_WORKDIR/mysqldata/error.log --pid-file=$PRODAGENT_WORKDIR/mysqldata/mysqld.pid > /dev/null 2>&1 < /dev/null &

sleep 6

mysqladmin -u root password '98passwd' --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock

sleep 2

prodAgent-install-db

#These are magic lines that are necessary only if you want to use JobQueue/ResourceMonitor setup
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --new --site=CERN --ce-name=none --se-name=srm.cern.ch --processing-threshold=500 --min-submit=1 --max-submit=50 #--merge-threshold=100


#once that's in,you can dynamically edit the sql via:
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=processingThreshold --value=1000
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=cleanupThreshold --value=200
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=mergeThreshold --value=100
python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=CERN --set-threshold=logcollectThreshold --value=100 #Added on 26/08/2008 (Diego)
#see https://twiki.cern.ch/twiki/bin/view/CMS/ProdAgentResourceControlDB  for full documentation

# Modify the ProdAgentConfig.xml for putting the correct name of the host and choosing an unused port in the HTTPFrontend block:
prodAgent-edit-config --component=HTTPFrontend --parameter=Host --value=`hostname` 

port=0; val_ant=8887;
for val in `cat /data/$PRODAGENT_USER/PAProd/*/ProdAgentConfig.xml | grep -A 4 'ConfigBlock Name="HTTPFrontend"' | grep 'Parameter Name="Port"' | cut -d '"' -f 4 | sort -g`; do 
    if [ "`expr $val - $val_ant`" -gt "1" ]; then port=`expr $val_ant + 1`; break; fi;
    val_ant=$val;
done
if [ "$port" -eq "0" ]; then port=`expr $val + 1`; fi
prodAgent-edit-config --component=HTTPFrontend --parameter=Port --value=$port


#Install the BOSS schema:
if [ "$INST_TYPE" == "LCG" ]; then
    if [[ "$RUN_SITE" == *FNAL* ]]; then
	# Added on 26/08/2008, creating a new jdl file!
	echo "Requirements = (other.GlueCEPolicyMaxCPUTime>=1200) && (RegExp(\"fnal.gov\", other.GlueCEUniqueId)) ;" > $MYTESTAREA/../jdl_reqs.jdl
        python $MYTESTAREA/PRODAGENT/util/resourceControl.py --new --site=FNAL --ce-name=cmsosgce.fnal.gov/jobmanager-condor --se-name=cmssrm.fnal.gov --processing-threshold=500 --min-submit=1 --max-submit=50 --merge-threshold=100
        prodAgent-edit-config --component=RelValInjector --parameter=SitesList --value=cmssrm.fnal.gov
    #MIT won't be used anymore
    #elif [[ "$RUN_SITE" == *MIT* ]]; then 
        #python $MYTESTAREA/PRODAGENT/util/resourceControl.py --new --site=MIT --ce-name=ce01.cmsaf.mit.edu/jobmanager-condor --se-name=se01.cmsaf.mit.edu --processing-threshold=500 --min-submit=1 --max-submit=50 --merge-threshold=100
        #prodAgent-edit-config --component=RelValInjector --parameter=SitesList --value=se01.cmsaf.mit.edu
    fi
    python $MYTESTAREA/PRODAGENT/util/resourceControl.py --new --site=Default --ce-name=Default --se-name=Default --processing-threshold=500 --min-submit=1 --max-submit=50 --merge-threshold=100
    python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=Default --set-threshold=cleanThreshold --value=10

    #Added on 21.08.2008
    python $MYTESTAREA/PRODAGENT/util/resourceControl.py --edit --site=Default --set-threshold=logcollectThreshold --value=100

    echo "Installation of the BOSS schema. It uses the information from the ProdAgentConfig.xml for its configuration"
    prodAgent-edit-config --component=JobTracking --parameter=BOSSDIR --value=$MYTESTAREA/$SCRAM_ARCH/cms/boss/$BOSS_VERSION
    prodAgent-edit-config --component=JobTracking --parameter=BOSSVERSION --value=$BOSS_VERSION
    prodAgent-edit-config --component=BOSS --parameter=rtDomain --value=`hostname`
    prodAgent-edit-config --component=BOSS --parameter=rtHost --value=`hostname`
    bossConfigFiles
    prodAgent-install-boss-db
fi


