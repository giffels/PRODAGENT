#! /bin/bash

. scripts/OptionParser.sh


# Description of the script
Abstract \${0##*/} - Create a standard workflow for backfill jobs.


# Description of how to use the script
Usage \${0##*/} "-i,-s,-a,-v [-x]"


# Defining the parameters to be parsed
OptionWithArgument i shifter_initials shifter_initials 'Shifter initials.'
OptionWithArgument s site site 'Site associated to the workflow.'
OptionWithArgument a acquisition_era acquisition_era 'Acquisition era (1,2,test01,test02).'
Option x create create 'Create the workflow.'


# Generating the option parser
GenerateParser "$@"

if [ -z $shifter_initials ] || [ -z $site ] || [ -z $acquisition_era ] 
then
    Usage
fi

# KH, replacing incremental version with timestamp
# Reading/Updating version map
#. scripts/VersionMapHelpers.sh
#if [ -f etc/VersionStatus ]
#then
#    ReadVersionMap ${site}_${acquisition_era}
#fi
#UpdateVersionMap ${site}_${acquisition_era}
processing_version=backfill-`date "+%s"`

# Workflow directory
workflow_dir=workflows/$site/$acquisition_era

# make sure the path to the cfg files is setup
if [[ "x${CFG_PATH}" == "x" ]]; then 
    echo "Please set your CFG_PATH first ...";
    exit 1;
fi;

# Lookup table to the site information
. etc/BackfillSiteConfiguration.sh

if ! $site
then
    echo 'Unkown site, check BackfillSiteConfiguration.sh'
    exit 1
fi

    # setup CMSSW ...

    if [[ "x${CMSSW_VERSION}" == "x" ]]; then 
      echo "Please set up your CMSSW environment first ...";
      exit 1;
    fi;

    # setup PA ...
    if [[ "x${PRODAGENT_ROOT}" == "x" ]]; then
      echo "Please set up your prodAgent environment first ...";
      exit 1;
    fi;

    str="python2.4 ${PRODAGENT_ROOT}/util/createProcessingWorkflow.py \
     --py-cfg=${cfg} \
     --version=$version \
     --category=backfill \
     --dataset=${dataset} \
     --dbs-url=http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet \
     --split-type=file \
     --split-size=1 \
     --only-sites=$site \
     --acquisition_era=$acquisition_era \
     --activity=backfill \
     --processing_version=$processing_version \
     --group=DataOps \
     --workflow_tag=$shifter_initials"
    
    if [ "x${onlyblocks}" != "x" ]; then
	str="${str} --only-blocks='${onlyblocks}'"
    fi;   


# Check for creating a job
if [ "$create" == "true" ]
then

    echo $str
    eval $str

    # replacing with timestamp
    #WriteVersionMap

    if [ -d $workflow_dir ]
    then
        mv *.xml $workflow_dir
    else
        mkdir -p $workflow_dir
        mv *.xml $workflow_dir
    fi

else

    echo $str       

fi

