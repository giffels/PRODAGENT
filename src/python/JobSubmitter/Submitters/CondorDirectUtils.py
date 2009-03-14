#!/usr/bin/env python
"""
_LCGDirectUtils_

Utils for LCG Direct to CE submission

"""

import os


def outOfSpaceReport():
    
    scriptBase = \
    """
      echo "<FrameworkJobReport JobSpecID=\"$JOB_SPEC_NAME\" Name=\"cmsRun1\" WorkflowSpecID=\"$WORKFLOW_SPEC_NAME\" Status=\"Failed\">" > FrameworkJobReport.xml
      echo '<ExitCode Value=\"60999\"/>' >> FrameworkJobReport.xml
      echo '<FrameworkError ExitStatus=\"60999\" Type=\"NoSpaceOnDevice\">' >> FrameworkJobReport.xml
      echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
      echo "  site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
      echo "</FrameworkError>"  >> FrameworkJobReport.xml
      echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
    """
    return scriptBase

def standardOSGScriptHeader(jobSpecId, wfSpecId):
    """
    _standardOSGScriptHeader_

    Standard Submission script header for OSG Jobs

    """
    script = []
    script.append("if [ -d \"$OSG_GRID\" ]; then\n")
    script.append("   source $OSG_GRID/setup.sh\n")
    script.append("fi\n")
    script.append(". $OSG_APP/cmssoft/cms/cmsset_default.sh\n")
    script.append("echo site: $OSG_SITE_NAME\n")
    script.append("echo gatekeeper: $OSG_JOB_CONTACT\n")

    script.append("echo Starting up ProdAgent job\n")
    script.append("echo hostname: `hostname -f`\n")
    script.append("export JOB_SPEC_NAME=%s\n" % jobSpecId)
    script.append("echo job: $JOB_SPEC_NAME\n")
    script.append("export WORKFLOW_SPEC_NAME=%s\n" % wfSpecId)
    script.append("echo request: $WORKFLOW_SPEC_NAME\n") 
    script.append("echo pwd: `pwd`\n")
    script.append("echo date: `date`\n")
    script.append("echo df output:\n")
    script.append("df\n")
    script.append("echo env output:\n")
    script.append("env\n")
    script.append("\n")
    script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
    
    script.append("MIN_DISK=1500000\n") 
    script.append("DIRS=\"$OSG_WN_TMP $_CONDOR_SCRATCH_DIR\"\n")
    script.append("for dir in $DIRS; do\n")
    script.append("  space=`df -P $dir | tail -1 | awk '{print $4}'`\n")
    script.append("  if [ \"$space\" -gt $MIN_DISK ]; then \n")
    script.append("     CHOSEN_WORKDIR=$dir\n")
    script.append("     break\n")
    script.append("  fi\n")
    script.append("done\n")
    script.append("if [ \"$CHOSEN_WORKDIR\" = \"\" ]; then\n")
    script.append("  echo Insufficient disk space: Found no directory with $MIN_DISK kB in the following list: $DIRS\n")
    script.append("  touch FrameworkJobReport.xml\n")
    script.append(outOfSpaceReport())
    script.append("  exit 1\n")
    script.append("fi\n")
    script.append("echo CHOSEN_WORKDIR: \"$CHOSEN_WORKDIR\"\n") 
    script.append("cd $CHOSEN_WORKDIR\n")
    
    
    return script


def standardLCGScriptHeader(jobSpecId, wfSpecId):
    
    """
    _standardLCGScriptHeader_

    Standard Submission script header for LCG Jobs

    """
    script = []
    script.append(". $VO_CMS_SW_DIR/cmsset_default.sh\n")
    script.append("echo Starting up ProdAgent job\n")
    script.append("echo hostname: `hostname -f`\n")
    script.append("echo CE: $GLOBUS_CE\n")
    script.append("export JOB_SPEC_NAME=%s\n" % jobSpecId)
    script.append("echo job: $JOB_SPEC_NAME\n")
    script.append("export WORKFLOW_SPEC_NAME=%s\n" % wfSpecId)
    script.append("echo request: $WORKFLOW_SPEC_NAME\n") 
    script.append("echo pwd: `pwd`\n")
    script.append("echo date: `date`\n")
    script.append("echo df output:\n")
    script.append("df\n")
    script.append("echo env output:\n")
    script.append("env\n")
    script.append("\n")
    script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")

    return script

stdFunctions = \
"""
#===================================================================
#========Standard Tools for retrieving input sandbox via http=======
#===================================================================

SQUID_PROXIES=""

function findProxies(){
  PROXY_FILE=$1
  if [ -e $PROXY_FILE ]; then
     echo "Found $PROXY_FILE"
     OUTPUT=`cat $PROXY_FILE | grep "<proxy url=" $PROXY_FILE | cut -d\\\" -f2`
     SQUID_PROXIES="$OUTPUT"
  else
     echo "File Not Found"
  fi

}


function useProxies(){
  INP=$1
  OUTP=$2
  if [ "$SQUID_PROXIES" = "" ]; then
   echo "No Squid proxies found" 
   return 1
  else
    for PROX in $SQUID_PROXIES
    do 
      echo "Using Proxy: $PROX"
      export http_proxy="$PROX"
      wget --proxy=on -O $OUTP $INP
      EXIT_STATUS=$?
      if [ $EXIT_STATUS -eq 0 ]; then
         return 0
      else
         echo "Failed using Proxy: $PROX"
         /bin/rm $OUTP
      fi
    done
    return 1
  fi
}



function fetchInputFile(){
    INPUT_URL=$1
    OUTPUT_FILE=$2
    echo "File Fetch:"
    echo "  ==> Input=$INPUT_URL"
    echo "  ==> Output=$OUTPUT_FILE"
    useProxies $INPUT_URL $OUTPUT_FILE
    if [ $? -ne 0 ]; then
       echo "Proxy based retrieval failed, using direct retrieval..."
       wget  --proxy=off -O $OUTPUT_FILE $INPUT_URL
       if [ $? -ne 0 ]; then
         /bin/rm $OUTPUT_FILE
       fi
    fi
    if [ -e $OUTPUT_FILE ]; then
        return 0
    fi
    return 1
    
}

echo "Searching for Proxies in $CMS_PATH/SITECONF/local/JobConfig/site-local-config.xml"
findProxies $CMS_PATH/SITECONF/local/JobConfig/site-local-config.xml
echo "Proxies are: $SQUID_PROXIES"


#===================================================================
#========End Standard Tools for retrieving input sandbox via http===
#===================================================================



"""



fetchScript = \
"""

MISSING_INPUT_FILE=0

fetchInputFile $INPUT_SANDBOX_TAR $INPUT_SANDBOX_TAR_BASENAME
if [ $? -ne 0 ]; then
   MISSING_INPUT_FILE=1
fi

if [ "$INPUT_JOBSPEC_FILE" = "None" ];then
   echo "Fetching Input Jobspec sandbox"
   fetchInputFile $INPUT_JOBSPEC_TAR $INPUT_JOBSPEC_TAR_BASENAME
   if [ $? -ne 0 ]; then
        MISSING_INPUT_FILE=1
   fi
else
   echo "Fetching Input Jobspec file"
   fetchInputFile $INPUT_JOBSPEC_FILE $INPUT_JOBSPEC_FILE_BASENAME
   if [ $? -ne 0 ]; then
        MISSING_INPUT_FILE=1
   fi

fi


if [ $MISSING_INPUT_FILE != 0 ]; then
   echo "Unable to proceed without Input Sandbox File"
   echo "<FrameworkJobReport JobSpecID=\"$JOB_SPEC_NAME\"  WorkflowSpecID=\"$WORKFLOW_SPEC_NAME\" Status="Failed">" > FrameworkJobReport.xml
   echo "<ExitCode Value="60996"/>" >> FrameworkJobReport.xml
   echo "<FrameworkError ExitStatus="60996" Type="MissingInputSandboxFile">" >> FrameworkJobReport.xml
   echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
   echo "  input_jobspec=$INPUT_JOBSPEC_TAR " >> FrameworkJobReport.xml
   echo "  input_jobspec_file=$INPUT_JOBSPEC_FILE " >> FrameworkJobReport.xml
   echo "  input_sandbox=$INPUT_SANDBOX_TAR " >> FrameworkJobReport.xml
   echo "</FrameworkError>"  >> FrameworkJobReport.xml
   echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
   exit 60996
fi

if [ "$INPUT_JOBSPEC_FILE" = "None" ];then
    tar -zxf $INPUT_JOBSPEC_TAR_BASENAME

    echo "===Available JobSpecs:==="
    /bin/ls `pwd`/BulkSpecs
    echo "========================="
    JOB_SPEC_FILE="`pwd`/BulkSpecs/$JOB_SPEC_NAME-JobSpec.xml"
else
    JOB_SPEC_FILE="`pwd`/$INPUT_JOBSPEC_FILE_BASENAME"
fi


PROCEED_WITH_SPEC=0

if [ -e "$JOB_SPEC_FILE" ]; then
   echo "Found Job Spec File: $JOB_SPEC_FILE"
   PROCEED_WITH_SPEC=1
else
   echo "Job Spec File Not Found: $JOB_SPEC_NAME"
   PROCEED_WITH_SPEC=0
fi

if [ $PROCEED_WITH_SPEC != 1 ]; then
   echo "Unable to proceed without JobSpec File"
   echo "<FrameworkJobReport JobSpecId=\"$JOB_SPEC_NAME\"  WorkflowSpecID=\"$WORKFLOW_SPEC_NAME\" Status=\"Failed\">" > FrameworkJobReport.xml
   echo "<ExitCode Value="60998"/>" >> FrameworkJobReport.xml
   echo "<FrameworkError ExitStatus="60998" Type="MissingJobSpecFile">" >> FrameworkJobReport.xml
   echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
   echo "  jobspecfile=$JOB_SPEC_FILE " >> FrameworkJobReport.xml
   echo "  available_specs=`/bin/ls ./BulkSpecs` " >> FrameworkJobReport.xml
   echo "</FrameworkError>"  >> FrameworkJobReport.xml
   echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
   exit 60998
fi




"""

unpackSandbox = \
"""
#=====================================================
#=========Unpacking Input Sanbox======================
#=====================================================
echo "Unpacking Input Sandbox..."
echo "Sandbox : `pwd`/$INPUT_SANDBOX_TAR_BASENAME"
tar -zxf $INPUT_SANDBOX_TAR_BASENAME
if [ $? -ne 0 ];then
   echo "Error unpacking Input Sandbox..."
   echo "TODO: Generate failure report for this even though it will bomb later"
fi

"""


def makeOSGScript(job, workflow, cacheUrl, inputSandbox, **options):
    """
    _makeScript_

    Create a script to retrieve an input sandbox over wget

    """
    script = standardOSGScriptHeader(job, workflow)
    script.append(stdFunctions)

    sandboxBasename = os.path.basename(inputSandbox)
    sandboxUrl = "%s/download/?filepath=%s" % (cacheUrl, inputSandbox)

    script.append("INPUT_SANDBOX_TAR=\"%s\"\n" % sandboxUrl)
    script.append("INPUT_SANDBOX_TAR_BASENAME=\"%s\"\n" % sandboxBasename)

    
    if options.has_key("InputJobSpec"):
        inputJobSpec = options['InputJobSpec']
        inputJobSpecBasename = os.path.basename(inputJobSpec)
        inputJobSpecUrl = "%s/download/?filepath=%s" % (cacheUrl, inputJobSpec)
        script.append("INPUT_JOBSPEC_FILE=\"%s\"\n" % inputJobSpecUrl )
        script.append(
            "INPUT_JOBSPEC_FILE_BASENAME=\"%s\"\n" % inputJobSpecBasename)
        script.append("INPUT_JOBSPEC_TAR=\"None\"\n")
        script.append("INPUT_JOBSPEC_TAR_BASENAME=\"None\"\n")
    else:
        inputJobTar = options['InputJobTar']
        inputJobTarBasename = os.path.basename(inputJobTar)
        inputJobTarUrl = "%s/download/?filepath=%s" % (cacheUrl, inputJobTar)
        script.append("INPUT_JOBSPEC_TAR=\"%s\"\n" % inputJobTarUrl )
        script.append(
            "INPUT_JOBSPEC_TAR_BASENAME=\"%s\"\n" % inputJobTarBasename)
        script.append("INPUT_JOBSPEC_FILE=\"None\"\n")
        script.append("INPUT_JOBSPEC_FILE_BASENAME=\"None\"\n")


    script.append(fetchScript)
    script.append(unpackSandbox)
    return script

def makeLCGScript(job, workflow, cacheUrl, inputSandbox, **options):
    """
    _makeScript_

    Create a script to retrieve an input sandbox over wget

    """
    script = standardLCGScriptHeader(job, workflow)
    script.append(stdFunctions)
    
    sandboxBasename = os.path.basename(inputSandbox)
    sandboxUrl = "%s/download/?filepath=%s" % (cacheUrl, inputSandbox)

    script.append("INPUT_SANDBOX_TAR=\"%s\"\n" % sandboxUrl)
    script.append("INPUT_SANDBOX_TAR_BASENAME=\"%s\"\n" % sandboxBasename)

    
    if options.has_key("InputJobSpec"):
        inputJobSpec = options['InputJobSpec']
        inputJobSpecBasename = os.path.basename(inputJobSpec)
        inputJobSpecUrl = "%s/download/?filepath=%s" % (cacheUrl, inputJobSpec)
        script.append("INPUT_JOBSPEC_FILE=\"%s\"\n" % inputJobSpecUrl )
        script.append(
            "INPUT_JOBSPEC_FILE_BASENAME=\"%s\"\n" % inputJobSpecBasename)
        script.append("INPUT_JOBSPEC_TAR=\"None\"\n")
        script.append("INPUT_JOBSPEC_TAR_BASENAME=\"None\"\n")
    else:
        inputJobTar = options['InputJobTar']
        inputJobTarBasename = os.path.basename(inputJobTar)
        inputJobTarUrl = "%s/download/?filepath=%s" % (cacheUrl, inputJobTar)
        script.append("INPUT_JOBSPEC_TAR=\"%s\"\n" % inputJobTarUrl )
        script.append(
            "INPUT_JOBSPEC_TAR_BASENAME=\"%s\"\n" % inputJobTarBasename)
        script.append("INPUT_JOBSPEC_FILE=\"None\"\n")
        script.append("INPUT_JOBSPEC_FILE_BASENAME=\"None\"\n")


    script.append(fetchScript)
    script.append(unpackSandbox)
    return script



missingRepScript = \
"""
if [ -e $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml ]; then 
   echo "FrameworkJobReport exists for job: $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml"
else 
   echo "ERROR: No FrameworkJobReport produced by job!!!"
   echo "Generating failure report..."
   echo "<FrameworkJobReport JobSpecID=\"$JOB_SPEC_NAME\"   WorkflowSpecID=\"$WORKFLOW_SPEC_NAME\" Status=\"Failed\">" > FrameworkJobReport.xml
   echo '<ExitCode Value=\"60997\"/>' >> FrameworkJobReport.xml
   echo '<FrameworkError ExitStatus=\"60997\" Type=\"JobReportMissing\">' >> FrameworkJobReport.xml
   echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
   echo "</FrameworkError>"  >> FrameworkJobReport.xml
   echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
   /bin/cp -f ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR
   echo \"===CUT HERE===\"
   cat FrameworkJobReport.xml >&2
   cat FrameworkJobReport.xml
   exit 60997
fi


"""
   
def missingJobReportCheck():
    """
    _missingJobReportCheck_

    If no FrameworkJobReport file exists at job completion,
    generate one

    """
    return missingRepScript
    

