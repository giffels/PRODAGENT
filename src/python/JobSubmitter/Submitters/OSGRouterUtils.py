#!/usr/bin/env python
"""
_OSGUtils_

Common utils for OSG jobs

"""

import os


def makeErrorReportScript(jobSpecId, wfspecid = None):
    
    scriptBase = \
    """
      echo '<FrameworkJobReport JobSpecID=\"%s\" Name=\"cmsRun1\" WorkflowSpedID=\"%s\" Status=\"Failed\">' > FrameworkJobReport.xml
      echo '<ExitCode Value=\"60999\"/>' >> FrameworkJobReport.xml
      echo '<FrameworkError ExitStatus=\"60999\" Type=\"NoSpaceOnDevice\">' >> FrameworkJobReport.xml
      echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
      echo "  site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
      echo "</FrameworkError>"  >> FrameworkJobReport.xml
      echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
    """ % (jobSpecId,  wfspecid)
    return scriptBase

def standardScriptHeader(jobSpecId, wfSpecId, minDiskSize=1500000, black_hole_delay=1200):
    """
    _standardScriptHeader_

    Standard Submission script header for OSG Jobs

    """
    script = []
    script.append("echo Starting up OSG prodAgent job\n")
    script.append("echo hostname: `hostname -f`\n")
    script.append("echo site: $OSG_SITE_NAME\n")
    script.append("echo gatekeeper: $OSG_JOB_CONTACT\n")
    script.append("echo pwd: `pwd`\n")
    script.append("echo date: `date`\n")
    script.append("echo df output:\n")
    script.append("df\n")

    # Source OSG_GRID after the above debugging output in case the 'source'
    # fails and crashes our script.
    script.append("if [ -d \"$OSG_GRID\" ]; then\n")
    script.append("   echo OSG_GRID: $OSG_GRID\n")
    script.append("   source $OSG_GRID/setup.sh\n")
    script.append("fi\n")
    script.append("echo env output:\n")
    script.append("env\n")

    script.append("start_time=`date '+%s'`\n")
    script.append("WrapupAndExit() {\n")
    script.append("  now=`date '+%s'`\n")
    script.append("  sleep_time=$(($start_time-$now+" + str(black_hole_delay) + "))\n")
    script.append("  if [ $sleep_time -gt 0 ]; then\n")
    script.append("    echo Delaying exit for $sleep_time seconds to avoid rapid failures.\n")
    script.append("    sleep $sleep_time\n")
    script.append("  fi\n")
    script.append("  exit $1\n")
    script.append("}\n\n")
    script.append("\n")

    script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
    script.append("echo PRODAGENT_JOB_INITIALDIR = $PRODAGENT_JOB_INITIALDIR\n")
    script.append("MIN_DISK=%s\n" % minDiskSize) 
    script.append("DIRS=\"$OSG_WN_TMP $_CONDOR_SCRATCH_DIR\"\n")
    #Insert by Ajit 6/6/07
    #If _CONDOR_SCRATCH_DIR and OSG_WN_TMP are on same partition,
    #use _CONDOR_SCRATCH_DIR, because it is guaranteed to be cleaned up.
    script.append("if [ \"$_CONDOR_SCRATCH_DIR\" != \"\" ]; then\n")
    script.append("  OSG_WN_TMP_partition=`stat -c \"%D\" $OSG_WN_TMP`\n")
    script.append("  _CONDOR_SCRATCH_DIR_partition=`stat -c \"%D\" $_CONDOR_SCRATCH_DIR`\n")
    script.append("  if [ \"$OSG_WN_TMP_partition\" != \"\" ] && [ \"$OSG_WN_TMP_partition\" = \"$_CONDOR_SCRATCH_DIR_partition\" ]; then\n")
    script.append("    DIRS=\"$_CONDOR_SCRATCH_DIR\"\n")
    script.append("  fi\n")
    script.append("fi\n")
    #End Insert
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
    script.append(makeErrorReportScript(jobSpecId, wfSpecId))
    script.append("  WrapupAndExit 1\n")
    script.append("fi\n")
    #script.append("echo CHOSEN_WORKDIR: `$CHOSEN_WORKDIR`\n")
    script.append("echo CHOSEN_WORKDIR: \"$CHOSEN_WORKDIR\"\n") 
    script.append("cd $CHOSEN_WORKDIR\n")
    
    return script


bulkUnpackerScriptMain = \
"""

echo "===Available JobSpecs:==="
/bin/ls `pwd`/BulkSpecs
echo "========================="


JOB_SPEC_FILE="`pwd`/BulkSpecs/$JOB_SPEC_NAME"

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
   echo "<FrameworkJobReport JobSpecID=\"$JOB_SPEC_NAME\" Status=\"Failed\">" > FrameworkJobReport.xml
   echo '<ExitCode Value="60998"/>' >> FrameworkJobReport.xml
   echo '<FrameworkError ExitStatus="60998" Type="MissingJobSpecFile">' >> FrameworkJobReport.xml
   echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
   echo "  site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
   echo "  jobspecfile=$JOB_SPEC_FILE " >> FrameworkJobReport.xml
   echo "  available_specs=`/bin/ls ./BulkSpecs` " >> FrameworkJobReport.xml
   echo "</FrameworkError>"  >> FrameworkJobReport.xml
   echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
   WrapupAndExit 60998
fi


"""



def bulkUnpackerScript(bulkSpecTarName):
    """
    _bulkUnpackerScript_

    Unpacks bulk spec tarfile, searches for required spec passed to
    script as argument $1

    If file not found, it generates a failure report and exits
    Otherwise, JOB_SPEC_FILE will be set to point to the script
    to invoke the run.sh command
    
    """
    lines = [
        "JOB_SPEC_NAME=$1\n", 
        "BULK_SPEC_NAME=\"$PRODAGENT_JOB_INITIALDIR/%s\"\n" % os.path.basename(bulkSpecTarName),
        "echo \"This Job Using Spec: $JOB_SPEC_NAME\"\n",
        "tar -zxf $BULK_SPEC_NAME\n",
        ]
    lines.append(bulkUnpackerScriptMain)
    return lines


missingRepScript = \
"""
if [ -e $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml ]; then 
   echo "FrameworkJobReport exists for job: $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml"
else 
   echo "ERROR: No FrameworkJobReport produced by job!!!"
   echo "Generating failure report..."
   echo "<FrameworkJobReport JobSpecID=\"$JOB_SPEC_NAME\" Status=\"Failed\">" > FrameworkJobReport.xml
   echo '<ExitCode Value=\"60997\"/>' >> FrameworkJobReport.xml
   echo '<FrameworkError ExitStatus=\"60997\" Type=\"JobReportMissing\">' >> FrameworkJobReport.xml
   echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
   echo "  site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
   echo "</FrameworkError>"  >> FrameworkJobReport.xml
   echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
   /bin/cp -f ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR
   WrapupAndExit 60997
fi


"""
   
def missingJobReportCheck(jobName):
    """
    _missingJobReportCheck_

    If no FrameworkJobReport file exists at job completion,
    generate one

    """
    
    lines = [
        "export JOB_SPEC_NAME=\"%s\"\n" % jobName,
        ]
    lines.append(missingRepScript)
    return lines


