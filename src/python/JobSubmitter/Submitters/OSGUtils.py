#!/usr/bin/env python
"""
_OSGUtils_

Common utils for OSG jobs

"""


def makeErrorReportScript(jobSpecId):

    scriptBase = \
    """
      echo "<FrameworkJobReport Name=\"%s\" Status=\"Failed\">" > FrameworkJobReport.xml
      echo "<ExitCode Value=\"60999\"/>" >> FrameworkJobReport.xml
      echo "<FrameworkError ExitStatus=\"60999\" Type=\"NoSpaceOnDevice\">" >> FrameworkJobReport.xml
      echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
      echo "  site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
      echo "</FrameworkError>"  >> FrameworkJobReport.xml
      echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
    """ % jobSpecId
    return scriptBase

def standardScriptHeader(jobSpecId, minDiskSize=1500000):
    """
    _standardScriptHeader_

    Standard Submission script header for OSG Jobs

    """
    script = []
    script.append("if [ -d \"$OSG_GRID\" ]; then\n")
    script.append("   source $OSG_GRID/setup.sh\n")
    script.append("fi\n")
    script.append("echo Starting up OSG prodAgent job\n")
    script.append("echo hostname: `hostname -f`\n")
    script.append("echo site: $OSG_SITE_NAME\n")
    script.append("echo gatekeeper: $OSG_JOB_CONTACT\n")
    script.append("echo pwd: `pwd`\n")
    script.append("echo date: `date`\n")
    script.append("echo df output:\n")
    script.append("df\n")
    script.append("echo env output:\n")
    script.append("env\n")
    script.append("\n")
    script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
    script.append("MIN_DISK=%s\n" % minDiskSize) 
    script.append("DIRS=\"$OSG_WN_TMP $_CONDOR_SCRATCH_DIR\"\n")
    script.append("for dir in $DIRS; do\n")
    script.append("  space=`df $dir | tail -1 | awk '{print $4}'`\n")
    script.append("  if [ \"$space\" -gt $MIN_DISK ]; then \n")
    script.append("     CHOSEN_WORKDIR=$dir\n")
    script.append("     break\n")
    script.append("  fi\n")
    script.append("done\n")
    script.append("if [ \"$CHOSEN_WORKDIR\" = \"\" ]; then\n")
    script.append("  echo Insufficient disk space: Found no directory with $MIN_DISK kB in the following list: $DIRS\n")
    script.append("  touch FrameworkJobReport.xml\n")
    script.append(makeErrorReportScript(jobSpecId))
    script.append("  exit 1\n")
    script.append("fi\n")
    #script.append("echo CHOSEN_WORKDIR: `$CHOSEN_WORKDIR`\n")
    script.append("echo CHOSEN_WORKDIR: \"$CHOSEN_WORKDIR\"\n") 
    script.append("cd $CHOSEN_WORKDIR\n")
    
    return script
