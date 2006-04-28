#!/usr/bin/env python
"""
_ScramSetupTools_

Common tools for generating scram setup commands for jobs using
the standard CMS error codes

"""

#  //
# // Standard check for CMS_VO_SW_DIR command
#//
_StandardCMSVOSWDIR = \
"""
# Check CMS_VO_SW_DIR is set
if [ -n "$VO_CMS_SW_DIR" ]; then
   echo "VO_CMS_SW_DIR is set to $VO_CMS_SW_DIR"
else
   echo "ERROR: VO_CMS_SW_DIR is not set"
   prodAgentFailure 10030
fi

"""

#  //
# // Standard Exit Code Check for Scram Env setup command
#//
_StandardSetupExitCheck = \
"""
# Check command exit code is zero
if [ $? -ne 0 ]; then
   echo "ERROR: Scram Setup Command failed"
   prodAgentFailure 10032
else
   echo "Scram Environment setup OK"
fi 
"""

def setupScramEnvironment(command):
    """
    _setupScramEnvironment_

    Wrap the initialisation command to actually setup the scram
    command environment.

    Eg commands like:
    At FNAL:  . /uscmst/prod/sw/cms/setup/bashrc
    On LCG:   . $VO_CMS_SW_DIR/cmsset_default.sh

    etc.

    If this command fails, then a standard error code is reported
    to the prodAgentFailure command which will abort the job with
    the appropriate error

    """
    result = ["# Scram Setup Command:"]
    if command.find("VO_CMS_SW_DIR") != -1:
        #  //
        # // Dependency on VO_CMS_SW_DIR: Check it explicitly
        #//
        result.append(_StandardCMSVOSWDIR)

    #  //
    # // Call the command and check its exit code
    #//
    result.append(command)
    result.append(_StandardSetupExitCheck)
    #  //
    # // Format the command
    #//
    commandString = ""
    for item in result:
        if not item.endswith("\n"):
            item += "\n"
        commandString += item
    return commandString


#  //
# // Check Exit code of scram project command
#//
_StandardScramProjCheck = \
"""
   # Check Scram Project Exit Code
   if [ $? -ne 0 ]; then
      echo "ERROR: Scram Project Command failed"
      prodAgentFailure 10035
   else
      echo "Scram Project Command Completed"
   fi

"""
    
def scramProjectCommand(projectName, projectVersion, scramCommand = "scramv1"):
    """
    _scramProjectCommand_

    Generate a scram project <project> <version> command call using the
    arguments provided.

    Reports errors with standard CMS Exit codes
    Does not run if exit.status already exists indicating an earlier
    failure

    """
    #  //
    # // Scram Project Command first
    #//
    result = ["# Scram Project Command"]
    result.append("if [ -e ./exit.status ]; then ")
    result.append("   echo \"exit.status has been found\"")
    result.append("   echo \"This indicates a setup command has failed\"")
    result.append("   echo \"Skipping Scram Project command\"")
    result.append("else")
    result.append( 
        "   %s project %s %s" % (scramCommand, projectName, projectVersion)
        )
    result.append(_StandardScramProjCheck)
    result.append("fi")

    #  //
    # // Scram Runtime Command: Conditional on project directory
    #//  existing
    #  //
    # // Note: Cant check exit status of scram ru command since it exists
    #//  in an eval. Need to add seperate non eval call as check in future??
    result.append("if [ -e ./exit.status ]; then ")
    result.append("   echo \"exit.status has been found\"")
    result.append("   echo \"This indicates a setup command has failed\"")
    result.append("   echo \"Skipping Scram Runtime command\"")
    result.append("else")
    result.append("   # Scram Runtime Command")
    result.append("   if [ -e \"./%s\" ]; then" % projectVersion)
    result.append("      cd %s" % projectVersion)
    result.append("      eval `scramv1 runtime -sh`")
    result.append("      cd ..")
    result.append("   else")
    result.append(
        "      echo \"Scram Project Dir not found for scram runtime\"")
    result.append("      echo \"EXITING WITH STATUS: 10036\"")
    result.append("      prodAgentFailure 10036")
    result.append("   fi")
    result.append("fi")
    

    #  //
    # // Format the command
    #//
    commandString = ""
    for item in result:
        if not item.endswith("\n"):
            item += "\n"
        commandString += item
    return commandString
    
