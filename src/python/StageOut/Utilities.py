#!/usr/bin/env python
"""
_Utilities_


Misc utils for stage out operations

"""
__version__ = "$Revision$"
__revision__ = "$Id$"


from IMProv.IMProvLoader import loadIMProvString
from IMProv.IMProvQuery import IMProvQuery


def extractStageOutFor(cfgStr):
    """
    _extractStageOutFor_

    Get the list of node names to stage out files for in this job

    """
    if len(cfgStr.strip()) == 0:
        return []

    try:
        config = loadIMProvString(cfgStr)
    except Exception, ex:
        # Not an XML formatted string
        return []

    query = IMProvQuery("/StageOutConfiguration/StageOutFor[attribute(\"NodeName\")]")
    nodelist = query(config)
    result = [ str(x) for x in nodelist if x.strip() != "" ]
    return result



def extractRetryInfo(cfgStr):
    """
    extractRetryInfo

    Extract retry configuration settings

    """
    result = {
        "NumberOfRetries" : 3,
        "RetryPauseTime" : 600,
        }

    if len(cfgStr.strip()) == 0:
        return result

    try:
        config = loadIMProvString(cfgStr)
    except Exception, ex:
        # Not an XML formatted string
        return result

    query = IMProvQuery("/StageOutConfiguration/NumberOfRetries[attribute(\"Value\")]")
    vals = query(config)
    if len(vals) > 0:
        value = vals[-1]
        value = int(value)
        result['NumberOfRetries'] = value

    query = IMProvQuery("/StageOutConfiguration/RetryPauseTime[attribute(\"Value\")]")
    vals = query(config)
    if len(vals) > 0:
        value = vals[-1]
        value = int(value)
        result['RetryPauseTime'] = value
    return result


def extractStageOutOverride(cfgStr):
    """
    _extractStageOutOverride_

    Extract an Override configuration from the string provided

    """
    if len(cfgStr.strip()) == 0:
        return {}

    try:
        override = loadIMProvString(cfgStr)
    except Exception, ex:
        # Not an XML formatted string
        return {}

    commandQ = IMProvQuery("/StageOutConfiguration/Override/command[text()]")
    optionQ = IMProvQuery("/StageOutConfiguration/Override/option[text()]")
    seNameQ = IMProvQuery("/StageOutConfiguration/Override/se-name[text()]")
    lfnPrefixQ = IMProvQuery("/StageOutConfiguration/Override/lfn-prefix[text()]")


    command = commandQ(override)
    if len(command) == 0:
        return {}
    else:
        command = command[0]

    seName = seNameQ(override)
    if len(seName) == 0:
        return {}
    else:
        seName = seName[0]

    lfnPrefix = lfnPrefixQ(override)
    if len(lfnPrefix) == 0:
        return {}
    else:
        lfnPrefix = lfnPrefix[0]


    option = optionQ(override)
    if len(option) > 0:
        option = option[0]
    else:
        option = None

    return {
        "command" : command,
        "se-name" : seName,
        "lfn-prefix" : lfnPrefix,
        "option" : option,
        }



