#!/usr/bin/env python
"""
_SubmitTools_

Standard tools for wrapping a job for submission and execution

"""

__revision__ = "$Id: SubmitTools.py,v 1.2 2006/05/02 12:31:14 elmer Exp $"

import os
from subprocess import Popen, PIPE, STDOUT

def createTarball(targetDir, sourceDir, tarballName):
    """
    _createTarball_

    Create a tarball in targetDir named tarballName.tar.gz containing
    the contents of sourceDir.

    Return the path to the resulting tarball

    """
    tarballFile = os.path.join(targetDir, "%s.tar.gz" % tarballName)
    tarComm = "tar -czf %s -C %s %s " % (
        tarballFile,
        os.path.dirname(sourceDir),
        os.path.basename(sourceDir)
        )

    pop = Popen(tarComm, shell = True, output = PIPE, stderr = STDOUT)
    output.pop.communicate()[0]
    exitCode = pop.poll()

    if exitCode:
        msg = "Error creating Tarfile:\n"
        msg += tarComm
        msg += "Exited with code: %s\n" % exitCode
        msg += output
        raise RuntimeError, msg
    return tarballFile


