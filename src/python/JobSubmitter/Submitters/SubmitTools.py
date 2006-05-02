#!/usr/bin/env python
"""
_SubmitTools_

Standard tools for wrapping a job for submission and execution

"""

__revision__ = "$Id:$"

import os
from popen2 import Popen4

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

    pop = Popen4(tarComm)
    while pop.poll() == -1:
            exitCode = pop.poll()
    exitCode = pop.poll()

    if exitCode:
        msg = "Error creating Tarfile:\n"
        msg += tarComm
        msg += "Exited with code: %s\n" % exitCode
        msg += pop.fromchild.read()
        raise RuntimeError, msg
    return tarballFile


