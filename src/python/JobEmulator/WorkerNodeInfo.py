"""
_WorkerNodeInfo_

Dictionary based container for information of a worker node.

"""
__revision__ = "$Id: "
__version__ = "$Revision: "

class WorkerNodeInfo(dict):
    """
    _WorkerNodeInfo_

    Dictionary based container for information of a worker node.

    """
    def __init__(self):
        dict.__init__(self)
        
        self.setdefault("SiteName", None)
        self.setdefault("HostID", None)
        self.setdefault("HostName", None)
        self.setdefault("se-name", None)
        self.setdefault("ce-name", None)
        
    def updateWorkerNodeInfo(self, workerNodeInfo):
        
        self.update(workerNodeInfo)