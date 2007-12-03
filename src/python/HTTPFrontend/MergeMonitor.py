#!/usr/bin/env python
"""
_MergeMonitor_

CherryPy handler for displaying the merge status of a dataset based
on information from the merge sensor db

"""

from MergeSensor.MergeCrossCheck import MergeSensorCrossCheck
from MergeSensor.MergeCrossCheck import listAllMergeDatasets

class MergeDatasetMonitor:
    """
    _MergeDatasetMonitor_

    Generate a list of datasets and display them as links to the
    MergeMonitor for that dataset

    """
    def __init__(self, mergeMonitorUrl = None):
        self.mergeMon = mergeMonitorUrl
    
    def index(self):
        html = """<html><body><h2>MergeSensor Datasets</h2>\n """
        html += "<ul>\n"
        for dataset in listAllMergeDatasets():
            html += "<li><a href=\"%s/?dataset=%s\">%s</a></li>\n" % (
                self.mergeMon, dataset, dataset)
        html += "</ul>\n"
        html += """</body></html>"""
        return html
    index.exposed = True

class MergeMonitor:
    """
    _MergeMonitor_

    Display LFN mappings and site info for the list of unmerged files
    in a dataset
    
    """
    def index(self, dataset):
        
        xCheck = MergeSensorCrossCheck(dataset)
        files = xCheck.getFileMap()
        sites = xCheck.getBlocksMap()

        html = """<html><body><h2>MergeSensor State for """
        html += "Dataset %s</h2>\n " % dataset
        html += "<table>\n"
        
        html += "<tr>"
        html += "<th>Unmerged File</th><th>Merged File</th><th>Site</th>"
        html += "</tr>\n"


        for unmrg, mrg in files.items():
            site = sites.get(unmrg, "Unknown")
            html += "<tr>"
            html += "<td>%s</td><td>%s</td><td>%s</td>" % (unmrg, mrg, site)
            html += "</tr>\n"
        
        
        html += "</table>\n"
        html += """</body></html>"""
        return html
    index.exposed = True
     
