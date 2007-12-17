#!/usr/bin/env python
"""
_MergeMonitor_

CherryPy handler for displaying the merge status of a dataset based
on information from the merge sensor db

"""
import os

from MergeSensor.MergeCrossCheck import MergeSensorCrossCheck
from MergeSensor.MergeCrossCheck import listAllMergeDatasets

class MergeDatasetMonitor:
    """
    _MergeDatasetMonitor_

    Generate a list of datasets and display them as links to the
    MergeMonitor for that dataset

    """
    def __init__(self, mergeMonitorUrl = None, graphMonUrl = None, datasetUrl = None):
        self.mergeMon = mergeMonitorUrl
        self.graphmon = graphMonUrl
        self.datasetmon = datasetUrl
    
    def index(self):
        html = """<html><body><h2>MergeSensor Datasets</h2>\n """
        html += "<table>\n"
        html += "<tr><th>Dataset Name</th><th>File list</th><th> Graphs</th><th>Local DBS</th></tr>\n"
        for dataset in listAllMergeDatasets():
            html += "<tr><td>%s</td><td><a href=\"%s/?dataset=%s\">Files</a></td><td><a href=\"%s?dataset=%s\">Graph</a></td><td><a href=\"%s?dataset=%s\">DBS</td></tr>\n" % (
                dataset, self.mergeMon, dataset, self.graphmon, dataset,
                self.datasetmon, dataset)
        html += "</table>\n"
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


        html += "<h4>Files undergoing merging</h4>\n"
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

        html += "<h4>New Unmerged files</h4>\n"
        html += "<table>\n"
        html += "<tr><th>LFN</th></tr>\n"
        for lfn in xCheck.getPendingUnmergedFiles():
            html += "<tr><td>%s</td></tr>\n" % lfn
        html += "</table>\n"
        html += """</body></html>"""
        return html
    index.exposed = True
     

class MergeGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index(self, dataset):

        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml
            
        xCheck = MergeSensorCrossCheck(dataset)
        files = xCheck.getFileMap()
        newFiles = xCheck.getPendingUnmergedFiles()

        totalNew = len(newFiles)

        if (len(newFiles) == 0) and (len(files) == 0):
            html = "<html><body>No files for dataset: %s</body></html>" % (
                dataset,)
            return html
            
        totalNotMerged = 0
        for unmrg, mrg in files.items():
            if mrg.strip() == "":
                totalNotMerged += 1
                
        
        totalMerged = len(files) - totalNotMerged

        datasetPng = dataset.replace("/", "", 1)
        datasetPng = datasetPng.replace("/", "-")
        datasetPng = "MergeGraph-%s.png" % datasetPng
        pngfile = os.path.join(self.workingDir, datasetPng)
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, datasetPng)
        
        
        data = { "new" : totalNew,
                 "merging" : totalNotMerged,
                 "merged" : totalMerged }
        
        metadata = {'title':'Merge Status for %s' % dataset}
        pie = PieGraph()
        coords = pie.run( data, pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
        return html

        
    index.exposed = True
