#!/usr/bin/env python
"""
_Channel_

Implements an RSS channel

"""

__revision__ = "$Id: Channel.py,v 1.3 2007/02/26 18:02:01 ckavka Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "Carlos.Kavka@ts.infn.it"

import os
import socket
from time import localtime, strftime, time
from xml.dom.minidom import Document
from xml.dom import minidom
from xml.dom.ext import PrettyPrint
from shutil import copyfile, move

##############################################################################
# Channel class
##############################################################################

class Channel:
    """
    _Channel_

    An RSS channel

    """

    ##########################################################################
    # RSS channel initialization
    ##########################################################################

    def __init__(self, name = 'ProdAgent', fromFile = False):
        """

        Arguments:

          name -- name of the channel
          fromFile -- true if it has to initialize itself from a file 

        Return:

          none

        """

        # get configuration parameters
        self.prodAgentInstance = self.__class__.prodAgentInstance
        self.itemListLength = int(self.__class__.itemListLength)
        self.port = self.__class__.port
        self.baseDirectory = self.__class__.baseDirectory
        self.channelDirectory = os.path.join(self.baseDirectory, \
                                             name)
        self.filesDirectory = os.path.join(self.channelDirectory, 'files')
        self.webFilesDirectory = os.path.join('/files', name, 'files')

        # get host ip
        self.ipAddress = str(socket.gethostbyname(socket.gethostname()))

        # create directores if necessary
        if not os.path.exists(self.filesDirectory):
            os.makedirs(self.filesDirectory)
        if not os.path.exists(self.channelDirectory):
            os.makedirs(self.channelDirectory)

        # verify it it has to reload itself from a file
        if fromFile:

            # reload, generating exception if there are problems
            path = os.path.join(self.baseDirectory, name + '.xml') 
            doc = minidom.parse(path)
            channel = doc.getElementsByTagName('channel')[0]

        else:

            # create empty channel
            doc = Document()

            # Create the <rss> base element
            rss = doc.createElement("rss")
            rss.setAttribute("version", "2.0")
            doc.appendChild(rss)

            # Create the <channel>
            channel = doc.createElement("channel")
            rss.appendChild(channel) 

            # Create the <title>
            title = doc.createElement("title")
            channel.appendChild(title)
            text = doc.createTextNode("RSS Channel: " + \
                                  name)
            title.appendChild(text)
       
            # Create the <link>
            link = doc.createElement("link")
            channel.appendChild(link)
            text = doc.createTextNode("http://" + self.ipAddress + ":" + \
                                      str(self.port) + \
                                      "/" + str(name) + ".xml")
            link.appendChild(text)

            # Create the <description>
            description = doc.createElement("description")
            channel.appendChild(description)
            text = doc.createTextNode("Information about the PA instance: " + \
                                      str(self.prodAgentInstance) )
            description.appendChild(text)
 
            # Create the <generator>
            generator = doc.createElement("generator")
            channel.appendChild(generator)
            text = doc.createTextNode("RssFeeder component")
            generator.appendChild(text)

        # for debugging
        # print doc.toprettyxml(indent="  ")

        # store name, document and channel
        self.name = name
        self.doc = doc
        self.channel = channel
 
        # counter to provide unique key to messages produced at the
        # same time
        self.counter = 0

        # make changes persistent
        self.store()

    ##########################################################################
    # RSS channel add item
    ##########################################################################

    def addItem(self, author, title, description, link = None):
        """

        Arguments:

          author -- the author of the item. 
          title -- the title of the item.
          description -- the item synopsis.
          link -- optional URL 

        Return:

          none

        """

        # create item entry
        item = self.doc.createElement("item")
        self.channel.appendChild(item)

        # add title
        titleNode = self.doc.createElement("title")
        item.appendChild(titleNode)
        text = self.doc.createTextNode(title)
        titleNode.appendChild(text)

        # add author
        authorNode = self.doc.createElement("author")
        item.appendChild(authorNode)
        text = self.doc.createTextNode(author)
        authorNode.appendChild(text)

        # add text
        descriptionNode = self.doc.createElement("description")
        item.appendChild(descriptionNode)
        text = self.doc.createTextNode(description)
        descriptionNode.appendChild(text)

        # add publication date
        publicationNode = self.doc.createElement("pubDate")
        item.appendChild(publicationNode)
        date = strftime("%a, %d %b %Y %H:%M:%S %Z", localtime())
        text = self.doc.createTextNode(date)
        publicationNode.appendChild(text) 

        # add guid   
        guidNode = self.doc.createElement("guid")
        guidNode.setAttribute("isPermaLink", "false")
        item.appendChild(guidNode)
        guid = str(time()) + '.' + str(self.counter)
        text = self.doc.createTextNode(guid)
        guidNode.appendChild(text)

        # copy link if provided or create one in other case
        if link is not None:
            linkName = self.processLink(link, title, author)
        else:
            linkName = self.createLink(guid, author, description)

        # add link
        linkNode = self.doc.createElement("link")
        item.appendChild(linkNode)
        text = self.doc.createTextNode(linkName)
        linkNode.appendChild(text)
        
        # count the number of items
        items = self.doc.getElementsByTagName('item')

        # verify limit 
        if len(items) > self.itemListLength:

            # get all file names
            files = [item.getElementsByTagName('link')[0].firstChild.data \
                     for item in items]

            # for all extra items
            numberOfItemsToRemove = len(items) - self.itemListLength 

            for item in range(numberOfItemsToRemove):

                # verify if oldest file is not used in newer messages 
                oldest = files[item]

                if oldest not in files[(item + 1):]:

                    # yes, build real file name from virtual space
                    baseName = os.path.basename(oldest)
                    path = os.path.join(self.filesDirectory, baseName)

                    # remove it (file should be there...)
                    try:
                        os.remove(path)
                    except OSError:
                        pass 
 
                # remove oldest item from XML structure
                parentNode = items[item].parentNode
                parentNode.removeChild(items[item])

        # increment counter
        self.counter = (self.counter + 1) % 1000

        # for debugging
        # print self.doc.toprettyxml(indent="  ")

        # make changes persistent
        self.store()

    ##########################################################################
    # process a link
    ##########################################################################

    def processLink(self, link, title, author):
        """

        Arguments:

          link -- the link name
          title -- the title
          author -- the author of the message

        Return:

          the link

        """

        # check for URL
        if link.startswith('http://'):

            # fine, return it
            return link

        # process text file
        if link.endswith('.txt'):
        
            # read the file
            aFile = open(link, 'r')
            text = aFile.read()
            aFile.close()

            # convert newlines
            text = text.replace('\n','<br>\n')

            # build html document
            document = "<html>\n<body>\n" + \
                   "<h2>Message from " + str(author) + "</h2>\n" + \
                   "<h3>" + title + "</h3>\n<hr>\n" + \
                   "<p>\n" + text + "\n<p>\n</body>\n</html>\n"

            # save it
            fileName = os.path.basename(link).replace('.txt', '.html')
            targetPath = os.path.join(self.filesDirectory, fileName)
            aFile = open(targetPath, 'w')
            aFile.write(document)
            aFile.close()

        # different type of file
        else:

            # copy file into  local area (can generate exception IOError)
            fileName = os.path.basename(link)
            targetPath = os.path.join(self.filesDirectory, fileName)
            copyfile(link, targetPath)

        # return the name of the file in channel area
        return 'http://' + self.ipAddress + ':' + str(self.port) + \
               os.path.join(self.webFilesDirectory, fileName)

    ##########################################################################
    # create a link
    ##########################################################################

    def createLink(self, name, sender, text):
        """

        Arguments:

          name -- the file name
          text -- the text to include 

        Return:

          the link

        """

        document = "<html>\n<body>\n" + \
                   "<h2>Message from " + sender + "</h2>\n" + \
                   "<p>\n" + str(text) + "\n<p>\n</body>\n</html>\n"

        targetPath = os.path.join(self.filesDirectory, name + '.html')

        aFile = open(targetPath, 'w')
        aFile.write(document)
        aFile.close()

        # return the name of the file in channel area
        return 'http://' + self.ipAddress + ':' + str(self.port) + \
               os.path.join(self.webFilesDirectory, name + '.html')

    ##########################################################################
    # store channel in file
    ##########################################################################

    def store(self, path = None):
        """

        Arguments:

          path -- the path where to store the channel

        Return:

          none

        """

        # select default target directory if not specified
        if path is None:
            path = self.baseDirectory

        # define target path (temporary path)
        targetPath = os.path.join(path, self.name + '.xml-tmp')

        # write into file
        aFile = open(targetPath, 'w')
        PrettyPrint(self.doc, aFile)
        aFile.close()

        # rename the temporary file
        move(targetPath, targetPath.replace('.xml-tmp', '.xml'))

    ##########################################################################
    # setters for class variables
    ##########################################################################

    @classmethod
    def setProdAgentInstance(cls, name):
        """set ProdAgent instance name"""
        cls.prodAgentInstance = name

    @classmethod
    def setItemListLength(cls, length):
        """set the maximum length of the item list"""
        cls.itemListLength = length

    @classmethod
    def setPort(cls, port):
        """set the feed port"""
        cls.port = port

    @classmethod
    def setBaseDirectory(cls, path):
        """set the base directory"""
        cls.baseDirectory = path



