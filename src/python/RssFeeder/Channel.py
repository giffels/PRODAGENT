#!/usr/bin/env python
"""
_Channel_

Implements an RSS channel

"""

__revision__ = "$Id$"
__version__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

from time import localtime, strftime, time
from xml.dom.minidom import Document

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

    def __init__(self, prodAgentInstance, itemListLength, name, port):
        """

        Arguments:

          prodAgentInstance -- name of the PA instance
          itemListLength -- maximum number of items

        Return:

          none

        """

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
        text = doc.createTextNode("Production Agent RSS Channel: " + \
                                  name)
        title.appendChild(text)
       
        # Create the <link>
        link = doc.createElement("link")
        channel.appendChild(link)
        text = doc.createTextNode("http://localhost:" + str(port) + \
                                  "/" + str(name) + ".xml")
        link.appendChild(text)

        # Create the <description>
        description = doc.createElement("description")
        channel.appendChild(description)
        text = doc.createTextNode("Information about the PA instance: " + \
                                  str(prodAgentInstance) )
        description.appendChild(text)
 
        # Create the <generator>
        generator = doc.createElement("generator")
        channel.appendChild(generator)
        text = doc.createTextNode("RssFeeder component")
        generator.appendChild(text)

        # for debugging
        #print doc.toprettyxml(indent="  ")

        # store document and channel
        self.doc = doc
        self.channel = channel
 
        # store maximum length
        self.itemListLength = int(itemListLength)

        # counter to provide unique key to messages produced at the
        # samet time
        self.counter = 0

    ##########################################################################
    # RSS channel add item
    ##########################################################################

    def addItem(self, author, title, description):
        """

        Arguments:

          author -- the author of the item. 
          title -- the title of the item.
          description -- the message text.

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
        item.appendChild(guidNode)
        guid = str(time()) + '.' + str(self.counter)
        text = self.doc.createTextNode(guid)
        guidNode.appendChild(text)

        # count the number of items
        items = self.doc.firstChild.firstChild.childNodes[3:]

        # verify limit 
        if len(items) > self.itemListLength:

            # remove oldest
            self.doc.firstChild.firstChild.removeChild(items[0])

        # increment counter
        self.counter = (self.counter + 1) % 1000

        # for debugging
        #print self.doc.toprettyxml(indent="  ")


