#!/usr/bin/env python
"""
_DirSize_

Auxilary methods to detmerine the size of a directory in python.

"""


import os
from os.path import *


def convertSize(bytes,units):
   """
   converts the size from bytes to KB or MB
   """
   if units == 'k':
	return float(float(bytes) / float(1024))
   elif units == 'm':
	return float(float(bytes) / float(1024) / float(1024))
   else:
 	return bytes

def dirSize(start, follow_links, my_depth, max_depth):
   """
   recursively determinse the size of a dir.
   -start: the dir path
   -follow_links: follow symbolic links or not
   -my_depth: on what level do you start
   -max_depth: the maximum depth (0 equals all)
   """

   total = 0L
   try:
	dir_list = os.listdir (start)
   except:
	if isdir (start):
		print 'Cannot list directory %s' % start
	return 0
   for item in dir_list:
	path = '%s/%s' % (start, item)
	try:
		stats = os.stat (path)
	except:
		print 'Cannot stat %s' % path
		continue
	total += stats[6]
	if isdir (path) and (follow_links or \
		(not follow_links and not islink (path))):
		bytes = dirSize(path, follow_links, my_depth + 1, max_depth)
		total += bytes
		if (my_depth < max_depth):
			print_path (path, bytes)
   return total

# main area
follow_links = 0
depth = 0
path='/home/fvlingen/tmp/PRODAGENT'

bytes = dirSize(path, follow_links, 0, depth)
print convertSize(bytes,'m')


