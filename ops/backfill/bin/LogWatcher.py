#! /usr/bin/env python

# Helper classes

# Format
bright = 1
dim = 2
underline = 4
blink = 5
reverse = 7
hidden = 8

# Forground
black = 30
red = 31
green = 32
yellow = 33
blue = 34
magenta = 35
cyan = 36
white = 37

# Background
onblack = 40
onred = 41
ongreen = 42
onyellow = 43
onblue = 44
onmagenta = 45
oncyan = 46
onwhite = 47

def AnsiFormat(message, *args):
    return "\033[%sm%s\033[0m" % (";".join(["%s" % f for f in args]), message)
 
import os, time

def SendEmail(recipients, message):
    mail = '/bin/mail' 
    pipe = os.popen('%s -s "LogWatcher Alarm (noreplay)" "%s" ' % (mail, recipients), "w")
    pipe.write(message)
    sts = pipe.close()

class ReverseFile(object):
    """Iterate backwards through a file. f should be an open file handle"""
    def __init__(self, f):
        self._f = f
        self.end = os.stat(f.name).st_size

    def __iter__(self): return self

    def next(self):
        if self.end == 0:
            raise StopIteration

        pos = self.end-2
        while pos >= 0:
            self._f.seek(pos)
            if self._f.read(1) == '\n':
                end = self.end
                self.end = pos
                return self._f.read(end - pos - 1)
            pos -= 1

        end = self.end
        self.end = 0
        self._f.seek(0)
        return self._f.read(end).strip("\n")

# Main

from optparse import OptionParser

usage = "usage: %prog [options]"
parser = OptionParser()
parser.add_option('-i', '--interval', action='store', type='float', dest='interval', default=600.,
                  help="interval of time to search in the logs (default 600s)")
parser.add_option('-r', '--red_pattern', action='store', dest='red', default='Error|JobFailed|raise',
                  help="re pattern to search in the logs (defaut 'Error|JobFailed|raise')")
parser.add_option('-b', '--blue_pattern', action='store', dest='blue', default=None,
                  help="re pattern to search in the logs")
parser.add_option('-c', '--components', action='store', dest='components', default=None,
                  help="re pattern to search between components")
parser.add_option('-n', '--nocolor', action='store_true', dest='nocolor', default=False,
                  help="print the output with colors")
parser.add_option('-m', '--mail', action='store', dest='mail', default=None,
                  help="list of recipients separated by comma")
parser.add_option('-a', '--acknowledge', action='store_true', dest='acknowledge', default=False,
                  help="acknowledge those components without any matching")
parser.add_option('-l', '--loop', action='store_true', dest='loop', default=False,
                  help="continous monitoring in period given by 98% of interval")

(options, args) = parser.parse_args()

import re

workDir = os.environ['PRODAGENT_ROOT'] + "/workdir"

while 1:
  gresults = ''
  present = time.localtime()
  for dir in os.listdir( workDir ):
    if options.components:
      if not re.search(options.components, dir): 
        continue 
    file = workDir + "/" + dir + '/ComponentLog'
    if os.path.isfile(file):
      results = ''
      uresults = ''
      component = 'Component %s based time %s (interval %-.1f s)' % (dir, time.strftime('%Y-%m-%d %H:%M:%S', present), options.interval)
      fileHandler = open(file, 'r')
      for line in ReverseFile(fileHandler):
        timeString = line.split(',')[0]
        try:
          timeStamp = time.strptime(timeString,'%Y-%m-%d %H:%M:%S')
        except ValueError:
          continue 
        if time.mktime(present) - time.mktime(timeStamp) < options.interval:
          if re.search(options.red, line, re.I):
            if results == '': 
               results = results + AnsiFormat(line, white, onred, bright)
               uresults = uresults + line
            else:
               results = results + '\n' + AnsiFormat(line, white, onred, bright)
               uresults = uresults + '\n' + line
          if options.blue:
            if re.search(options.blue, line, re.I):
              if results == '':
                results = results + AnsiFormat(line, white, onblue, bright)
                uresults = uresults + '\n' + line
              else:
                results = results + '\n' + AnsiFormat(line, white, onblue, bright)
                uresults = uresults + '\n' + line
        else:
          break
      if results != '' and not options.nocolor:
        print AnsiFormat(component, red, blink) 
        print results
      if uresults != '':
        gresults = gresults + component + '\n' + uresults + '\n'
        if options.nocolor:
          print component
          print uresults + '\n'  
      elif options.acknowledge:
        print component + AnsiFormat(' OK', green, bright)
  if options.mail and gresults != '':
    SendEmail(options.mail, gresults) 
  if options.loop:
    time.sleep(options.interval * 0.98)
    print
  else:
    break

