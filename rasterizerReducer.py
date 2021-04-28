#!/usr/bin/env python

from defs import *
from ast import literal_eval as lit
import shapely.wkt as wkt
import sys

# for each pixel x y,z, output a single pixel per x y coordinate
# which one?
currentcoords = None
bestcolor = None
extremities = []
for line in sys.stdin:
  if line[:3] == "maxx":
    extremities.append(line)
    continue
  key, val = line.split("\t")
  try:
    val = int(val)
  except:
    continue
  if key == currentcoords:
    if val > bestcolor or bestcolor == None:
      bestcolor = val
  else:
    print '%s\t%s' % (currentcoords, bestcolor)
    currentcoords = key
    bestcolor = val
if currentcoords == key:
  print '%s\t%s' % (currentcoords, bestcolor)
for i in extremities:
  print i
