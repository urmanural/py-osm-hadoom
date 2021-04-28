#!/usr/bin/env python

from defs import *
from ast import literal_eval as lit
import shapely.wkt as wkt
import sys

def printpoint(pt, color): 
  print '%s,%s\t%s' % (int(pt[0]), int(pt[1]), color)

extremities = [0,0,0,0]

# for each (way: tags, geometry), output a pixel(x,y z)
# x,y = lat/lon
# z = pixel color value, based on tags
for line in sys.stdin:
  key, val = line.split("\t")
  try:
    way = lit(val)
  except:
    print '%s\t%s' % (key, "Error: {}".format(val))
    continue

  geom = wkt.loads(way["geometry"])

  roadtype = ""
  for i in range(len(way["keys"])):
    if way["keys"][i] == "highway":
      roadtype = way["vals"][i]
  color = wayMap(roadtype)

  if len(geom.coords) > 1 and color != 0:
    frompt = geom.coords[0]
    for i in range(1, len(geom.coords)):
      topt = geom.coords[i]
      drawline(frompt[0], frompt[1], topt[0], topt[1], 10**6, printpoint, color)
      #pixels = drawline(frompt[0], frompt[1], topt[0], topt[1], 100000, printpoint, color)
      #for o in pixels:
      #  printpoint(o, color)
      frompt = topt
      if frompt[0] > extremities[0]:
        extremities[0] = frompt[0]
      if frompt[1] > extremities[1]:
        extremities[1] = frompt[1]
      if frompt[0] < extremities[2]:
        extremities[2] = frompt[0]
      if frompt[1] < extremities[3]:
        extremities[3] = frompt[1]

print("maxx: {}, maxy: {}, minx: {}, miny: {}".format(extremities[0],extremities[1],extremities[2],extremities[3]))
