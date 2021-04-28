#!/usr/bin/env python

from pyspark import *
from defs import *
import zlib
import binascii
import sys

#assuming this reads each line as a [key, val] pair
for line in sys.stdin:
  #print("data: length: {}, head: {}, tail: {}".format(len(line), line[:50], line[-50:]))
  #print(line)
  if True: #line.find("OSMData") <= 5:
    #datastart = line.find("', '") + 4
    linedata = bytes(line[14:-3])
    blobstr = binascii.unhexlify(linedata)
    blob = Blob()
    blob.ParseFromString(blobstr)
    entities = parseBlob(blob)
    #realblob = PrimitiveBlock()
    #realblob.ParseFromString(zlib.decompress(blob.zlib_data))
    #nodecount = len(realblob.primitivegroup[0].dense.lat)
    #print '%s\t%s' % ("entitycount: ", nodecount)
    for entity in entities:
      #map way
      if entity["type"] == "way":
        #map wayNodes to their nodeID
        tag = "highway"

        #ordinal - probably wrong
        #tagOrdinal = -1
        #for i in range(len(entity["keys"])):
        #  if entity["keys"][i] == tag:
        #    tagOrdinal = i
        #wayNodeVals = (entity.id, tagOrdinal)
        wayNodeVals = {"type": "node", "wayID": entity["id"], "ordinal": -1}
        ordinal = 0
        for nodeID in entity["refs"]:
          wayNodeVals["ordinal"] = ordinal
          ordinal += 1
          print '%s\t%s' % (nodeID, wayNodeVals)
      
        #map way to its wayID
        wayID = entity["id"]
        wayVals = {"type": "way", "keys": entity["keys"], "vals": entity["vals"]}
        print '%s\t%s' % (wayID, wayVals)
  
      #map node
      elif entity["type"] == "node":
        nodeID = entity["id"]
        nodeVals = {"type": "node", "lat": entity["lat"], "lon": entity["lon"]}
        print '%s\t%s' % (nodeID, nodeVals)
    
      else:
        print '%s\t%s' % ("error: ", entity)
