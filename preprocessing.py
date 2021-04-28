from pyspark import *
from fileformat_pb2 import *
from defs import bintoint32
import time
import binascii

start_time = time.time()
country = "georgia"

f = open("../../data/{}-latest.osm.pbf".format(country), "rb")

#spark setup
conf = SparkConf()
conf.setMaster('local')
conf.setAppName('osmPreprocessing')
sc = SparkContext(conf = conf)

blobs = []
blocksRead = 0
bytesRead = 0
outFileNum = 0
while True:
  print("block #{}".format(blocksRead))
  blocksRead += 1
  hlen = f.read(4)
  bytesRead += 4
  try:
    hlenint = bintoint32(hlen)
  except:
    break

  bheader = BlobHeader()
  bheader.ParseFromString(f.read(hlenint))
  bytesRead += hlenint

  rawblob = f.read(bheader.datasize)
  hexblob = binascii.hexlify(rawblob)
  #blob = Blob()
  #blob.ParseFromString(f.read(bheader.datasize))
  #zdata = blob.zlib_data
  bytesRead += bheader.datasize
  if bheader.type == "OSMData":
    blobrecord = (bheader.type, hexblob)
    blobs.append(blobrecord)

  if bytesRead > 30000000:
    print("byte limit reached! outputting file #{}".format(outFileNum))
    rdd = sc.parallelize(blobs)
    rdd.saveAsTextFile("hdfs://10.10.1.247:9000/preprocessed/{}/{}{}".format(country, country, outFileNum))
    rdd = None
    blobs = []
    bytesRead = 0
    outFileNum += 1

print("bytes read: {}".format(bytesRead))
print("EOF reached! outputting file #{}".format(outFileNum))
rdd = sc.parallelize(blobs)
rdd.saveAsTextFile("hdfs://10.10.1.247:9000/preprocessed/{}/{}{}".format(country, country, outFileNum))
end_time = time.time()

print("time taken: {}".format(end_time - start_time))
