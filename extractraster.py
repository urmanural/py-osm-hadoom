from pyspark import *
from fileformat_pb2 import *
from defs import bintoint32, filtercoords, addtoarr
import png
import time
import binascii

start_time = time.time()

conf = {
  "country": "georgia3",
  "minx": 40000,
  "maxx": 50000,
  "miny": 39990,
  "maxy": 47000,
}
filepath = "hdfs://10.10.1.247:9000/rasters/{}/part-00000".format(conf["country"])

#spark setup
sconf = SparkConf()
sconf.setMaster('local')
sconf.setAppName('osmPreprocessing')
sc = SparkContext(conf = sconf)


rasterrdd = sc.textFile(filepath)
#for some reason this treats each row of the 2d array as a pointer to the same underlying 1d array
#rasterarr = [[0]*(conf["maxx"] - conf["minx"])]*(conf["maxy"] - conf["miny"])
#but this doesn't
rasterarr = []
for i in range(conf["maxy"] - conf["miny"]):
  rasterarr.append([0]*(conf["maxx"] - conf["minx"]))

filteredraster = rasterrdd.filter(lambda x: filtercoords(x, conf["minx"], conf["miny"], conf["maxx"], conf["maxy"])).collect()
for line in filteredraster:
  addtoarr(line, rasterarr, conf["minx"], conf["miny"])

# output png image
outpath = "tiles/{}_tile{}to{};{}to{}.png".format(conf["country"], conf["minx"], conf["maxx"], conf["miny"], conf["maxy"])
png.from_array(rasterarr, 'L').save(outpath)


end_time = time.time()
print("time taken: {}".format(end_time - start_time))
