import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from numpy import linalg as LA
import matplotlib.pyplot as plt

def parseData(line):
  data = line.split(",")
  index= data[:2] # the index number, this number is not equal to row number
  temp = data[2:]
  features = []
  for i in temp:
    if i == "":
      features.append(0)
    else:
    
      features.append(float(i.replace("T","").replace("-", "")))
  return [index, np.array(features)]

""" 
multiply matrix A by vector x
"""


 

if __name__ == "__main__":
    # set up environment
  conf = SparkConf()
  conf.setAppName("columns_reduction")
  sc = SparkContext(conf=conf)
  inputData = sc.textFile("/home/jiz414/bosch/non_nan_test1.csv")
  header = inputData.first()
  inputData = inputData.filter(lambda x : x != header)
  data = inputData.map(parseData).take(2)
  print data
  totalPoints = data.count()
  print totalPoints
  print data[1:5]
