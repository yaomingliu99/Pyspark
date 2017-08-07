###################  Spark Machine Learning 
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.ml.param import Param, Params
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

%reset

sc=SparkContext()
spark=SparkSession(sc)

# TODO - Change this directory to the right location where the data is stored
dataDir = "D:/Yaoming/Learn/Python/Pyspark/data/"

# Create the the RDD by reading the wine data from the disk
lines = sc.textFile(dataDir + "winequality-red.csv")
lines.take(10)
splitLines = lines.map(lambda l: l.split(";"))
splitLines.take(2)

# Vector is a data type with 0 based indices and double-typed values. 
# In that there are two types namely dense and sparse.
# A dense vector is backed by a double array representing its entry values
# A sparse vector is backed by two parallel arrays: indices and values

wineDataRDD = splitLines.map(lambda p: (float(p[11]), Vectors.dense([float(p[0]),\
                                       float(p[1]), float(p[2]), float(p[3]),\
                                       float(p[4]), float(p[5]), float(p[6]),\
                                       float(p[7]), float(p[8]),float(p[9]), float(p[10])])))

# Create the data frame containing the training data having two columns. 
# 1) The actula output or label of the data 
# 2) The vector containing the features

trainingDF = spark.createDataFrame(wineDataRDD, ['label', 'features'])