import sys
from pyspark import SparkConf, SparkContext

#Initialize Spark application
conf = SparkConf().setAppName("Lab_5")
sc = SparkContext(conf = conf)

#Input/Output folder and prefix string(the values are specified by means of three input parameters)
inputPath  = sys.argv[1] 
outputPath = sys.argv[2] 
prefix = sys.argv[3] 

'TASK 1'
#Read data
inputFile = sc.textFile(inputPath)
#Filter word that start with chosen prefix
RDDwordFreq = inputFile.filter(lambda line: line.startwith(prefix))

#Count the number of filtered lines and max frequency
numberLines = RDDwordFreq.count()
maxFreq = RDDwordFreq.reduce(lambda e1,e2 : max(e1,e2))

print("Number of Lines = "+str(numberLines)+" & max_freq = "+str(maxFreq))

#Save the results in the output path
RDDResult = sc.parallelize([("Number of Lines = "+str(numberLines)+" & max_freq = "+str(maxFreq))])
RDDResult.saveAsTextFile(outputPath)
