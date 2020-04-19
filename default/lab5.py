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
RDDwordFreq = inputFile.filter(lambda line: line.startswith(prefix))

#Count the number of filtered lines
numberLines = RDDwordFreq.count()

#Get only the frequencies and choose the max
maxfreqRDD = RDDwordFreq.map(lambda line: float(line.split("\t")[1]))
maxFreq = maxfreqRDD.reduce(lambda e1,e2 : max(e1,e2))

print("Number of Lines = "+str(numberLines)+" & max_freq = "+str(maxFreq))

#Save the results in the output path
RDDResult = sc.parallelize([("Number of Lines = "+str(numberLines)+" & max_freq = "+str(maxFreq))])
RDDResult.saveAsTextFile(outputPath)

'TASK 2'
#Keep only the lines with a frequency freq greater than 0.8*maxfreq
RDDwordsNew = RDDwordFreq.filter(lambda line: float(line.split("\t")[1]) > 0.8*maxFreq)
#Count the number of filtered lines and print in standard output
numberLines2 = RDDwordsNew.count()
print("Number of filtered lines = "+str(numberLines2))

#Save the selected words (without frequency) in an output folder (one word per line)
RDDBestWords = RDDwordsNew.map(lambda line : line.split("\t")[0])
#Save the results in a new output path
outputPath2 = sys.argv[4] 
RDDBestWords.saveAsTextFile(outputPath2)

sc.stop()