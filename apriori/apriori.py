from pyspark import SparkContext
import os
import _apriori


os.environ["SPARK_HOME"] = "D:\Documents\Downloads\spark-2.0.1-bin-hadoop2\spark-2.0.1-bin-hadoop2.7"

sc=SparkContext(appName="apriori")
lines=sc.textFile('service.txt')

data=lines.map(lambda line:line.split('\n'))
data1=data.map(lambda line: line[0].split(' ')[:-1])
print _apriori.apriori(data1.collect(),0.2)

