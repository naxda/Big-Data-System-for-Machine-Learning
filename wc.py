import re
import sys
from pyspark import SparkConf,SparkContext

def get_union(x,y):
	return list(set(x)|set(y))

conf = SparkConf()
sc = SparkContext(conf=conf)
lines = sc.textFile(sys.argv[1])
words = lines.flatMap(lambda l: re.split(r'[^\w]+',l))
#words = lines.flatMap(lambda l: re.spl)
filtered = words.filter(lambda w:len(w)>0 and w[0].isalpha())
pairs = filtered.map(lambda w: (w[0].lower(),[w]))
lists = pairs.reduceByKey(lambda n1, n2: get_union(n1,n2))
counts = lists.map(lambda (f,l):(f,len(l)))

#counts.saveAsTextFile(sys.argv[2])
counts.coalesce(1).saveAsTextFile(sys.argv[2])

sc.stop()
