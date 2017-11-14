from pyspark import SparkContext

sc = SparkContext("local", "Hello Witold")
text_file = sc.parallelize(['lorem lipsum dolor sit amet', 'ein enim lorem sit'])
counts = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)
    
print counts.take(20)