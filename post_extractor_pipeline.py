from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import re
import json
from post_extractor.modules.posts import *
from pyspark.ml import Pipeline

dataFolder = str(sys.argv[1])
if not dataFolder.endswith("/"):
    dataFolder += "/"

print("Using {} as data root".format(dataFolder))

sconf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('PipelineFlow')

sc = SparkContext.getOrCreate(sconf)
sess = SparkSession(sc)

mappings = sc.textFile(dataFolder + 'mapping.csv') \
    .map(lambda line: line.split('|')) \
    .toDF(['path', 'key'])

def extractKey(entry):
    fpath, content = entry
    fname = fpath.split('/')[-1]
    key = re.sub('\.json$', '', fname)
    return (key, json.loads(content)) #json parsing should be part of transformer code
statements = sc.wholeTextFiles(dataFolder + 'statement/') \
    .map(extractKey) \
    .toDF(['key', 'content'])
    
df = mappings.join(statements, 'key')

poster = PostTransformer().setInputCol('content').setOutputCol('posts')
translator = TranslateTransformer().setInputCol('posts').setOutputCol('translated')
sentencer = SentenceTransformer().setInputCol('translated').setOutputCol('sentences')

pipeline = Pipeline(stages=[poster, translator, sentencer])
out = pipeline.fit(df).transform(df)
a = out.select('sentences').first().sentences[0]
b = out.select('sentences').first().sentences[1]
c = out.select('sentences').first().sentences[2]
d = out.select('translated').first().translated[0]

print('{}\n\n{}\n\n{}\n\n{}'.format(a,b,c,d))