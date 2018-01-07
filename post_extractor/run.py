import json

import pyspark
import pyspark.sql
from pyspark.ml import Pipeline

from modules.posts import SentenceTransformer, PostTransformer, TranslateTransformer


sc = pyspark.SparkContext('local[*]', 'PipelineFlow')
sess = pyspark.sql.SparkSession(sc)
rdd = sc.wholeTextFiles('data/*')
rdd = rdd.map(lambda x: (x[0], json.loads(x[1])))
df = rdd.toDF(['file', 'content'])

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
