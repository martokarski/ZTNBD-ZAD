
# coding: utf-8

# In[1]:


import pyspark
from pyspark import sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
from bs4 import BeautifulSoup
from pyspark.ml.feature import HashingTF, Tokenizer, IDF, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
import sys


# In[2]:


sc = pyspark.SparkContext('local', 'PipelineFlow')


# In[3]:


sess = sql.SparkSession(sc)


# In[4]:


labels = [1, 2, 2]
set_label = udf(lambda i: labels[i], IntegerType())
username = sys.argv[1]
df = sc.wholeTextFiles("hdfs:///user/TZ/%s/ztnbd/*" % (username)) .map(lambda row: (row[0], row[1].replace('\n', ''))) .toDF(['file', 'content']).withColumn('label', set_label(monotonically_increasing_id()))


# In[5]:


df.show()


# In[11]:


import uuid
from pyspark.ml.param.shared import *
from pyspark.ml import Transformer, Pipeline

class PageSplitterTransformer(Transformer, HasInputCol):
    # def __init__(self):
    #     super().__init__()

    def _transform(self, dataset):
        def split_page(row):
            html = row[1]
            parser = BeautifulSoup(html, 'html5lib')
            return (row[0], [t.name for t in parser.body.find_all()], parser.body.getText(), (parser.title.getText()).encode('utf-8').strip())
        
        id_col = str(uuid.uuid1())
        in_col = self.getInputCol()
        source = dataset.withColumn(id_col, monotonically_increasing_id())
        transformed = source.select(id_col, in_col).rdd.map(split_page).toDF([id_col, 'body_tags', 'body_text','title'])
        
        return source.join(transformed, [id_col]).drop(id_col)


# In[12]:


splitter = PageSplitterTransformer().setInputCol('content')


# In[13]:


title_tokenizer = Tokenizer(inputCol = 'title', outputCol = 'title_words')
title_hasher = HashingTF(numFeatures=5, inputCol=title_tokenizer.getOutputCol(), outputCol='title_wc')

title_pipeline = Pipeline(stages=[title_tokenizer, title_hasher])

body_tokenizer = Tokenizer(inputCol = 'body_text', outputCol = 'body_words')
body_hasher = HashingTF(numFeatures=20, inputCol=body_tokenizer.getOutputCol(), outputCol='body_wc')

body_pipeline = Pipeline(stages=[body_tokenizer, body_hasher])

tag_hasher = HashingTF(numFeatures=20, inputCol='body_tags', outputCol='body_tc')
vec_assembler = VectorAssembler(inputCols=['title_wc', 'body_wc', 'body_tc'], outputCol = 'features')
classifier = DecisionTreeClassifier()
    


# In[14]:


pipeline = Pipeline(stages=[splitter, title_pipeline, body_pipeline, tag_hasher, vec_assembler, classifier])


# In[15]:


pipeline.fit(df).transform(df).select('label', 'prediction').show(truncate=False)


# In[16]:


# invalid_pipeline = Pipeline(stages=[splitter, body_hasher, body_tokenizer])


# In[17]:


# invalid_pipeline.fit(df).transform(df)


# In[ ]:




