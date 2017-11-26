import json

import pyspark.sql
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

# from modules.posts import SentenceTransformer, PostTransformer, TranslateTransformer
from module_loader import TransformerModuleManager


def generate_column_names(initial, intermediate_count, final):
    columns = ["__col_{:02d}".format(idx) for idx in range(intermediate_count)]
    columns.insert(0, initial)
    columns.append(final)
    return columns


if __name__ == "__main__":
    sc = pyspark.SparkContext('local[*]', 'PipelineFlow')
    sess = pyspark.sql.SparkSession(sc)
    rdd = sc.wholeTextFiles('data/*')
    rdd = rdd.map(lambda x: (x[0], json.loads(x[1])))
    print(type(rdd.take(1)[0][1][0]))
    schema = StructType([
        StructField('file', StringType(), True),
        StructField('content', ArrayType(MapType(StringType(), StringType())), True)
    ])
    df = sess.createDataFrame(rdd, schema)

    trans_manager = TransformerModuleManager("modules")
    print("Available transformers' names: {}".format(", ".join(trans_manager.loaded_transformers_names)))

    loaded_transformers = trans_manager.loaded_transformers
    col_names = generate_column_names("content", len(loaded_transformers) - 1, "sentences")

    stages = list(map(
        lambda idx, transformer: transformer().setInputCol(col_names[idx]).setOutputCol(col_names[idx + 1]),
        range(len(loaded_transformers)), loaded_transformers)
    )

    pipeline = Pipeline(stages=stages)
    out = pipeline.fit(df).transform(df)
    a = out.select('sentences').first().sentences[0]
    b = out.select('sentences').first().sentences[1]
    c = out.select('sentences').first().sentences[2]
    # d = out.select('translated').first().translated[0]

    print('{}\n\n{}\n\n{}'.format(a, b, c))
