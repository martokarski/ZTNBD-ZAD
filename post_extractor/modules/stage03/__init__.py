from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Sentences transformer
from textblob.blob import TextBlob

NAME = "SENTENCE_TRANSFORMER"


class PipelineTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def extract_sentences(data):
            sentences = []
            for post in data:
                for sentence in TextBlob(post).sentences:
                    sentences.append(str(sentence))
            return sentences

        ext_sentn = udf(extract_sentences, ArrayType(StringType()))
        return dataframe.withColumn(out_col, ext_sentn(in_col))
