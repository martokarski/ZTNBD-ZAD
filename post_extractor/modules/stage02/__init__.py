from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from textblob.blob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError

NAME = "TRANSLATE TRANSFORMER"


class PipelineTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def translate(p):
            try:
                return str(TextBlob(p).translate(from_lang='pl'))
            except (NotTranslated, TranslatorError) as e:
                return 'Translation error'

        def process(data):
            translated = [translate(p) for p in data]
            return translated

        process = udf(process, ArrayType(StringType()))
        return dataframe.withColumn(out_col, process(in_col))
