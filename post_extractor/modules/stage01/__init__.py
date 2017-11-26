from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

NAME = "POST TRANSFORMER"


class PipelineTransformer(Transformer, HasInputCol, HasOutputCol):
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def get_content(data):
            contents = [doc.get('content') for doc in data]
            return contents

        get_cntn = udf(get_content, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))
