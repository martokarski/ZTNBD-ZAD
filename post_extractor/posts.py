# coding=utf-8
import json
import ast

from textblob import TextBlob
from textblob.exceptions import NotTranslated, TranslatorError

from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, MapType, IntegerType, StructType, StructField, DoubleType


class PostTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa PostTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta wydobywa z kolumny inputCol, z formatu json, content i umieszcza go w kolumnie outputCol w postaci tekstu.
    """
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


class TranslateTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa TranslateTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta tłumaczy tekst zawarty w kolumnie inputCol  z języka polskiego na angielski i umieszcza go w kolumnie
    outputCol.
    """
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


class SentenceTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SentenceTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta dzieli tekst zawarty w kolumnie inputCol  na zdania i umieszcza go w kolumnie outputCol w postaci
    tablicy tekstów.
    """
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

class SpeechPartsTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SpeechPartsTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta z tekstu zawartego w kolumnie inputCol  zlicza wystąpienie części mowy i wstawia do outputCol w postaci
    jsona.
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def tags_sum_by_key(tags):
            types = {}

            for tag in tags:
                if tag[1] in types:
                    types[tag[1]] += 1
                else:
                    types[tag[1]] = 1

            return types
        
        def extract_speech_parts(data):
            tags = []
            for post in data:
                tags.extend(TextBlob(post).tags)
            speech_parts = tags_sum_by_key(tags)
            return speech_parts

        ext_speech_parts = udf(extract_speech_parts, MapType(StringType(), IntegerType()))
        return dataframe.withColumn(out_col, ext_speech_parts(in_col))

class SentimentTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa SentimentTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta z tekstu zawartego w kolumnie inputCol  wylicza sentiment i wstawia do kolumny outputCol.
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def calculate_sentiment(data):
            sentiments = []
            for post in data:
                sentiments.append(TextBlob(post).sentiment)
            return sentiments

        cnt_senti = udf(calculate_sentiment, ArrayType(
            StructType(
                [StructField("polarity", DoubleType()),
                 StructField("subjectivity", DoubleType())])
            )
        )
        return dataframe.withColumn(out_col, cnt_senti(in_col))

class BasicSpeechPartsTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Klasa BasicSpeechPartsTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe.
    Metoda ta z tekstu zawartego w kolumnie inputCol zlicza wystąpienie podstawowych części mowy (rzeczownik, czasownik, przymiotnik)
    i wstawia do outputCol w postaci tablicy wartości.
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataframe):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def tags_sum_by_key(tags):
            types = {}

            for tag in tags:
                if tag[1] in types:
                    types[tag[1]] += 1
                else:
                    types[tag[1]] = 1

            return types
        
        def extract_speech_parts(data):
            tags = []
            for post in data:
                tags.extend(TextBlob(post).tags)
            speech_parts = tags_sum_by_key(tags)
            basic_speech_parts = [speech_parts.get("NN",0)+speech_parts.get("NNS",0)+speech_parts.get("NNP",0)+
                                      speech_parts.get("NNPS",0), #nouns
                                  speech_parts.get("VB",0)+speech_parts.get("VBD",0)+speech_parts.get("VBG",0)+
                                      speech_parts.get("VBN",0)+speech_parts.get("VBP",0)+speech_parts.get("VBZ",0), #verbs
                                  speech_parts.get("JJ",0)+speech_parts.get("JJR",0)+speech_parts.get("JJS",0)] #adjectives
            return basic_speech_parts

        ext_speech_parts = udf(extract_speech_parts, ArrayType(IntegerType()))
        return dataframe.withColumn(out_col, ext_speech_parts(in_col))