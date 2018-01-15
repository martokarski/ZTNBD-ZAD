# coding=utf-8
import json
import statistics

from abc import ABC, abstractmethod

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol, HasOutputCol, Param
)
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType, StringType
)


class BaseFeatureTransformer(Transformer, HasInputCol, HasOutputCol, ABC):

    @keyword_only
    def __init__(self, **kwargs):
        super().__init__()
        self.features = Param(self, "features", "")
        self._setDefault(features=set())
        self.set_params(**self._input_kwargs)

    @keyword_only
    def set_params(self, **kwargs):
        return self._set(**self._input_kwargs)

    def set_features(self, value):
        self._paramMap[self.features] = value
        return self

    def get_features(self):
        return self.getOrDefault(self.features)

    @abstractmethod
    def _transform(self, dataframe):
        pass


class MaxFeaturesTransformer(BaseFeatureTransformer):
    """
    Klasa MaxFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza średnie wartości dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących
    się w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy
    double’i.
    """
    def _transform(self, dataframe):

        features = self.get_features()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def feature_collect(data):
            features_dict = {}

            for feature in features:
                features_dict[feature] = 0
            lines = data.splitlines(keepends=False)

            for line in lines:
                json_line = json.loads(line)
                feature_array = json_line.get('features')

                for element in feature_array:
                    name = element.get('name')
                    if name in features_dict:
                        features_dict[name] = max(features_dict[name],element.get('value'))

            values = []
            for feature in features:
                values.append(features_dict[feature])
            return values

        get_cntn = udf(feature_collect, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))


class MeanFeaturesTransformer(BaseFeatureTransformer):
    """
    Klasa MeanFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza średnie wartości dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących
    się w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy
    double’i.
    """
    def _transform(self, dataframe):

        features = self.get_features()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def feature_collect(data):
            features_dict = {}
            count_dict = {}

            for feature in features:
                features_dict[feature] = 0
                count_dict[feature] = 0
            lines = data.splitlines(keepends=False)

            for line in lines:
                json_line = json.loads(line)
                feature_array = json_line.get('features')

                for element in feature_array:
                    name = element.get('name')
                    if name in features_dict:
                        features_dict[name] += element.get('value')
                        count_dict[name] += 1

            values = []
            for feature in features:
                if count_dict[feature] != 0:
                    values.append(features_dict[feature] / count_dict[feature])
                else:
                    values.append(0)
            return values

        get_cntn = udf(feature_collect, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))


class MedianFeaturesTransformer(BaseFeatureTransformer):
    """
    Klasa MedianFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza medianę dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących się
    w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy double’i.
    """
    def _transform(self, dataframe):

        features = self.get_features()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def feature_collect(data):
            features_dict = {}
            count = 0

            for feature in features:
                features_dict[feature] = []
            lines = data.splitlines(keepends=False)

            for line in lines:
                count += 1
                json_line = json.loads(line)
                feature_array = json_line.get('features')

                for element in feature_array:
                    name = element.get('name')
                    if name in features_dict:
                        features_dict[name].append(element.get('value'))

            values = []
            for feature in features:
                if not features_dict[feature]:
                    values.append(0)
                else:
                    values.append(statistics.median(features_dict[feature]))
            return values

        get_cntn = udf(feature_collect, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))


class NumberOfOccurrencesFeaturesTransformer(BaseFeatureTransformer):
    """
    Klasa MedianFeaturesTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol,
    pyspark.ml.param.shared.HasOutputCol. Klasa ta przyjmuje dodatkowy parametr features, który zawiera listę nazw cech,
    które mają zostać zagregowane. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda
    ta wylicza medianę dla cech podanych w parametrze features, ze wszystkich obiektów jsonowych znajdujących się
    w tekście zawartym w kolumnie inputCol. Wyliczone wartości wstawia do kolumny outputCol w postaci listy double’i.
    """
    def _transform(self, dataframe):

        features = self.get_features()
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        def feature_collect(data):
            features_dict = {}
            count = 0

            for feature in features:
                features_dict[feature] = 0
            lines = data.splitlines(keepends=False)

            for line in lines:
                count += 1
                json_line = json.loads(line)
                feature_array = json_line.get('features')

                for element in feature_array:
                    name = element.get('name')
                    if name in features_dict:
                        features_dict[name] += 1

            values = []
            for feature in features:
                values.append(features_dict[feature])
            return values

        get_cntn = udf(feature_collect, ArrayType(StringType()))
        return dataframe.withColumn(out_col, get_cntn(in_col))
