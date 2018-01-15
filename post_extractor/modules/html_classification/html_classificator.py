from lxml import html
import numpy as np
from collections import Counter
from sklearn.externals import joblib
import os
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

from post_extractor.modules.html_classification.constants import feature_names, classes_names

# Załadowanie drzewa decyzyjnego z pliku - należy rozważyć czy coś takiego jest w ogóle możliwe w Sparku,
# a jeśli nie to spróbować jakoś to obejść
this_dir, _ = os.path.split(__file__)
clf = joblib.load(this_dir + '/clf.pkl')


class HtmlTransformer(Transformer, HasInputCol, HasOutputCol):
    """
    Transformer, który z kolumny inputCol pobiera html (jako string), natomiast zwraca tablicę z 3 wartościami,
    które informują o prawdopodobieństwie istnienia na stronie:
    - innego obiektu
    - pola do przeszukiwania (search bar)
    - formularza logowania (login form)
    Wartości są typu float w przedziale <0.0, 1.0>
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        classify = udf(extract_features, ArrayType(DoubleType()))
        return dataset.withColumn(out_col, classify(in_col))


def extract_features(content):
    tree = html.fromstring(content)
    elements = getNthParentsOfLeafes(tree, 7)
    X = get_X(elements, feature_names)

    i = 0
    predictions = []
    for pred in clf.predict_proba(X):
        t = np.argmax(pred)
        if t != 0:
            predictions.append((classes_names[t], np.max(pred), elements[i]))
        i += 1

    results = [0.0, 0.0, 0.0]
    for (name, value, _) in predictions:
        index = classes_names.index(name)
        if results[index] < value:
            results[index] = value

    return results


def getNthParentsOfLeafes(tree, n):
    """
    :param tree: drzewo html
    :param n: minimalna głębokość poddrzewa
    :return: lista poddrzew utworzonych z podanego drzewa, która posiada conajmniej minimalną głębokość
    """
    return tree.xpath("//*[not(*)]" + ("/.." * n))


def get_X(elements, feature_names):
    """
    :param elements: lista z fragmentami html, które mają zostać poddane klasyfikacji
    :param feature_names: lista cech opisująca badane elementy
    :return: tablica informująca o posiadanych cechach danych elementów
    """
    X = np.zeros((len(elements), len(feature_names)))
    i = 0
    for t in elements:
        el = Counter(get_html_elements(t))
        for k in list(el.keys()):
            try:
                if "Comment" in k:
                    print(k)
            except TypeError:
                print(type(k))
                break
            if k in feature_names:
                X[i][feature_names.index(k)] = el[k]
        el = Counter(getClasses(t))
        for k in list(el.keys()):
            if (len(k) < 1):
                continue
            if k in feature_names:
                X[i][feature_names.index(k)] = el[k]
        el = Counter(getIds(t))
        for k in list(el.keys()):
            if k in feature_names:
                X[i][feature_names.index(k)] = el[k]
        i += 1
    return X


def getIds(tree):
    return tree.xpath("//*[@id]/@id")


def getClasses(tree):
    cl = []
    for cs in tree.xpath("//*[@class]/@class"):
        for c in cs.split(" "):
            cl.append(c)
    return cl


def get_html_elements(element):
    """ Take an lxml ElementTree; return Counter of elements; count once. """
    res = []
    for e in element.iter():
        if "cython_function_or_method" in str(type(e.tag)):
            continue
        res.append(e.tag)
    return res
