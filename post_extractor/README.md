# Projekt ZTNBD

## Lokalna instalacja
###### Wymagania
- docker
- ~4GB wolnego miejsca na dysku
- zalecany linux + build-essential

###### Linux 
```bash
make notebook
```
###### Others (nie sprawdzane)
```bash
# lokalizacja - główny katalogu projektu
docker run -it --rm -v $(pwd):/home/jovyan/work -p 8888:8888 jupyter/pyspark-notebook
```

Komenda startuje kontener dockerowy z Jupyterem i podmontowuje katalog projektu.
Moduły znajdują się w katalogu `modules` i tam też będą lądowały kolejne.
Uruchomienie póki co możliwe jest tylko z poziomu notebook'a.

## Moduły

##### PostTransformer
Klasa PostTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta wydobywa z kolumny inputCol, z formatu json, content i umieszcza go w kolumnie outputCol w postaci tekstu.
##### TranslateTransformer
Klasa TranslateTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta tłumaczy tekst zawarty w kolumnie inputCol  z języka polskiego na angielski i umieszcza go w kolumnie outputCol.
##### SentenceTransformer
Klasa SentenceTransformer dziedziczy po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta dzieli tekst zawarty w kolumnie inputCol  na zdania i umieszcza go w kolumnie outputCol w postaci tablicy tekstów.
##### SpeechPartsTransformer
Klasa SpeechPartsTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta z tekstu zawartego w kolumnie inputCol  zlicza wystąpienie części mowy i wstawia do outputCol w postaci jsona.
##### SentimentTransformer
Klasa SentimentTransformer dziedziczy  po klasach pyspark.ml.Transformer, pyspark.ml.param.shared.HasInputCol, pyspark.ml.param.shared.HasOutputCol. Posiada metodę transform, która przyjmuje na wejściu obiekt typu dataframe. Metoda ta z tekstu zawartego w kolumnie inputCol  wylicza sentiment i wstawia do kolumny outputCol.

## Dokumentacja
[Google Docs](https://docs.google.com/document/d/1IylTvJbRe8s_j_bZqbM-6nWVa2IQDjQiZx-NyJsGZbg)

 
