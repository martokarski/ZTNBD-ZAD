DIR=$(dir $(realpath $(firstword $(MAKEFILE_LIST))))
SUBMIT=/usr/local/spark/bin/spark-submit
J_HOME=/home/jovyan/work

help:
	@echo "Try to use --> 'make notebook'"

notebook:
	@docker run -it --rm -v $(DIR):$(J_HOME) -p 8888:8888 jupyter/pyspark-notebook

run:
	@pip install setuptools wheel virtualenv
	@pip install textblob.zip
	@python -m textblob.download_corpora
	@docker run --rm -it -v $(DIR):/app jupyter/pyspark-notebook $(SUBMIT) /app/run.py
