from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from cliqz_etl import main

if __name__ == "__main__":
    conf = SparkConf().setAppName('cliqz_etl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    main.etl_job(sc, sqlContext)
