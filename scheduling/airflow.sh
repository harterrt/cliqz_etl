#!/bin/bash

# We use jupyter by default, but here we want to use python
unset PYSPARK_DRIVER_PYTHON

# Clone, install, and run
git clone https://github.com/harterrt/cliqz_etl.git cliqz_etl
cd cliqz_etl
pip install .
spark-submit scheduling/airflow.py
