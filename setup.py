#!/usr/bin/env python

from setuptools import setup, find_packages
from distutils.core import setup

setup(name='cliqz_etl',
      version='0.1',
      description='An ETL job to save Cliqz testpilot data to parquet',
      author='Ryan Harter (harterrt)',
      author_email='harterrt@mozilla.com',
      url='https://github.com/harterrt/cliqz_etl.git',
      packages=find_packages(exclude=['tests']),
)
