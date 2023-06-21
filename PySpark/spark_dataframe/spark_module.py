# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 22:07:11 2023

@author: Yogesh
"""

import findspark
from pyspark.sql import SparkSession

def initialize_spark():
    findspark.init()
    findspark.find()

# Create Spark session
def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("My Spark Application") \
        .master("local") \
        .getOrCreate()
    return spark

# Initialize Spark once
initialize_spark()  


