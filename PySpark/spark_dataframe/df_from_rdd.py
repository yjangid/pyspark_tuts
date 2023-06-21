# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 22:55:03 2023

@author: Yogesh
"""

from spark_module import create_spark_session, initialize_spark
from pyspark.sql import dataframe


def create_dataframe_from_rdd_by_sparkcontext() -> None:
    
    """
    Function creates dataframe from rdd
    to create a dataframe from rdd use spark function .toDF
    """
    
    # Intialize and get the spark session
    initialize_spark()
    spark = create_spark_session()
    
    # Create Spark Context objec
    sc = spark.sparkContext

    # create the data and columns    
    columns = ["language","users_count"]
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    
    # Create a rdd from data
    rdd = sc.parallelize(data)
    
    # Convert rdd to dataframe
    DFfromrdd1 = rdd.toDF()
    
    DFfromrdd1.show()
    
    DFfromrdd1.printSchema()
    
    ##### Output of DFfromrdd1 #####
    """
    ----- DFfromrdd1.show() -----
    +------+------+
    |    _1|    _2|
    +------+------+
    |  Java| 20000|
    |Python|100000|
    | Scala|  3000|
    +------+------+
    
    ----- DFfromrdd1.printSchema() -----
    root
     |-- _1: string (nullable = true)
     |-- _2: string (nullable = true)
    """
    
    # Convert RDD to DF with columns
    DFfromrdd2 = rdd.toDF(columns)
    
    DFfromrdd2.show()
    DFfromrdd2.printSchema()
    
    """
    ----- DFfromrdd2.show() -----
    +--------+-----------+
    |language|users_count|
    +--------+-----------+
    |    Java|      20000|
    |  Python|     100000|
    |   Scala|       3000|
    +--------+-----------+
    
    ----- DFfromrdd1.printSchema() -----
    root
     |-- language: string (nullable = true)
     |-- users_count: string (nullable = true)
    """
    
    
def create_dataframe_from_rdd_by_sparksession() -> None:
    """
    Using createDataFrame() from SparkSession is another way to create 
    manually and it takes rdd object as an argument. and chain with toDF() 
    to specify name to the columns.
    """
    
    # Intialize and get the spark session
    initialize_spark()
    spark = create_spark_session()
    
    # Create Spark Context objec
    sc = spark.sparkContext

    # create the data and columns    
    columns = ["language","users_count"]
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    
    # Create a rdd from data
    rdd = sc.parallelize(data)
    
    DFfromrdd = spark.createDataFrame(rdd).toDF(*columns)
    
    DFfromrdd.show()
    
    """
    ------- DFfromrdd.show() ----------
    +--------+-----------+
    |language|users_count|
    +--------+-----------+
    |    Java|      20000|
    |  Python|     100000|
    |   Scala|       3000|
    +--------+-----------+
    """
    
    
create_dataframe_from_rdd_by_sparkcontext()
create_dataframe_from_rdd_by_sparksession()
    
    
    