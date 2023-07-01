# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 22:43:46 2023

@author: Yogesh
"""

import findspark
import sys
import os
# sys.path.append('D:\new_git_repo\pyspark_tuts\MyPySparkProject')

# Get the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Add the project directory to sys.path
sys.path.append(project_dir)

from spark_modules.spark_utils import SparkContext

findspark.init()


def create_df_from_rdd(spark):
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    
    columns = ["language","users_count"]
    
    # create spark context object
    sc = spark.sparkContext
    
    # Create an RDD
    rdd =  sc.parallelize(data)
    
    # Convert RDD to DataFrame
    DFFromRDD1 = rdd.toDF()
    
    DFFromRDD1.show()
    
    DFFromRDD1.printSchema()

    out_path = r"D:\\git_pyspark\\pyspark_tuts\\MyPySparkProject\\outfiles"
    print("output_path - ", out_path)
    # DFFromRDD1.write.format("csv").mode('overwrite').save(out_path)
    
    """
    ################# Output of DFFromRDD1 #################
    +------+------+
    |    _1|    _2|
    +------+------+
    |  Java| 20000|
    |Python|100000|
    | Scala|  3000|
    +------+------+
    
    #DFFromRDD1.printSchema()
    
    root
     |-- _1: string (nullable = true)
     |-- _2: string (nullable = true)
    """
    
    # Create RDD from DF with column names
    DDFromRDD2 = rdd.toDF(schema=columns)
    
    DDFromRDD2.show()
    
    DDFromRDD2.printSchema()
    
    """
    ################# Output of DFFromRDD2 #################
    +--------+-----------+
    |language|users_count|
    +--------+-----------+
    |    Java|      20000|
    |  Python|     100000|
    |   Scala|       3000|
    +--------+-----------+
    
    DDFromRDD2.printSchema()
    root
     |-- language: string (nullable = true)
     |-- users_count: string (nullable = true)
    
    """
def main():
    
    # Create Spark session using context manager
    with SparkContext() as spark:
        
        # Use the 'spark' object for Spark operations        
        create_df_from_rdd(spark)
        

if __name__ == "__main__":
    main()