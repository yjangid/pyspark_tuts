# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 00:19:26 2023

@author: Yogesh
"""

import findspark
import sys
# sys.path.append('D:\git_repo_restored\MyPySparkProject')
import os
# sys.path.append('D:\new_git_repo\pyspark_tuts\MyPySparkProject')

# Get the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Add the project directory to sys.path
sys.path.append(project_dir)
from spark_modules.spark_utils import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def create_dataframe_from_sparkSession(spark) -> None:
    """
    Calling createDataFrame() from SparkSession is another way to create 
    PySpark DataFrame manually, it takes a list object as an argument. 
    and chain with toDF() to specify names to the columns.
    """

    # create the data and columns    
    columns = ["language","users_count"]
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    
    df = spark.createDataFrame(data).toDF(*columns)
    
    print("########## create_dataframe_from_sparkSession ############")
    df.show()
    """
    ---- output ----
    +--------+-----------+
    |language|users_count|
    +--------+-----------+
    |    Java|      20000|
    |  Python|     100000|
    |   Scala|       3000|
    +--------+-----------+
    """
    
def create_dataframe_from_row_type(spark) -> None:
    """
    createDataFrame() has another signature in PySpark which takes the 
    collection of Row type and schema for column names as arguments. 
    To use this first we need to convert our “data” object from the 
    list to list of Row.
    """
    
    # create the data and columns    
    columns = ["language","users_count"]
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    
    rowData = map(lambda x: Row(*x), data)
    
    """
    # To understand how rowData looks like so we are printing the rowData
    print(list(rowData))
    --- output of above print statment ---
    
    it creates list of Row objects - 
    
    [<Row('Java', '20000')>, <Row('Python', '100000')>, <Row('Scala', '3000')>]
    """
    
    
    df = spark.createDataFrame(rowData,columns)
    
    print("########## create_dataframe_from_row_type ############")
    df.show()
    """
    ---- output ----
    +--------+-----------+
    |language|users_count|
    +--------+-----------+
    |    Java|      20000|
    |  Python|     100000|
    |   Scala|       3000|
    +--------+-----------+
    """
    
def create_dataframe_from_schema(spark) -> None:
    """
    createDataFrame() has another signature in PySpark which takes the 
    collection of Row type and schema for column names as arguments. 
    To use this first we need to convert our “data” object from the 
    list to list of Row.
    """
    
    # Create data as list
    data = [
             ("James","","Smith","36636","M",3000), 
             ("Michael","Rose","","40288","M",4000), 
             ("Robert","","Williams","42114","M",4000), 
             ("Maria","Anne","Jones","39192","F",4000), 
             ("Jen","Mary","Brown","","F",-1)
             ]
    
    # Create column with schema name
    schema = StructType(
                        [StructField("First_name", StringType(), True), 
                         StructField("Middle_name", StringType(), True),
                         StructField("Last_name", StringType(), True),
                         StructField("Id", StringType(), True), 
                         StructField("Gender", StringType(), True),
                         StructField("Salary", IntegerType(), True)
                        ]
                        )
    
    df = spark.createDataFrame(data=data, schema=schema)
    
    print("########## create_dataframe_from_schema ############")
    df.show()
    df.printSchema()
    """
    --------- Output ------------
    ------ df.show() -------
    +----------+-----------+---------+-----+------+------+
    |First_name|Middle_name|Last_name|   Id|Gender|Salary|
    +----------+-----------+---------+-----+------+------+
    |     James|           |    Smith|36636|     M|  3000|
    |   Michael|       Rose|         |40288|     M|  4000|
    |    Robert|           | Williams|42114|     M|  4000|
    |     Maria|       Anne|    Jones|39192|     F|  4000|
    |       Jen|       Mary|    Brown|     |     F|    -1|
    +----------+-----------+---------+-----+------+------+
    
    ------------ df.printSchema() --------------
    root
     |-- First_name: string (nullable = true)
     |-- Middle_name: string (nullable = true)
     |-- Last_name: string (nullable = true)
     |-- Id: string (nullable = true)
     |-- Gender: string (nullable = true)
     |-- Salary: integer (nullable = true)
    
    """

def main():

    findspark.init()

    with SparkContext() as spark:
        create_dataframe_from_sparkSession(spark)
        create_dataframe_from_row_type(spark)
        create_dataframe_from_schema(spark)

    
if __name__ == "__main__":
    main()
    
    
