# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 00:19:26 2023

@author: Yogesh
"""

from spark_module import create_spark_session, initialize_spark
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def create_dataframe_from_sparkSession() -> None:
    """
    Calling createDataFrame() from SparkSession is another way to create 
    PySpark DataFrame manually, it takes a list object as an argument. 
    and chain with toDF() to specify names to the columns.
    """
    
    # Intialize and get the spark session
    initialize_spark()
    spark = create_spark_session()

    # create the data and columns    
    columns = ["language","users_count"]
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    
    df = spark.createDataFrame(data).toDF(*columns)
    
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
    
def create_dataframe_from_row_type() -> None:
    """
    createDataFrame() has another signature in PySpark which takes the 
    collection of Row type and schema for column names as arguments. 
    To use this first we need to convert our “data” object from the 
    list to list of Row.
    """
    
    # Intialize and get the spark session
    initialize_spark()
    spark = create_spark_session()

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
    
def create_dataframe_from_schema() -> None:
    """
    createDataFrame() has another signature in PySpark which takes the 
    collection of Row type and schema for column names as arguments. 
    To use this first we need to convert our “data” object from the 
    list to list of Row.
    """
    
    # Intialize and get the spark session
    initialize_spark()
    spark = create_spark_session()

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
    print(df.printSchema())
    
    df.show()
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
    
    
    
# create_dataframe_from_sparkSession()
# create_dataframe_from_row_type()
create_dataframe_from_schema()
    
    
    