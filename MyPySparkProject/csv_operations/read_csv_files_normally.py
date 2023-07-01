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

findspark.init()

from spark_modules.spark_utils import SparkContext
from pyspark.sql.types import StructType, StringType, IntegerType 

# In this tutorial we will understand how we can read the files

def get_file_path(file_name, file_format):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
    file_path = os.path.join(project_dir, "data", f"{file_name}.{file_format}")

    return file_path


def read_csv_normal_way(spark, file_path):
    """
    Reading a csv file in simple way
    +-----------+----------------+--------------+
    |        _c0|             _c1|           _c2|
    +-----------+----------------+--------------+
    |employee_id|   employee_name| department_id|
    |          1|        John Doe|             1|
    |          2|      Jane Smith|             2|
    |          3| Michael Johnson|             1|
    |          4|  Sarah Williams|             3|
    +-----------+----------------+--------------+
    above output, column treated as data in normal way
    """
    df = spark.read.csv(file_path)
    df.show()

def read_csv_with_header(spark, file_path):

    # here we would use option to provide the header = true
    df =  spark.read.format("csv").option("header", "true").load(file_path)
    df.show()
    df.printSchema()
    """
    ######### Output ##############
        We have passed header=True that's why it considerd that file containes the header
        +-----------+----------------+--------------+
        |employee_id|   employee_name| department_id|
        +-----------+----------------+--------------+
        |          1|        John Doe|             1|
        |          2|      Jane Smith|             2|
        |          3| Michael Johnson|             1|
        |          4|  Sarah Williams|             3|
        +-----------+----------------+--------------+

        root
        |-- employee_id: string (nullable = true)
        |--  employee_name: string (nullable = true)
        |--  department_id: string (nullable = true)
    """

def read_csv_with_header_infraschema(spark, file_path):

    """
        Other ways to pass the options 
        spark.read.options(inferSchema='True',delimiter=',').csv(file_path)
    """
    df = (
        spark.read.format("csv")
          .option("inferSchema",True)
          .option("delimiter",',')
          .option("header", True)
          .load(file_path)
          )
    
    # df.show()
    # df.printSchema()

    """
    By defauld=t infraschema is False that consider all columns as String
    if we set it True then it dynamically specifies the schema
    +-----------+----------------+--------------+
    |employee_id|   employee_name| department_id|
    +-----------+----------------+--------------+
    |          1|        John Doe|           1.0|
    |          2|      Jane Smith|           2.0|
    |          3| Michael Johnson|           1.0|
    |          4|  Sarah Williams|           3.0|
    +-----------+----------------+--------------+

    root
    |-- employee_id: integer (nullable = true)
    |--  employee_name: string (nullable = true)
    |--  department_id: double (nullable = true)
    """

    # Now lets add the schema then try it
    schema = (
                StructType()
                .add("employee_id", IntegerType(), True)
                .add("employee_name", StringType(), True)
                .add("department_id", IntegerType(), True)
                )
    
    dfschema = spark.read.format("csv").options(header='True', delimiter=',', nullValue='null', trim=True).schema(schema).load(file_path)
    # dfschema.show()
    # dfschema.printSchema()

    """
    Here you can add your own schema implementation
    +-----------+---------------+-------------+
    |employee_id|  employee_name|department_id|
    +-----------+---------------+-------------+
    |          1|       John Doe|            1|
    |          2|     Jane Smith|            2|
    |          3|Michael Johnson|            1|
    |          4| Sarah Williams|            3|
    +-----------+---------------+-------------+

    root
    |-- employee_id: integer (nullable = true)
    |-- employee_name: string (nullable = true)
    |-- department_id: integer (nullable = true)

    """
    return dfschema

def main():
    file_path = get_file_path("employee", 'csv')

    with SparkContext() as spark:
        # read_csv_normal_way(spark, file_path)
        # read_csv_with_header(spark, file_path)
        read_csv_with_header_infraschema(spark, file_path)
        


if __name__ == "__main__":
    main()