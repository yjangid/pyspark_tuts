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
from person import get_person_data

#################################################################################################
#################################################################################################

def extract_columns_from_df(spark, df):
    
    # Get columns
    col = df.columns
    """
    ########## OUtput ################
    Return the list of columns
    print(col)
    ['first_name', 'middle_name', 'last_name', 'age', 'email_id', 'is_skilled', 'Industry', 'Address_id']
    """

    num_of_columns_in_df = len(df.columns)
    """
    ########## OUtput ################
    print(num_of_columns_in_df)
    Return the list of columns
    total no. of columns is 8
    """
#################################################################################################
#################################################################################################

def get_statistics_summry(spark, df):
    """
        describe operation is used to calculate the summary statistics of numerical column(s) in DataFrame. If we don’t specify the name of columns, 
        it will calculate summary statistics for all numerical columns present in DataFrame.

        The describe() function in Spark DataFrame provides statistical summary of each column in the DataFrame. It computes count, mean, standard deviation, minimum value, and maximum value for numeric columns. For string columns, it provides count, number of unique values, most frequent value, and its frequency.

    Let's go through the output of df.describe().show() for each column:

    first_name: Since it is a string column, the output will provide the count (number of non-null values), the number of unique values, the most frequent value, and its frequency.

    middle_name: Similar to the previous column, as it is a string column, the output will provide the count, number of unique values, most frequent value, and its frequency.

    last_name: Again, for this string column, the output will include the count, number of unique values, most frequent value, and its frequency.

    age: As it is an integer column, the output will include the count, mean (average), standard deviation, minimum value, and maximum value.

    email_id: Since it is a string column, the output will provide the count, number of unique values, most frequent value, and its frequency.

    is_skilled: This boolean column will show the count, mean (proportion of True values), standard deviation, minimum value (False), and maximum value (True).

    Industry: Similar to the string columns, the output will include the count, number of unique values, most frequent value, and its frequency.

    Address_id: As it is an integer column, the output will include the count, mean, standard deviation, minimum value, and maximum value.

    By using the describe() function, you can quickly obtain summary statistics for each column in a DataFrame, which can be useful for initial data exploration and understanding the distribution of data.
    """
    df.describe().show()
    """
    +-------+----------+-----------+---------+------------------+--------------------+--------+-----------------+
    |summary|first_name|middle_name|last_name|               age|            email_id|Industry|       Address_id|  
    +-------+----------+-----------+---------+------------------+--------------------+--------+-----------------+  
    |  count|        20|          3|       20|                20|                  20|      20|               20|  
    |   mean|      null|       null|     null|              34.3|                null|    null|             10.5|  
    | stddev|      null|       null|     null|5.6484744097760355|                null|    null|5.916079783099616|  
    |    min| Alexander|         A.| Anderson|                26|alexander.martine...| Finance|                1|  
    |    max|   William|         M.|    Young|                45|william.clark@exa...|   Sales|               20|  
    +-------+----------+-----------+---------+------------------+--------------------+--------+-----------------+ 
    """


#################################################################################################
#################################################################################################

def describe_on_one_column(spark, df):

    df.describe('age').show()

    """
    #### Output #########
    +-------+------------------+
    |summary|               age|
    +-------+------------------+
    |  count|                20|
    |   mean|              34.3|
    | stddev|5.6484744097760355|
    |    min|                26|
    |    max|                45|
+-------+------------------+
    """
    
#################################################################################################
#################################################################################################

def main():
    findspark.init()
    with SparkContext() as spark:
        person_data, person_schema = get_person_data()
        df = spark.createDataFrame(person_data, schema= person_schema)       
        # extract_columns_from_df(spark, df)

        # get_statistics_summry(spark, df)

        describe_on_one_column(spark, df)

#################################################################################################
#################################################################################################

if __name__ == "__main__":
    main()