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

def select_columns(spark, df):
    df.select('first_name', 'is_skilled').show(5)
    """
        +----------+----------+
        |first_name|is_skilled|
        +----------+----------+
        |      John|      true|
        |      Jane|     false|
        |     David|      true|
        |      Emma|      true|
        |   Michael|     false|
    """
#################################################################################################
#################################################################################################
def get_distinct_values(spark, df):
    df.select('Industry').distinct().show()
    """
        +---------+
        | Industry|
        +---------+
        |    Sales|
        |       HR|
        |  Finance|
        |Marketing|
        |       IT|
        +---------+
    """
#################################################################################################
#################################################################################################
def cross_tab_pair_frequency(df):
    """
        We can use crosstab operation on DataFrame to calculate the pair wise frequency of columns.
    """

    df.crosstab('is_skilled', 'gender').show()
    """
        Basically we can say that it finds the differences like
        We have toatl number of records in dataframe is 20
        now - when we passing is_skilled and gender columns it is able to find
        no. of skilled and non skilled - female and male
        +-----------------+---+---+
        |is_skilled_gender|  F|  M|
        +-----------------+---+---+
        |             true|  6|  7|
        |            false|  4|  3|
        +-----------------+---+---+
    """

#################################################################################################
#################################################################################################
def drop_duplicate_from_df(df):
    non_duplicate_df = df.select("age","is_skilled").dropDuplicates()
    non_duplicate_df.show()
    print(non_duplicate_df.count())

    """
        +---+----------+
        |age|is_skilled|
        +---+----------+
        | 30|     false|
        | 29|     false|
        | 41|      true|
        | 39|     false|
        | 26|      true|
        | 31|      true|
        | 45|      true|
        | 32|      true|
        | 29|      true|
        | 34|      true|
        | 28|     false|
        | 33|      true|
        | 43|      true|
        | 38|     false|
        | 27|      true|
        | 36|     false|
        | 31|     false|
        | 42|      true|
        | 37|      true|
        | 35|      true|
        +---+----------+

        count - 20
        no duplecates found 
    """

    
#################################################################################################
#################################################################################################
def fill_empty_columns(df):
    """
        Use fillna operation here. The fillna will take two parameters to fill the null values.
        value:
        It will take a dictionary to specify which column will replace with which value.
        A value (int , float, string) for all columns.
        subset: Specify some selected columns.
    """
    df.fillna("No middle name", subset=["middle_name"]).show(5)
    """
        +----------+---------+--------------+
        |first_name|last_name|   middle_name|
        +----------+---------+--------------+
        |      John|    Smith|            A.|
        |      Jane|      Doe|            M.|
        |     David|    Brown|No middle name|
        |      Emma|    Davis|No middle name|
        |   Michael|      Lee|No middle name|
        +----------+---------+--------------+
    """

    ###### if you want to make all rows and columns to be fill then no need to provide the columns ####
    df.fillna('-1').show(5)
    """
    You are correct. The issue was that the column you were trying to fill with -1 was of string type, and you were passing an integer value fillna(-1). 
    In Spark, the value provided to fillna() should match the data type of the column.
    output
        Only we have middle_name column empty but we can apply this way 
       +----------+-----------+---------+---+--------------------+----------+---------+----------+------+
        |first_name|middle_name|last_name|age|            email_id|is_skilled| Industry|Address_id|gender|
        +----------+-----------+---------+---+--------------------+----------+---------+----------+------+
        |      John|         A.|    Smith| 35|john.smith@exampl...|      true|       IT|         1|     M|
        |      Jane|         M.|      Doe| 28|jane.doe@example.com|     false|    Sales|         2|     F|
        |     David|         -1|    Brown| 42|david.brown@examp...|      true|  Finance|         3|     M|
        |      Emma|         -1|    Davis| 31|emma.davis@exampl...|      true|Marketing|         4|     F|
        |   Michael|         -1|      Lee| 39|michael.lee@examp...|     false|       IT|         5|     M|
        +----------+-----------+---------+---+--------------------+----------+---------+----------+------+
    """
#################################################################################################
#################################################################################################
def order_by_desc(df):
    df.orderBy(df.age.desc()).show(5)
    """
        #### output #############
        +----------+-----------+---------+---+--------------------+----------+---------+----------+------+
        |first_name|middle_name|last_name|age|            email_id|is_skilled| Industry|Address_id|gender|
        +----------+-----------+---------+---+--------------------+----------+---------+----------+------+
        |     Sarah|         J.|  Johnson| 45|sarah.johnson@exa...|      true|       HR|         6|     F|
        |      Noah|       null|    Green| 43|noah.green@exampl...|      true|Marketing|        19|     M|
        |     David|       null|    Brown| 42|david.brown@examp...|      true|  Finance|         3|     M|
        |     Emily|       null| Anderson| 41|emily.anderson@ex...|      true|       IT|        10|     F|
        |   Michael|       null|      Lee| 39|michael.lee@examp...|     false|       IT|         5|     M|
    """
#################################################################################################
#################################################################################################

def get_first_row_from_df(df):
    """
        The show() method is used to display the contents of a DataFrame, but the first() method returns the first row of the DataFrame as a Row object. 
        Therefore, calling show() on the result of first() will result in an error.
        df.first() is mostly equivalant to df.show(1)
    """
    new_df = df.first()
    print(new_df)
    """
        output -
        Row(first_name='John', middle_name='A.', last_name='Smith', age=35, email_id='john.smith@example.com', is_skilled=True, Industry='IT', Address_id=1, gender='M')
    """
#################################################################################################
#################################################################################################

def main():
    findspark.init()
    with SparkContext() as spark:
        person_data, person_schema = get_person_data()
        df = spark.createDataFrame(person_data, schema= person_schema) 

        # select_columns(spark,df)      
        # get_distinct_values(spark, df)
        # cross_tab_pair_frequency(df)
        # drop_duplicate_from_df(df)
        # fill_empty_columns(df)
        # order_by_desc(df)
        get_first_row_from_df(df)


#################################################################################################
#################################################################################################
if __name__ == "__main__":
    main()