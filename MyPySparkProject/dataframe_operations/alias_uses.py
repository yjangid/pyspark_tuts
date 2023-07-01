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

"""
In PySpark, the alias() method is used to assign an alternate name or alias to a DataFrame or to the columns within a DataFrame. It can be helpful when you want to refer to a DataFrame or its columns by a different name, 
especially when performing operations involving multiple DataFrames or complex transformations.
"""

def work_with_alias_method(spark):
    """
        we first create a DataFrame df with columns "name" and "age". Then, we assign an alias "people" to the DataFrame using the alias() method, creating a new DataFrame df_alias. We can use both the original DataFrame df and the aliased DataFrame df_alias to perform operations.

        The alias() method is also useful when you want to disambiguate column names after joining or when working with self-joins or subqueries. It allows you to refer to columns using the specified alias to avoid naming conflicts.

        Note that alias() does not modify the original DataFrame. It creates a new DataFrame with the assigned alias, and you can use the aliased DataFrame for further operations.

    """
    # Create a DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    # Assign an alias to the DataFrame
    df_alias = df.alias("people")

    # Use the original DataFrame and the aliased DataFrame
    df.show()
    df_alias.show()

    """
        ############# df.show() #########################
        +-------+---+
        |   name|age|
        +-------+---+
        |  Alice| 25|
        |    Bob| 30|
        |Charlie| 35|
        +-------+---+
        ############# df_alias.show() #########################
        +-------+---+
        |   name|age|
        +-------+---+
        |  Alice| 25|
        |    Bob| 30|
        |Charlie| 35|
        +-------+---+
    """

    # Select columns from the original DataFrame using the alias
    df.select(df.name.alias("person_name"), df.age).show()
    """
        ###########output of selecct##############
        +-----------+---+
        |person_name|age|
        +-----------+---+
        |      Alice| 25|
        |        Bob| 30|
        |    Charlie| 35|
        +-----------+---+

    """

    # Perform a join operation using the aliased DataFrame
    joined_df = df_alias.join(df, df_alias.name == df.name, "inner")
    joined_df.show()
    """
        ###########output of join##############
        +-------+---+-------+---+
        |   name|age|   name|age|
        +-------+---+-------+---+
        |  Alice| 25|  Alice| 25|
        |    Bob| 30|    Bob| 30|
        |Charlie| 35|Charlie| 35|
        +-------+---+-------+---+
    """

def main():
    findspark.init()
    with SparkContext() as spark:
        work_with_alias_method(spark)

if __name__ == "__main__":
    main()