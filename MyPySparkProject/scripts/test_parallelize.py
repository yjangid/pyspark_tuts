# -*- coding: utf-8 -*-
"""
Created on Sun Jun 25 23:04:05 2023

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

def create_dataframe_from_parallelize(spark):
    sc = spark.sparkContext
    # Parallelize a list of numbers
    nums = sc.parallelize([1, 2, 3, 4])
    
    # Glom the numbers into groups of 1
    result = nums.glom().collect()
    
    return result
    

def main():
    
    # Create Spark session using context manager
    with SparkContext() as spark:
        
        # Use the 'spark' object for Spark operations        
        result = create_dataframe_from_parallelize(spark)
        
        # Print the results
        print(result)

if __name__ == "__main__":
    main()

"""
############# Output ##############
[[1, 2, 3, 4]]
"""