# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 22:10:43 2023

@author: Yogesh
"""



from spark_module import create_spark_session, initialize_spark
import findspark
# Usage example
# def initialize_spark():
#     global spark
#     findspark.init()
#     findspark.find()
#     spark = create_spark_session()

def main():
    initialize_spark()
    spark = create_spark_session()
    
    sc = spark.sparkContext
    # Parallelize a list of numbers
    nums = sc.parallelize([1, 2, 3, 4])

    # Glom the numbers into groups of 1
    result = nums.glom().collect()

    # Print the results
    print(result)

if __name__ == "__main__":
    main()