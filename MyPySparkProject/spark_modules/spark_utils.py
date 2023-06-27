# -*- coding: utf-8 -*-
"""
Created on Sun Jun 25 23:01:50 2023

@author: Yogesh
"""

import os
from pyspark.sql import SparkSession

class SparkContext:
    def __enter__(self):
        self.spark = SparkSession.builder \
            .appName("My Spark Application") \
            .master("local") \
            .getOrCreate()
        return self.spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()

def main():
    
    file_name = os.path.basename(__file__)
    print(f"Module - {file_name}")

if __name__ == "__main__":
    main()