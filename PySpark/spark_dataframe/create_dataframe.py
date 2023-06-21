# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 22:35:31 2023

@author: Yogesh
"""

from spark_module import create_spark_session, initialize_spark

def main():
    
    # Intialize the spark
    initialize_spark()
    
    # Get the spark session
    spark = create_spark_session()
    
    # Create a list of data
    data = [('James','','Smith','1991-04-01','M',3000), 
            ('Michael','Rose','','2000-05-19','M',4000),
            ('Robert','','Williams','1978-09-05','M',4000),  
            ('Maria','Anne','Jones','1967-12-01','F',4000), 
            ('Jen','Mary','Brown','1980-02-17','F',-1)
            ]

    # Create a list of columns
    columns = ["firstname","middlename","lastname","dob","gender","salary"]

    #Create a dataframe
    df =  spark.createDataFrame(data=data, schema=columns)

    df.show()
    
if __name__ == '__main__':
    main()