import findspark
findspark.init()
"""
The foreach action in PySpark is used to apply a function to each element in a DataFrame. It is commonly used for performing side effects, such as writing the elements to an external storage system or printing them.

Here's an example to help you understand the foreach action:
"""

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame with some values
data = [("John", 25), ("Jane", 30), ("Mike", 35), ("Alex", 40)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Define a function to print each row
def print_row(row):
    name = row['name']
    age = row['age']
    print(f"Name: {name}, Age: {age}")

"""
In this example, we create a DataFrame df with columns "name" and "age". We define a function print_row that takes a row as input and prints the values of the "name" and "age" columns. 
Then, we use the foreach action on the DataFrame and pass the print_row function as an argument using a lambda function. As a result, the print_row function is applied to each row in 
the DataFrame, and the values are printed.

It's important to note that the foreach action is a transformation that happens on the distributed data in parallel. The function provided to foreach is executed on each partition of 
the DataFrame independently, and the order of output might not be guaranteed.
"""
# Apply the function to each row using foreach
df.foreach(lambda row: print_row(row))
# Output:
# Name: John, Age: 25
# Name: Jane, Age: 30
# Name: Mike, Age: 35
# Name: Alex, Age: 40

#############################################################################################
####################### Different ways of using foreach #####################################
#############################################################################################
# Using a lambda function
df.foreach(lambda row: print_row(row))
# We have already execute this in above way

#Using a named function
df.foreach(print_row)
"""
the same way but we directly passing the function name here
Name: John, Age: 25                                                 (0 + 8) / 8]
Name: Alex, Age: 40
Name: Jane, Age: 30
Name: Mike, Age: 35
"""

#Using an instance method
# In the example you provided, we have a RowProcessor class with a process_row method. 
# This method will be used to process each row of the DataFrame using the foreach action.
class RowProcessor:
    def process_row(self, row):
        name = row['name']
        age = row['age']
        print(f"Name: {name}, Age: {age}")

row_processor = RowProcessor()
df.foreach(row_processor.process_row)
"""
Name: Alex, Age: 40                                                 (0 + 8) / 8]
Name: John, Age: 25
Name: Jane, Age: 30
Name: Mike, Age: 35
In this example, we create a DataFrame df with columns "name" and "age". We define a RowProcessor class with a process_row method.
 This method takes a row as input, extracts the values of the "name" and "age" columns, and prints them.
 Additional processing logic can be added within the process_row method.

Next, we create an instance of the RowProcessor class called row_processor. Finally, we use the foreach action on the DataFrame and 
pass the row_processor.process_row method as an argument. The foreach action applies the process_row method to each row in the DataFrame, 
and the values are printed.

By using a class and an instance method, you have the flexibility to define more complex processing logic within the process_row method. 
This can include calculations, aggregations, or any other operations you need to perform on each row of the DataFrame.
"""
