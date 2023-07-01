import findspark
findspark.init()

"""
The foreachPartition function in PySpark allows you to apply a function to each partition of a DataFrame. It is useful when you want to perform some operation that requires handling a subset of data at a time, such as writing data to an external system or performing bulk operations.

Here's an example to illustrate the usage of foreachPartition
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("foreachPartions use").getOrCreate()

# Create a DataFrame with some values
data = [("John", 25), ("Jane", 30), ("Mike", 35), ("Alex", 40)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

def process_partitions(iterator):
    for row in iterator:
        name = row['name']
        age = row['age']
        print(f"Name: {name}, Age:{age}")

df.foreachPartition(process_partitions)
"""
Name: John, Age:25                                                  (0 + 8) / 8]
Name: Jane, Age:30
Name: Mike, Age:35====================>                             (4 + 4) / 8]
Name: Alex, Age:40

In this example, we create a DataFrame df with columns "name" and "age". We define a function process_partition that takes an iterator as input. 
The iterator represents a partition of the DataFrame. Within the function, we iterate over each row in the partition and perform some processing 
logic. In this case, we extract the values of the "name" and "age" columns and print them.

Finally, we use the foreachPartition function on the DataFrame and pass the process_partition function as an argument. The foreachPartition 
function applies the process_partition function to each partition of the DataFrame. The processing logic defined within the function will be 
applied separately to each partition.

It's important to note that the foreachPartition function is executed on the executors, so any side effects or external operations performed
 within the function should be designed accordingly.
"""

##############################################################

class ProcessPartitions:
    def process_row_partitions(self, iterator):
        for row in iterator:
            name = row['name']
            age = row['age']
            print(f"Name: {name}, Age:{age}")

row_processor = ProcessPartitions()

df.foreachPartition(row_processor.process_row_partitions)

"""
Name: John, Age:25                                                  (0 + 8) / 8]
Name: Mike, Age:35
Name: Alex, Age:40
Name: Jane, Age:30
"""