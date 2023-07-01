import findspark
findspark.init()

"""
df.filter() is used to apply a filter condition to a DataFrame and retrieve the rows that satisfy the condition. 
There are multiple ways to apply filters using df.filter(). Let's explore them with examples:

Consider the following DataFrame:
"""

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame with some values
data = [
    ("John", 25),
    ("Jane", 30),
    ("Mike", 35),
    ("Alex", 40)
]

columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Filter based on a single condition using a column name and comparison operator:
filtered_df = df.filter(df.age > 30)
# filtered_df.show()
"""
+----+---+
|name|age|
+----+---+
|Mike| 35|
|Alex| 40|
+----+---+
"""

# Filter based on multiple conditions using logical operators:
filtered_df_logical_op = df.filter((df.age > 25) & (df.age <35) & (df.name.startswith("J")))
# filtered_df_logical_op.show()
"""
+----+---+
|name|age|
+----+---+
|Jane| 30|
+----+---+
"""

# Filter using SQL-like syntax
# Filter rows where age is equal to 30 using SQL-like syntax
filtered_df = df.filter("age = 30")
filtered_df.show()
"""
+----+---+
|name|age|
+----+---+
|Jane| 30|
+----+---+
"""
