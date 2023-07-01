import findspark
findspark.init()

"""
The fillna() function in PySpark's DataFrame API is used to fill null or missing values in a DataFrame with a specified value or using a specific strategy. 
Here are the different possibilities of fillna() with examples:
"""
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame with null values
data = [
    ("John", None, 25),
    ("Jane", "Doe", None),
    (None, "Smith", 30),
    ("Mike", "Johnson", 35),
    ("Alex", "Williams", None)
]

columns = ["first_name", "last_name", "age"]
df = spark.createDataFrame(data, columns)

# Fill null values with a constant value
df_filled_constant = df.fillna("Unknown")

# df_filled_constant.show()
"""
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      John|  Unknown| 25|
|      Jane|      Doe|Unknown|
|   Unknown|    Smith| 30|
|      Mike|  Johnson| 35|
|      Alex|Williams|Unknown|
+----------+---------+---+

"""

# Fill null values in specific columns with a value
df_filled_subset = df.fillna({"first_name": "N/A", "age": 0})

# df_filled_subset.show()
"""
Here we have chosen first_name and age so value gets updated only in first_name and age which have value None
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      John|     null| 25|
|      Jane|      Doe|  0|
|       N/A|    Smith| 30|
|      Mike|  Johnson| 35|
|      Alex| Williams|  0|
+----------+---------+---+
"""
# Fill null values in a numerical column with the mean value
from pyspark.sql.functions import mean

age_mean = df.select(mean("age")).first()[0]

print(age_mean)
# Row(avg(age)=30.0)
df_filled_mean = df.fillna(age_mean, subset=["age"])

df_filled_mean.show()

"""
########### Output ################
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      John|     null| 25|
|      Jane|      Doe| 30|
|      null|    Smith| 30|
|      Mike|  Johnson| 35|
|      Alex| Williams| 30|
+----------+---------+---+

df.select(mean("age")) --> 30
df.select(mean("age")).first() --> Row(avg(age)=30.0)
df.select(mean("age")).first()[0] --> 30
"""

#################################################################################################
#################################################################################################
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame with some values
data = [
    ("John", 25),
    ("Jane", 30),
    ("Mike", 35),
    ("Alex", None)
]

columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

df.select(mean("age"))
"""
df.select(mean("age")): This part calculates the mean value of the "age" column using the mean() function from pyspark.sql.functions. It selects only the "age" column from the DataFrame and calculates the mean. 
The result is a DataFrame with a single row and a single column.
+--------+
|avg(age)|
+--------+
|    30.0|
+--------+
"""
df.select(mean("age")).first()
"""
.first(): This method retrieves the first row from the DataFrame. Since the previous step resulted in a DataFrame with a single row, 
using .first() returns that row.
Row(avg(age)=30.0)

"""
df.select(mean("age")).first()[0]
"""
[0]: This indexing operation extracts the value at index 0 from the row. In this case, 
it retrieves the mean value of the "age" column.
30
"""
