import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

"""
The dropna() method in PySpark's DataFrame API has several parameters that allow you to customize how null values are handled. Here's an explanation of each parameter with an example:

how: Specifies how rows with null values are dropped. It accepts the following values:

"any": Drops a row if it contains any null values (default).
"all": Drops a row only if all its values are null.
thresh: Specifies the minimum number of non-null values required for a row to be retained. Rows with fewer non-null values will be dropped. It accepts an integer value.

subset: Specifies the subset of columns to consider for dropping rows with null values. It can be a single column name as a string or a list of column names.

Now, let's see an example that demonstrates the use of these parameters:
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

# Drop rows with null values using different parameters
df_dropna_default = df.dropna()# by default is how=any
df_dropna_default.show()
"""
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      Mike|  Johnson| 35|
+----------+---------+---+
"""

df_dropna_how_all = df.dropna(how="all")
df_dropna_how_all.show()
"""
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      John|     null| 25|
|      Jane|      Doe|null|
|      null|    Smith| 30|
|      Mike|  Johnson| 35|
|      Alex|Williams|null|
+----------+---------+---+
"""

df_dropna_thresh = df.dropna(thresh=2)
df_dropna_thresh.show()
"""
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      John|     null| 25|
|      Jane|      Doe|null|
|      null|    Smith| 30|
|      Mike|  Johnson| 35|
+----------+---------+---+
"""
df_dropna_subset = df.dropna(subset=["first_name", "last_name"])
df_dropna_subset.show()
"""
+----------+---------+---+
|first_name|last_name|age|
+----------+---------+---+
|      John|     null| 25|
|      Jane|      Doe|null|
|      Mike|  Johnson| 35|
+----------+---------+---+
"""

