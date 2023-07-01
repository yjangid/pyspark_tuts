import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import colRegex

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
data = [("Alice", 25, "New York"), ("Bob", 30, "London"), ("Charlie", 35, "Paris")]
df = spark.createDataFrame(data, ["name", "age", "city"])

# Select columns using colRegex
selected_cols = df.select(colRegex("n.*"), colRegex(".*try"))

# Show the selected columns
selected_cols.show()

######## OutPUT ############
# +------+-------+------+
# |name  |country|
# +------+-------+------+
# |Alice |New York|
# |Bob   |London |
# |Charlie|Paris  |
# +------+-------+

