import findspark
findspark.init()
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

data = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
df = spark.createDataFrame(data, ["col1", "col2", "col3"])

# Calculate the Pearson correlation between col1 and col2
correlation = df.corr("col1", "col2", method="pearson")
print(f"Pearson correlation between col1 and col2: {correlation}")
# Pearson correlation between col1 and col2: 1.0

print("----------------------------------------------------")
correlation = df.corr("col2", "col3", method="pearson")
print(f"Pearson correlation between col2 and col3: {correlation}")

print("----------------------------------------------------")

correlation = df.corr("col1", "col3", method="pearson")
print(f"Pearson correlation between col1 and col3: {correlation}")

# output
# Pearson correlation between col1 and col2: 1.0
# ----------------------------------------------------
# Pearson correlation between col2 and col3: 1.0
# ----------------------------------------------------
# Pearson correlation between col1 and col3: 1.0
# # Calculate the Spearman correlation between col2 and col3
# correlation = df.corr("col2", "col3", method="spearman")
# print(f"Spearman correlation between col2 and col3: {correlation}")
