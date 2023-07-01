"""
pyspark.sql.DataFrame.freqItems¶
DataFrame.freqItems(cols, support=None)[source]
Finding frequent items for columns, possibly with false positives. Using the frequent element count algorithm described
 in “https://doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou”. DataFrame.freqItems() and 
 DataFrameStatFunctions.freqItems() are aliases.

New in version 1.4.0.

Parameters
cols: list or tuple
Names of the columns to calculate frequent items for as a list or tuple of strings.

support: float, optional
The frequency with which to consider an item ‘frequent’. Default is 1%. The support must be greater than 1e-4.
"""
import findspark
findspark.init()

"""
The freqItems function in PySpark is used to compute frequent items for selected columns in a DataFrame. 
It returns a DataFrame containing the frequent items for each specified column.

Here's an example to illustrate the usage of freqItems with different ways of applying it:
"""
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
data = [
    (1, "Apple", "Red"),
    (2, "Orange", "Orange"),
    (3, "Banana", "Yellow"),
    (4, "Apple", "Green"),
    (5, "Orange", "Yellow"),
    (6, "Banana", "Green")
]
df = spark.createDataFrame(data, ["ID", "Fruit", "Color"])

# Example 1: Compute frequent items for a single column
freq_items_1 = df.freqItems(["Fruit"], support=0.5)
freq_items_1.show(truncate=False)

# Example 2: Compute frequent items for multiple columns
freq_items_2 = df.freqItems(["Fruit", "Color"], support=0.5)
freq_items_2.show(truncate=False)

# Example 3: Set parameters for frequent items computation
freq_items_3 = df.freqItems(["Fruit"], support=0.5, numPartitions=2)
freq_items_3.show(truncate=False)

"""
# Example 1:
+-----------------+
|Fruit_freqItems  |
+-----------------+
|[Apple, Orange]  |
+-----------------+

# Example 2:
+-----------------+----------------------+
|Fruit_freqItems  |Color_freqItems       |
+-----------------+----------------------+
|[Apple, Orange]  |[Green, Yellow, Orange]|
+-----------------+----------------------+

# Example 3:
+-----------------+
|Fruit_freqItems  |
+-----------------+
|[Apple, Orange]  |
+-----------------+

In the first example, we compute the frequent items for the "Fruit" column. The result shows the list of frequent items in that column.

In the second example, we compute the frequent items for both the "Fruit" and "Color" columns. The result shows the frequent items for each column separately.

In the third example, we set additional parameters for the frequent items computation. We specify the number of partitions to control the parallelism of the 
computation.

Note that the support parameter represents the minimum fraction of rows that should contain an item to be considered frequent. 
It should be a value between 0 and 1.
"""

##########################################################################################
##########################################################################################
"""
Let's go through the concepts of support and numPartitions, and also address the question about the first example.

Support:

The support parameter in freqItems represents the minimum fraction of rows that should contain an item for it to be considered frequent.
It is specified as a value between 0 and 1. For example, a support value of 0.5 means that an item must appear in at least 50% of the rows to be considered frequent.
By setting a higher support value, you can filter out less frequent items and focus on the most common ones.
numPartitions:

The numPartitions parameter in freqItems determines the number of partitions used for parallel computation of frequent items.
It specifies the degree of parallelism and can be useful for improving performance when working with large datasets.
By default, the value is set to None, which means that the number of partitions is determined automatically based on the cluster configuration.
You can set numPartitions to a specific integer value to control the number of partitions used.
Regarding the first example and the absence of "Banana" in the frequent items:

In the first example, the support parameter was not explicitly specified, so the default value is used.
The default value of support is 0.1, which means that an item must appear in at least 10% of the rows to be considered frequent.
If the frequency of "Banana" is less than 10% in the "Fruit" column, it will not be considered a frequent item and will not appear in the result.
To include "Banana" as a frequent item, you can either reduce the support threshold or increase the frequency of "Banana" in the DataFrame.
"""
freq_items_1 = df.freqItems(["Fruit"], support=0.2)
freq_items_1.show(truncate=False)

"""
+-----------------+
|Fruit_freqItems  |
+-----------------+
|[Apple, Banana, Orange]  |
+-----------------+

"""


