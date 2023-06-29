import os

def read_data_files(spark, file_path, format="csv", header=True, partition_by=None):
    return spark.read.format(format).option("header", header).option("inferSchema", "true").load(file_path)

def main():
    module_name = os.path.basename(__file__)
    print(f"Module - {module_name}")

if __name__ == "__main__":
    main()