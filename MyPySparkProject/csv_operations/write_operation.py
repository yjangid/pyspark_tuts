import findspark
import sys
import os
# sys.path.append('D:\new_git_repo\pyspark_tuts\MyPySparkProject')

# Get the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Add the project directory to sys.path
sys.path.append(project_dir)

findspark.init()

from spark_modules.spark_utils import SparkContext
from read_csv_files_normally import get_file_path, read_csv_with_header_infraschema



def main():
    file_path = get_file_path("employee", 'csv')

    with SparkContext() as spark:
        # Get the df
        df = read_csv_with_header_infraschema(spark, file_path)

        #Write it
        # output_file_path = os.path.join(project_dir, "outfiles", "employee.csv")
        # print(output_file_path)
        out_path = r"D:\\git_pyspark\\pyspark_tuts\\MyPySparkProject\\outfiles"
        print("output_path - ", out_path)
        df.write.format("csv").mode('overwrite').save(out_path)
        
        
        


if __name__ == "__main__":
    main()