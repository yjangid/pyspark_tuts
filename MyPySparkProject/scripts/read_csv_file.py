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
from spark_modules.pipeline import Pipeline
from spark_modules.load_yaml_file import load_yaml
from spark_modules.read_files import read_data_files


def run_pipeline():
    with SparkContext() as spark:
        
        # Get the config file
        pipeline_config = load_yaml('employee')

        #Parse the pipeline
        pipeline = Pipeline(**pipeline_config)

        print(pipeline)
        df = read_data_files(spark, file_path=pipeline.relative_path, 
                        format="csv", 
                        header=True)

        df.show()



def main():
    run_pipeline()

if __name__=='__main__':
    main()

