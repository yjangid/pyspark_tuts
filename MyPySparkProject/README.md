#########################################################################################################
############################### How to run file from othre packages #####################################
#########################################################################################################
Suppose Below is the project directory
MyPySparkProject
    - __init__.py (This Should be the empty file)
    - scripts
        - __init__.py (This Should be the empty file)
        - test.py
    - modules
        -__init__.py (This Should be the empty file)
        - module.py

1. From Anaconda prompt - 
    Set the path till the MyPySparkProject
        - set set PYTHONPATH=D:\new_git_repo\pyspark_tuts\MyPySparkProject
        - python D:\new_git_repo\pyspark_tuts\MyPySparkProject\scripts\create_df_from_rdd.py

2. Also added path that interpreter can easily fine what has to be run
    - Append a path to sys.path.append(path) like below
        sys.path.append('D:\new_git_repo\pyspark_tuts\MyPySparkProject')
        OR
    - Let python itself add Dynamically path with below code -
        import os
        import sys
        # Get the current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the absolute path to the project directory
        project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        # Add the project directory to sys.path
        sys.path.append(project_dir)
