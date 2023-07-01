import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def get_industry_data():
    industry_columns = ['Industry_name', 'type_of_work', 'salary', 'work_hours']

    industry_data = [
        ['IT', 'Software Development', 8000, 40],
        ['Sales', 'Sales Representative', 6000, 35],
        ['Finance', 'Accountant', 7000, 40],
        ['Marketing', 'Marketing Specialist', 6500, 38],
        ['HR', 'Human Resources Manager', 7500, 40]
    ]

    industry_schema = (
        StructType(
        [
            StructField('Industry_name', StringType(), True),
            StructField,('type_of_work', StringType(), True),
            StructField,('salary', IntegerType(), True),
            StructField,('work_hours', IntegerType(), True),
            ]
        )
    )

    return industry_columns, industry_data, industry_schema

def main():
    module_name = os.path.basename(__file__)
    print(f"Module - {module_name}")

if __name__ == '__main__':
    main()