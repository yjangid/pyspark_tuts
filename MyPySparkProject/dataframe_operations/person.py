import os
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType
def get_person_data():
    
    # person_columns = ['first_name', 'middle_name', 'last_name', 'age', 'email_id', 'is_skilled', 'Industry', 'Address_id']

    person_data = [
        ['John', 'A.', 'Smith', 35, 'john.smith@example.com', True, 'IT', 1, 'M'],
        ['Jane', 'M.', 'Doe', 28, 'jane.doe@example.com', False, 'Sales', 2, 'F'],
        ['David', None, 'Brown', 42, 'david.brown@example.com', True, 'Finance', 3, 'M'],
        ['Emma', None, 'Davis', 31, 'emma.davis@example.com', True, 'Marketing', 4, 'F'],
        ['Michael', None, 'Lee', 39, 'michael.lee@example.com', False, 'IT', 5, 'M'],
        ['Sarah', 'J.', 'Johnson', 45, 'sarah.johnson@example.com', True, 'HR', 6, 'F'],
        ['Robert', None, 'Wilson', 33, 'robert.wilson@example.com', True, 'Sales', 7, 'M'],
        ['Olivia', None, 'Taylor', 29, 'olivia.taylor@example.com', False, 'Finance', 8, 'F'],
        ['William', None, 'Clark', 37, 'william.clark@example.com', True, 'Marketing', 9, 'M'],
        ['Emily', None, 'Anderson', 41, 'emily.anderson@example.com', True, 'IT', 10, 'F'],
        ['James', None, 'Moore', 36, 'james.moore@example.com', False, 'HR', 11, 'M'],
        ['Sophia', None, 'Garcia', 34, 'sophia.garcia@example.com', True, 'Sales', 12, 'F'],
        ['Alexander', None, 'Martinez', 27, 'alexander.martinez@example.com', True, 'Finance', 13, 'M'],
        ['Chloe', None, 'Lopez', 30, 'chloe.lopez@example.com', False, 'Marketing', 14, 'F'],
        ['Benjamin', None, 'Hall', 32, 'benjamin.hall@example.com', True, 'IT', 15, 'M'],
        ['Lily', None, 'Turner', 29, 'lily.turner@example.com', True, 'HR', 16, 'F'],
        ['Ethan', None, 'Collins', 38, 'ethan.collins@example.com', False, 'Sales', 17, 'M'],
        ['Mia', None, 'Baker', 26, 'mia.baker@example.com', True, 'Finance', 18, 'F'],
        ['Noah', None, 'Green', 43, 'noah.green@example.com', True, 'Marketing', 19, 'M'],
        ['Ava', None, 'Young', 31, 'ava.young@example.com', False, 'IT', 20, 'F']
    ]
    
    #Create schema for the data
    schema = (
                StructType().add("first_name", StringType(), True)
                .add("middle_name", StringType(), True)
                .add("last_name", StringType(), True)
                .add("age", IntegerType(), True)
                .add("email_id", StringType(), True)
                .add("is_skilled", BooleanType(), True)
                .add("Industry", StringType(), True)
                .add("Address_id", IntegerType(), True)
                .add("gender", StringType(), True)
            )

    return person_data, schema

def main():
    module_name = os.path.basename(__file__)
    print(f"Module - {module_name}")

if __name__ == '__main__':
    main()