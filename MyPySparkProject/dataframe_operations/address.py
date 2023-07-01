import os
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

def get_address_data():

    address_columns = ['Address_id', 'House_no', 'street_name', 'city', 'country', 'zip_code']

    address_data = [
        [1, '123', 'Main St', 'New York', 'USA', '10001'],
        [2, '456', 'Elm St', 'Chicago', 'USA', '60611'],
        [3, '789', 'Oak St', 'Los Angeles', 'USA', '90001'],
        [4, '321', 'Pine St', 'San Francisco', 'USA', '94101'],
        [5, '555', 'Cedar St', 'Seattle', 'USA', '98101'],
        [6, '777', 'Birch St', 'Denver', 'USA', '80201'],
        [7, '999', 'Maple St', 'Austin', 'USA', '78701'],
        [8, '111', 'Spruce St', 'Boston', 'USA', '02101'],
        [9, '888', 'Willow St', 'Miami', 'USA', '33101'],
        [10, '444', 'Oak St', 'Phoenix', 'USA', '85001'],
        [11, '222', 'Palm St', 'Las Vegas', 'USA', '89101'],
        [12, '666', 'Juniper St', 'Atlanta', 'USA', '30301'],
        [13, '777', 'Cherry St', 'Dallas', 'USA', '75201'],
        [14, '999', 'Sycamore St', 'Houston', 'USA', '77001'],
        [15, '555', 'Cypress St', 'New Orleans', 'USA', '70112'],
        [16, '111', 'Magnolia St', 'Orlando', 'USA', '32801'],
        [17, '888', 'Hickory St', 'Nashville', 'USA', '37201'],
        [18, '123', 'Ash St', 'San Diego', 'USA', '92101'],
        [19, '444', 'Beech St', 'Portland', 'USA', '97201'],
        [20, '222', 'Redwood St', 'Washington D.C.', 'USA', '20001']
    ]

    address_schema = (
        StructType(
        [
            StructField('Address_id', IntegerType(), True),
            StructField('House_no', StringType(), True),
            StructField,('street_name', StringType(), True),
            StructField,('city', StringType(), True),
            StructField,('country', StringType(), True),
            StructField,('zip_code', StringType(), True),
            ]
        )
    )

    return address_columns, address_data, address_schema

def main():
    module_name = os.path.basename(__file__)
    print(f"Module - {module_name}")

if __name__ == '__main__':
    main()