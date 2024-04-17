import re
import os
import logging

logger=logging.getLogger()

from postgresql_util import PostgresDbOperations
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# Create a SparkConf object and set driver memory
conf = SparkConf().setAppName("ReadTextFileExample").set("spark.driver.memory", "8g")

# Create a SparkContext with the configured SparkConf
sc = SparkContext(conf=conf)

# Create a SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()


def unpack_rar(file_path, extract_dir):
    """
    Unpacks a .rar file to a specified directory.
    
    Args:
        file_path (str): Path to the .rar file.
        extract_dir (str): Directory to extract the contents of the .rar file.
        
    Returns:
        bool: True if extraction is successful, False otherwise.
    """
    try:
        # Check if the specified file exists
        if not os.path.isfile(file_path):
            print(f"Error: File '{file_path}' not found.")
            return False
        
        # Create the extraction directory if it doesn't exist
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir)
        
        # Open the .rar file
        with rarfile.RarFile(file_path, 'r') as rf:
            # Extract all contents to the specified directory
            rf.extractall(path=extract_dir)
        
        print(f"Successfully extracted '{file_path}' to '{extract_dir}'.")
        return True
    
    except Exception as e:
        print(f"Error: Failed to extract '{file_path}'. {e}")
        return False


def parse_failure_log(line):
    """
    Parse a line from a failure log and extract relevant information.

    Args:
        line (str): A single line from a failure log.

    Returns:
        tuple or None: A tuple containing parsed information if the line matches the expected format,
    """
    match = re.match(r'\[(.*?)\]\s+ERROR\s+sensor\[(\d+)\]:\s+\(temperature\s+([\d.-]+),\s+vibration\s+([\d.-]+)\)', line)
    if match:
        timestamp, sensor_id, temperature, vibration = match.groups()
        return (timestamp, int(sensor_id), float(temperature), float(vibration))
    return None

unpack_rar('../data/equipment_failure_sensors.rar', '../data')

# Define schemas for the DataFrames
schema_eq_failure_sensors = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("vibration", FloatType(), True)
])

schema_eq = StructType([
    StructField("equipment_id", IntegerType(), True),
    StructField("group_name", StringType(), True),
    StructField("name", StringType(), True)
])

schema_eq_sensors = StructType([
    StructField("equipment_id", IntegerType(), True),
    StructField("sensor_id", IntegerType(), True)
])

# Read the text file into an RDD
text_file_rdd = sc.textFile("../data/equpment_failure_sensors.txt")

# Parse the failure log data
parsed_data = text_file_rdd.map(parse_failure_log).filter(lambda x: x is not None)

# Create DataFrame directly from RDD and apply schema
df_equipment_failure_sensor = spark.createDataFrame(parsed_data, schema=schema_eq_failure_sensors)

# Read the JSON file into a DataFrame
df_equipment = spark.read.schema(schema_eq).option('multiLine', True).json('../data/equipment.json')

# Read the CSV file into a DataFrame
df_equipment_sensor = spark.read.schema(schema_eq_sensors).csv('../data/equipment_sensors.csv', header=True)

# Initialize PostgreSQL database operations
db_operations = PostgresDbOperations()

# Create tables in the database
db_operations.create_tables()

# Insert data into database tables
for base_name in ['equipment', 'equipment_sensor', 'equipment_failure_sensor']:
    db_operations = PostgresDbOperations(base_name)
    
    db_operations.insert_file(eval(f'df_{base_name}'))

print('\nData inserted successfully!\n')