import sys
import logging
logger = logging.getLogger()

from postgresql_util import PostgresDbOperations
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

# Create a SparkConf object and set driver memory
conf = SparkConf().setAppName("ReadTextFileExample").set("spark.driver.memory", "8g")

# Create a SparkContext with the configured SparkConf
sc = SparkContext(conf=conf)

# Create a SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()

db_operations = PostgresDbOperations()

print('\n\n1. Total equipment failures that happened?')

query1 = db_operations.select_data('''
    SELECT COUNT(*) AS total_failures
    FROM equipment_failure_sensor;
''')

print(f'Total Failures: {query1[0][0]}\n\n')


print('2. Total equipment failures that happened in the last 7 days?')

query2 = db_operations.select_data('''
    SELECT e.name, COUNT(f.*) AS failure_count
    FROM equipment e
    JOIN equipment_sensor s ON e.equipment_id = s.equipment_id
    JOIN equipment_failure_sensor f ON s.sensor_id = f.sensor_id
    GROUP BY e.name
    ORDER BY failure_count DESC
    LIMIT 1;
''')

print(f'Equipment with the most failures: {query2[0][0]}')
print(f'Number of failures: {query2[0][1]}\n\n')


print('3. Equipment with the most failures in the last 7 days?')

query3 = db_operations.select_data('''
    SELECT group_name, AVG(failure_count) AS average_failures
    FROM (
        SELECT e.group_name, COUNT(f.*) AS failure_count
        FROM equipment e
        JOIN equipment_sensor s ON e.equipment_id = s.equipment_id
        JOIN equipment_failure_sensor f ON s.sensor_id = f.sensor_id
        GROUP BY e.group_name
    ) AS group_failures
    GROUP BY group_name
    ORDER BY average_failures ASC;
''')

query3 = [(row[0], int(row[1])) for row in query3]

spark.createDataFrame(query3, ['group_name', 'average_failures']).show()


print('\n4. Equipment with the most failures in the last 7 days?')

query4 = db_operations.select_data('''
    SELECT e.group_name, e.name, s.sensor_id, COUNT(f.*) AS error_count
    FROM equipment e
    JOIN equipment_sensor s ON e.equipment_id = s.equipment_id
    JOIN equipment_failure_sensor f ON s.sensor_id = f.sensor_id
    GROUP BY e.group_name, e.name, s.sensor_id
    ORDER BY e.group_name, e.name, error_count DESC;
''')

spark.createDataFrame(query4, ['group_name', 'equipament_name', 'sensor_id', 'error_count']).show()