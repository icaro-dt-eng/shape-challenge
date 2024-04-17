import psycopg2
import logging
import enum
import os
from dotenv import load_dotenv
load_dotenv()

import time
logger = logging.getLogger()

class OperationType(enum.Enum):
    CREATE_TABLE = 1
    INSERT_ROWS = 2
    SELECT_DATA = 3

class PostgresDbOperations():
    """
    Class for performing PostgreSQL database operations.
    """
    
    def __init__(self, table_name=None):
        self.__table_name = table_name
        self.__logger = logging.getLogger(__name__)
        self.__connection = None
        
    
    def __connect_db(self):
        """
        Connect to the PostgreSQL database.
        """
        try:
            self.__connection = psycopg2.connect(
                host=os.environ.get('DB_HOST'), #'postgres', #os.environ.get('DB_HOST'),
                database=os.environ.get('DB_NAME'), #'shape_challenge',
                user=os.environ.get('DB_USER'), #'postgres'
                password=os.environ.get('DB_PASSWORD'), #'postgres'
                port=os.environ.get('DB_PORT'), #'5432'
            )

        except psycopg2.Error as e:
            self.__logger.error("Unable to connect to the database.")
            self.__logger.error(e)
            raise  # Re-raise the exception for the calling code to handle

    
    def __disconnect_db(self):
        """
        Disconnect from the PostgreSQL database.
        """
        if self.__connection is not None:
            self.__connection.close()
    
    
    def __run_command(self, command, operation_type, row_values=None):
        """
        Run a command on the PostgreSQL database.

        Args:
            command (str): SQL command to run.
            operation_type (OperationType): Type of operation.
            row_values (list): Values to be inserted in case of INSERT operation.

        Returns:
            list: Result of SELECT operation, if applicable.
        """
        try:
            if self.__connection is None or self.__connection.closed:
                self.__connect_db()

            cursor = self.__connection.cursor()
            
            if operation_type in [OperationType.CREATE_TABLE, OperationType.SELECT_DATA]:
                cursor.execute(command)

                if operation_type == OperationType.SELECT_DATA:
                    return cursor.fetchall()
            
                
            elif operation_type == OperationType.INSERT_ROWS:
                cursor.executemany(command, row_values)
                
            self.__connection.commit()

        except psycopg2.Error as error:
            if operation_type  == OperationType.INSERT_ROWS:
                self.__connection.rollback()  # Rollback the transaction if there's an error

            self.__logger.error("Error executing command.")
            self.__logger.error(error)
            raise  # Re-raise the exception for the calling code to handle

        finally:
            self.__disconnect_db()  # Ensure connection is closed even if an error occurs


    def insert_rows(self, rows, columns):
        """
        Insert rows into the specified table.

        Args:
            rows (list): Rows to be inserted.
            columns (list): Column names for insertion.

        """
        column_names = str(columns).replace('[',"").replace(']',"").replace("'", "")
        placeholders = '%s, '*len(columns)
        
        command = f"INSERT INTO {self.__table_name}({column_names}) VALUES({placeholders[:-2]});"
        
        self.__run_command(command, OperationType.INSERT_ROWS, rows)


    def insert_file(self, df):
        """
        Insert data from DataFrame into the specified table.

        Args:
            df (DataFrame): DataFrame containing data to be inserted.
        """

        columns = list(df.columns)
        rows = df.rdd.map(tuple).collect()

        self.insert_rows(rows, columns)


    def create_tables(self):
        """
        Create tables in the database if they do not exist.
        """
        commands = (
        """
        CREATE TABLE IF NOT EXISTS equipment (
            equipment_id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            group_name VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS equipment_sensor (
            sensor_id SERIAL PRIMARY KEY,
            equipment_id INTEGER REFERENCES equipment(equipment_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS equipment_failure_sensor (
            timestamp TIMESTAMP,
            sensor_id INTEGER REFERENCES equipment_sensor(sensor_id),
            temperature FLOAT,
            vibration FLOAT
        )
        """
        )

        for command in commands:
            self.__run_command(command, OperationType.CREATE_TABLE)


    def select_data(self, query):
        """
        Execute a SELECT query on the database.

        Args:
            query (str): SQL SELECT query to execute.

        Returns:
            list: Result of the SELECT query.
        """
        
        return self.__run_command(query, OperationType.SELECT_DATA)
    

if __name__ == '__main__':
    db = PostgresDbOperations()
    db.create_tables()
    
